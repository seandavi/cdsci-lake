"""Ingest OBO ontologies (semantic-sql SQLite builds) into ``ontology.*``.

The `semantic-sql <https://github.com/INCATools/semantic-sql>`_ project publishes
every OBO ontology as a SQLite database (the ``bbop-sqlite`` S3 bucket). Each DB is
the relational-graph encoding of one OWL ontology: a generic ``statements`` triple
table (literals in ``value``, IRI objects in ``object``) plus convenience views
(``edge`` = asserted direct edges, ``rdfs_label_statement``, ...). DuckDB's
``sqlite`` extension scans those directly — no parser code, mirroring how
``census_geo`` uses ``spatial``/``ST_Read``.

We project four typed, **cross-ontology stacked** tables (every row carries an
``ontology`` discriminator = the source DB stem, e.g. ``uberon``):

* ``ontology.terms``    — key ``(ontology, curie)``: label, definition, obsolete,
  replaced_by. The matching target.
* ``ontology.synonyms`` — key ``(ontology, curie, synonym, scope)``: exact / broad
  / narrow / related synonyms (the free-text match surface).
* ``ontology.xrefs``    — key ``(ontology, curie, xref)``: database cross-references.
* ``ontology.edges``    — key ``(ontology, subject, predicate, object)``: asserted
  direct edges (is_a, part_of, ...). Ancestor closure is computed on demand with a
  recursive CTE over this table (see ``docs/design/ontology.md``), so the giant
  ``entailed_edge`` closure is deliberately **not** materialized.

``snapshot_version`` (the upstream DB's S3 Last-Modified date) is excluded from
change-detection, so a re-load that finds no real change adds no snapshot.

Caveat: ``replaced_by`` is stored as semsql emits it (often a full IRI rather than a
contracted CURIE); consumers normalize against the ``prefix`` mapping if needed.
"""

from __future__ import annotations

import gzip
import shutil
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

import duckdb
import httpx

from ... import ops
from ...config import Settings, get_settings
from ...connect import LAKE, lake_connect, raw_dir, upsert
from ...download import download

_S3_NS = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
_TIMEOUT = httpx.Timeout(60.0, read=300.0)
_SUFFIX = ".db.gz"


@dataclass(frozen=True)
class Relation:
    """One projection of a semsql DB → one ``ontology`` table.

    ``select`` is a SQL template over the ATTACHed SQLite db (alias ``src``) with
    ``{onto}`` / ``{release}`` placeholders; it must emit ``snapshot_version`` last.
    """

    table: str
    key: tuple[str, ...]
    select: str


_TERMS = Relation(
    table="terms",
    key=("ontology", "curie"),
    select="""
        SELECT
            '{onto}' AS ontology,
            t.subject AS curie,
            t.label,
            d.definition,
            COALESCE(dep.obsolete, FALSE) AS obsolete,
            r.replaced_by,
            '{release}' AS snapshot_version
        FROM (
            SELECT subject, any_value(value) AS label
            FROM src.statements
            WHERE predicate = 'rdfs:label' AND value IS NOT NULL
            GROUP BY subject
        ) AS t
        LEFT JOIN (
            SELECT subject, any_value(value) AS definition
            FROM src.statements
            WHERE predicate = 'IAO:0000115' AND value IS NOT NULL
            GROUP BY subject
        ) AS d ON d.subject = t.subject
        LEFT JOIN (
            SELECT DISTINCT subject, TRUE AS obsolete
            FROM src.statements
            WHERE predicate = 'owl:deprecated' AND value = 'true'
        ) AS dep ON dep.subject = t.subject
        LEFT JOIN (
            SELECT subject, any_value(value) AS replaced_by
            FROM src.statements
            WHERE predicate = 'IAO:0100001' AND value IS NOT NULL
            GROUP BY subject
        ) AS r ON r.subject = t.subject
    """,
)

_SYNONYMS = Relation(
    table="synonyms",
    key=("ontology", "curie", "synonym", "scope"),
    select="""
        SELECT DISTINCT
            '{onto}' AS ontology,
            subject AS curie,
            value AS synonym,
            CASE predicate
                WHEN 'oio:hasExactSynonym' THEN 'exact'
                WHEN 'oio:hasBroadSynonym' THEN 'broad'
                WHEN 'oio:hasNarrowSynonym' THEN 'narrow'
                WHEN 'oio:hasRelatedSynonym' THEN 'related'
            END AS scope,
            '{release}' AS snapshot_version
        FROM src.statements
        WHERE predicate IN (
            'oio:hasExactSynonym', 'oio:hasBroadSynonym',
            'oio:hasNarrowSynonym', 'oio:hasRelatedSynonym'
        ) AND value IS NOT NULL
    """,
)

_XREFS = Relation(
    table="xrefs",
    key=("ontology", "curie", "xref"),
    select="""
        SELECT DISTINCT
            '{onto}' AS ontology,
            subject AS curie,
            value AS xref,
            '{release}' AS snapshot_version
        FROM src.statements
        WHERE predicate = 'oio:hasDbXref' AND value IS NOT NULL
    """,
)

_EDGES = Relation(
    table="edges",
    key=("ontology", "subject", "predicate", "object"),
    select="""
        SELECT DISTINCT
            '{onto}' AS ontology,
            subject,
            predicate,
            object,
            '{release}' AS snapshot_version
        FROM src.edge
        WHERE object IS NOT NULL
    """,
)

RELATIONS: dict[str, Relation] = {r.table: r for r in (_TERMS, _SYNONYMS, _XREFS, _EDGES)}


def available_ontologies(settings: Settings | None = None) -> list[str]:
    """List every ontology stem published in the semsql bucket (sorted).

    Reads the S3 ListObjectsV2 XML (paginated) and strips the ``.db.gz`` suffix, so
    "all available" always reflects the live bucket rather than a static registry.
    """
    s = settings or get_settings()
    base = s.semsql_base_url.rstrip("/")
    stems: list[str] = []
    token: str | None = None
    with httpx.Client(timeout=_TIMEOUT, follow_redirects=True) as client:
        while True:
            params = {"list-type": "2"}
            if token:
                params["continuation-token"] = token
            resp = client.get(f"{base}/", params=params)
            resp.raise_for_status()
            root = ET.fromstring(resp.text)
            for key in root.findall("s3:Contents/s3:Key", _S3_NS):
                name = key.text or ""
                if name.endswith(_SUFFIX):
                    stems.append(name[: -len(_SUFFIX)])
            if root.findtext("s3:IsTruncated", "false", _S3_NS) != "true":
                break
            token = root.findtext("s3:NextContinuationToken", None, _S3_NS)
            if not token:
                break
    return sorted(stems)


def _release(url: str) -> str:
    """Upstream release stamp = the DB object's S3 ``Last-Modified`` date (or today)."""
    try:
        with httpx.Client(timeout=_TIMEOUT, follow_redirects=True) as client:
            lm = client.head(url).headers.get("Last-Modified")
        if lm:
            return datetime.strptime(lm, "%a, %d %b %Y %H:%M:%S %Z").strftime("%Y-%m-%d")
    except (httpx.HTTPError, ValueError):
        pass
    return datetime.now().strftime("%Y-%m-%d")


def fetch_db(onto: str, settings: Settings | None = None) -> tuple[Path, str]:
    """Download + gunzip one semsql DB into the raw layer; return ``(path, release)``.

    Both steps are idempotent: ``download`` skips an existing ``.db.gz`` and the
    gunzip skips an existing ``.db`` — re-running an ingest re-uses the bronze files.
    """
    s = settings or get_settings()
    base = s.semsql_base_url.rstrip("/")
    url = f"{base}/{onto}{_SUFFIX}"
    root = raw_dir("ontology", s)
    gz = download(url, root / f"{onto}{_SUFFIX}")
    db = root / f"{onto}.db"
    if not db.exists():
        tmp = db.with_suffix(".db.part")
        with gzip.open(gz, "rb") as src, open(tmp, "wb") as dst:
            shutil.copyfileobj(src, dst, length=1 << 20)
        tmp.rename(db)
    return db, _release(url)


def curate(
    con: duckdb.DuckDBPyConnection,
    onto: str,
    db: Path | str,
    release: str,
    *,
    schema: str = "ontology",
) -> dict[str, int]:
    """Upsert all four projections of one semsql DB into ``ontology.*``.

    The SQLite DB is ATTACHed read-only as ``src``; each :class:`Relation` MERGEs on
    its key with ``snapshot_version`` excluded from change-detection.
    """
    con.execute("INSTALL sqlite; LOAD sqlite;")
    con.execute(f"ATTACH '{db}' AS src (TYPE sqlite, READ_ONLY);")
    try:
        counts: dict[str, int] = {}
        for rel in RELATIONS.values():
            target = f"{LAKE}.{schema}.{rel.table}"
            sql = rel.select.format(onto=onto, release=release)
            counts[rel.table] = upsert(
                con, target, sql, key=list(rel.key),
                exclude_change_cols=["snapshot_version"],
            )
    finally:
        con.execute("DETACH src;")
    return counts


def _today_version() -> str:
    """Run-level label -- each table's own `snapshot_version` is the per-ontology release."""
    return date.today().isoformat()


def ingest(
    *,
    ontologies: list[str] | None = None,
    schema: str = "ontology",
    settings: Settings | None = None,
) -> dict:
    """Download + curate ontologies (default: every one in the bucket).

    One DB at a time so memory stays flat; a failure on a single ontology is
    recorded and skipped rather than aborting the whole batch. The whole batch is
    one ``ops.run`` (ADR-0006), like every other source -- previously this ran
    outside the ledger entirely (issue #52), leaving no run row and no attributed
    snapshot.
    """
    s = settings or get_settings()
    onts = ontologies or available_ontologies(s)
    target = f"{LAKE}.{schema}"
    con = lake_connect(s)
    counts: dict[str, dict[str, int]] = {}
    errors: dict[str, str] = {}
    try:
        with ops.run(con, source="ontology", target=target, version=_today_version()) as r:
            for onto in onts:
                try:
                    db, release = fetch_db(onto, s)
                    counts[onto] = curate(con, onto, db, release, schema=schema)
                except (httpx.HTTPError, duckdb.Error, OSError) as exc:
                    errors[onto] = f"{type(exc).__name__}: {exc}"
            r.rows = sum(sum(c.values()) for c in counts.values())
    finally:
        con.close()
    return {
        **r.summary(), "schema": schema, "loaded": len(counts),
        "counts": counts, "errors": errors,
    }
