"""ROR → ``lake.ror.organization``: the institution-identity authority (issue #57, EL only).

RePORTER, OpenAlex and Reliance each name institutions their own way; ROR is the
CC0 authority that reconciles them. This lake already carries the join key —
``lake.openalex.works_authorships.institution_ror`` holds a full ROR URL — so
this table turns an affiliation string into a resolvable organization today,
with no new plumbing.

Exactly the ``bioregistry`` shape (single versioned bulk file → MERGE-upsert),
with one difference: ROR's dumps **carry their own version**. The Zenodo
*concept* record (6347574) always resolves to the newest release, whose
``metadata.version`` is the release tag (``v2.11``, 2026-08-03) — so the
snapshot is tagged by the dump's version, the ``scp``/``bugsigdb`` treatment,
not by pull date like the rolling sources.

**JSON, not CSV.** The dump ships both; ROR states JSON is authoritative and the
CSV a flattened subset (it drops ``relationships`` — the parent/child org
hierarchy — and the per-name ``lang``/``types``). Landing the JSON keeps
everything, and sidesteps a real hazard on the CSV path: **804 organization
names contain a bare ``"``** (``Universitatea de Stat de Medicină și Farmacie
"Nicolae Testemițanu"``), exactly the free-text-quote trap that costs the
tab-delimited sources a ``quote=''``/``escape=''``. JSON quotes its own strings,
so the question doesn't arise.

Verified against the real v2.11 dump (135,710 records), not assumed:

* **The key holds.** ``id`` is unique across all 135,710 records, and every one
  is ``https://ror.org/<lui>``. Landed as the bare LUI (``04ttjf776``) per this
  lake's id-column convention (#46: an ``<x>_id`` column holds the bioregistry
  prefix's local identifier, not a URI). The full URL is
  ``'https://ror.org/' || ror_id``, so it isn't landed; joining OpenAlex is
  ``split_part(institution_ror, '/', -1) = ror_id``.
* ``name`` is derived, and provably 1:1 — **every** record has exactly one name
  typed ``ror_display``, so picking it is a fact, not a heuristic. The raw
  ``names`` list lands verbatim beside it. Padding whitespace: 288 names carry
  it (216 acronyms, 71 aliases, 1 label) — but **zero** ``ror_display`` names
  do, so the ``trim()`` on ``name`` is defensive, not a fix for a live bug the
  way Reactome's was. Kept anyway: it costs nothing and the join column is the
  one place padding silently breaks things.
* **Country is deliberately not derived.** 131 records have more than one
  location (up to 5), so a ``locations[1]`` country would silently pick one.
  ``locations`` lands whole and nested; unnest it if you need every site.
* ``established`` is a year, NULL for 26,434 records; no ``-``/``\\N`` sentinel
  appears anywhere in the dump (it is JSON — absence is ``null``), so there is
  nothing to declare a ``nullstr`` for.
* ``status`` is ``active`` (132,706) / ``inactive`` (1,595) / ``withdrawn``
  (1,409). All three land: a withdrawn ROR id still needs to resolve for
  historical affiliation data.
* Nested ``LIST<STRUCT>`` columns round-trip through DuckLake/Parquet and MERGE
  idempotently (verified end-to-end on all 135,710 rows).

Column drift: the type spec below is stated, never sniffed (``auto_detect=false``).
But JSON columns are *name*-addressed, so drift can't shift values one to the
left the way it can in a headerless TSV — it fails quietly instead, as a column
that silently goes all-NULL. :func:`check_keys` restores the loud failure by
sweeping every record's top-level key set against the spec before loading
(0.7 s on the real file).

License: **CC0**. ror.readme.io/docs/data-dump (confirmed 2026-08-11): "All ROR
IDs and metadata in the data dump are provided under the Creative Commons CC0
1.0 Universal Public Domain Dedication." The Zenodo record itself carries
``license: cc-zero``.

Scope: EL only. Whether this ever reverse-ETLs to bioc-on-ice is the open
question #57 itself raises and does not settle — deliberately left unanswered
here, and publishing deferred per #63.

On ``ref`` vs. its own schema (#57's other open question), the same grain
argument #38 used for ``gene2pubmed``: ``ref.id_crosswalk``'s grain is one row
per PMID, and an organization is not a paper — it cannot go there without
breaking the grain. ``ref.bioregistry`` is the tempting counterexample, but it
is a *registry of this lake's own naming convention*, not an upstream entity
with a release cadence. Landing in ``lake.ror`` keeps the rule simple: **a
source's raw table lives in the source's schema; ``ref`` is the transform
layer's namespace** (``models/ref/*.sql``). If an institution crosswalk is ever
wanted, it belongs there as a model over this table — a decision that stays
cheap precisely because the raw landing didn't presume it.
"""

from __future__ import annotations

import re
from pathlib import Path

import duckdb

from ... import ops
from ...config import Settings, get_settings
from ...connect import LAKE, lake_connect, raw_dir, upsert
from ...download import download, get_json, unzip
from ...log import logger

_RAW = "ror"
_log = logger.bind(ctx="ror")

_GEONAMES = (
    "STRUCT(continent_code VARCHAR, continent_name VARCHAR, country_code VARCHAR, "
    "country_name VARCHAR, country_subdivision_code VARCHAR, "
    "country_subdivision_name VARCHAR, lat DOUBLE, lng DOUBLE, name VARCHAR)"
)
_ADMIN_ENTRY = 'STRUCT("date" VARCHAR, schema_version VARCHAR)'

# The dump's own top-level keys → the DuckDB type each lands as, stated rather
# than sniffed. `all` and `type` and `date` are quoted: they are DuckDB keywords.
COLUMNS: dict[str, str] = {
    "id": "VARCHAR",
    "names": "STRUCT(value VARCHAR, types VARCHAR[], lang VARCHAR)[]",
    "status": "VARCHAR",
    "types": "VARCHAR[]",
    "established": "BIGINT",
    "domains": "VARCHAR[]",
    "links": 'STRUCT("type" VARCHAR, value VARCHAR)[]',
    "external_ids": 'STRUCT("type" VARCHAR, "all" VARCHAR[], preferred VARCHAR)[]',
    "relationships": 'STRUCT("type" VARCHAR, label VARCHAR, id VARCHAR)[]',
    "locations": f"STRUCT(geonames_id BIGINT, geonames_details {_GEONAMES})[]",
    "admin": f"STRUCT(created {_ADMIN_ENTRY}, last_modified {_ADMIN_ENTRY})",
}

# Everything except `id` (→ ror_id) and the derived `name` lands under its own
# upstream key.
_VERBATIM = tuple(k for k in COLUMNS if k != "id")


def _sql_str(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _version_from_name(path: Path) -> str:
    """``v2.11-2026-08-03-ror-data.json`` → ``v2.11`` (the release's own tag)."""
    match = re.match(r"(v[\d.]+?)-\d{4}-\d{2}-\d{2}-", path.name)
    if not match:
        raise ValueError(
            f"cannot read a ROR release version out of {path.name!r} — pass version=..."
        )
    return match.group(1)


def download_dump(settings: Settings | None = None) -> tuple[Path, str]:
    """Download + unzip the newest ROR dump; return ``(json path, version)``.

    The Zenodo *concept* record resolves to the latest release, so there is no
    version to pin in config — the release names itself.
    """
    s = settings or get_settings()
    record = get_json(f"https://zenodo.org/api/records/{s.ror_zenodo_concept_record}")
    version = record["metadata"]["version"]
    files = record["files"]
    if len(files) != 1:
        raise ValueError(f"expected one ROR dump archive on Zenodo, got {len(files)}")
    archive = download(files[0]["links"]["self"], raw_dir(_RAW, s) / files[0]["key"])
    members = unzip(archive, raw_dir(_RAW, s) / version)
    jsons = [p for p in members if p.suffix == ".json"]
    if len(jsons) != 1:
        raise ValueError(f"expected one .json in {archive.name}, got {[p.name for p in jsons]}")
    return jsons[0], version


def check_keys(con: duckdb.DuckDBPyConnection, path: Path) -> None:
    """Fail loudly if the dump's top-level keys have drifted from :data:`COLUMNS`.

    A JSON column spec is name-addressed, so a renamed/dropped upstream key would
    otherwise land as a silently all-NULL column rather than an error. One sweep
    of every record's key set (0.7 s over the real 305 MB dump) buys the loud
    failure a headerless TSV gets from a positional spec.
    """
    found = {
        k for (k,) in con.execute(
            f"SELECT DISTINCT unnest(json_keys(json)) "
            f"FROM read_json_objects({_sql_str(str(path))}, format = 'array')"
        ).fetchall()
    }
    if found != set(COLUMNS):
        raise ValueError(
            f"ROR dump keys drifted: +{sorted(found - set(COLUMNS))} "
            f"-{sorted(set(COLUMNS) - found)} — update COLUMNS before landing"
        )


def _select_sql(path: Path, version: str, limit: int | None) -> str:
    columns_sql = ", ".join(f"'{k}': '{t}'" for k, t in COLUMNS.items())
    verbatim = ",\n            ".join(_VERBATIM)
    limit_sql = f" LIMIT {int(limit)}" if limit else ""
    # `name` is the one derived column: every record has exactly one ror_display
    # name (verified across all 135,710), trimmed because 288 arrive padded.
    return f"""
        SELECT
            split_part(id, '/', -1) AS ror_id,
            trim(list_filter(names, n -> list_contains(n.types, 'ror_display'))[1].value)
                AS name,
            {verbatim},
            CAST({_sql_str(version)} AS VARCHAR) AS snapshot_version
        FROM read_json(
            {_sql_str(str(path))}, format = 'array',
            columns = {{{columns_sql}}}, auto_detect = false
        )
        {limit_sql}
    """


def land(
    con: duckdb.DuckDBPyConnection,
    path: Path,
    version: str,
    *,
    schema: str = "ror",
    limit: int | None = None,
) -> int:
    """MERGE-upsert one ROR dump into ``lake.<schema>.organization`` on ``ror_id``."""
    check_keys(con, path)
    return upsert(
        con, f"{LAKE}.{schema}.organization", _select_sql(path, version, limit),
        key="ror_id", exclude_change_cols=["snapshot_version"],
    )


def ingest(
    *,
    file: str | None = None,
    version: str | None = None,
    schema: str = "ror",
    limit: int | None = None,
    settings: Settings | None = None,
) -> dict:
    """End-to-end: fetch the newest dump (unless ``file``) → MERGE-upsert → summary."""
    s = settings or get_settings()
    if file:
        path = Path(file)
        version = version or _version_from_name(path)
    else:
        path, dump_version = download_dump(s)
        version = version or dump_version
    target = f"{LAKE}.{schema}.organization"
    con = lake_connect(s)
    try:
        with ops.run(con, source="ror", target=target, version=version) as r:
            r.rows = land(con, path, version, schema=schema, limit=limit)
            _log.info("organization <- {:,} rows ({})", r.rows, version)
    finally:
        con.close()
    return r.summary()
