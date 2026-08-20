"""ORCID → ``lake.orcid.person``: researcher-identity crosswalk (issue #56, EL only).

**Deliberate deviation from this lake's "land raw whole" convention: the annual
bulk Public Data File is not landed. The public API is, scoped to a caller-given
set of ORCID iDs.** #56 anticipated this trade-off; here is what the real bytes
said (checked 2026-08-11, figshare record ``10.23640/07243.30375589``, the 2025
file):

* The summaries archive alone is **46.33 GB compressed / 863,678 MB (~863 GB)
  uncompressed**, plus eleven activities archives of 16–19 GB each — ~237 GB
  compressed in total.
* It is **XML, one file per record**, unpacked into ``summaries/<3-digit
  checksum>/<iD>.xml``. There is no scannable bulk artifact at all: DuckDB reads
  CSV/JSON/Parquet, not a tar of ~20 million tiny XML documents. Landing it
  whole is not "a big download", it is a bespoke XML shredder for a registry
  this lake has no reader for.
* And the join it would serve does not exist yet: **no table in this lake
  carries an ORCID iD today** (OpenAlex's authorship struct extracts
  ``author.id``/``display_name`` and the institution ROR, not ``author.orcid``;
  RePORTER carries PI names and RePORTER-internal ``pi_ids``). Landing 20M+
  researcher records to join against zero rows is the definition of speculative.

So the EL is demand-driven: hand it the iDs you actually have, it fetches those.
The iD set comes from ``orcids=`` (a comma-separated list) or ``orcids_sql=``
(any query against the attached lake returning one column of iDs — the
"iDs referenced by existing sources" path #56 describes). It **refuses to guess**
when given neither, rather than silently landing nothing. Once an ORCID-bearing
column does land — adding ``"orcid":"VARCHAR"`` to OpenAlex's authorship struct
is one line, but a separate change with a re-ingest attached — the default
becomes a one-line ``orcids_sql``.

The endpoint is ``expanded-search``, not ``/record``. Both are unauthenticated
on ``pub.orcid.org``; ``/record`` returns ~254 KB of activities per researcher
(one HTTP round trip each), while ``expanded-search`` answers ``orcid:(A OR B OR
…)`` for **100 iDs in a single ~0.4 s request** and returns precisely the
identity fields a crosswalk is for: iD, given/family/credit names, other names,
emails, institution names. Verified on a real 100-iD batch: all 100 returned,
``orcid-id`` unique, no padded strings, no nulls in given/family names. An iD
that is unknown or withdrawn simply does not come back — the response is a hit
list, not a per-iD result, so absence is silent by design.

Because the registry is live and rolling (not a versioned release), the snapshot
is tagged by retrieval date — the ``retractionwatch``/``ncbi_gene`` treatment.
Fetched records are written to the raw layer as NDJSON before loading, so the
medallion contract still holds: a re-curate needs no re-fetch.

License: **CC0**. info.orcid.org/annual-data-files (confirmed 2026-08-11):
"ORCID releases the Public Data File under a CC0 1.0 Public Domain Dedication as
further described in our Privacy Policy", and the figshare record carries
``license: CC0``. The same terms cover the public API's data (ORCID's Public
Data File Use Policy adds community *norms* — cite ORCID, respect researcher
deletions on refresh — not license conditions). Personal data: only fields the
researcher chose to make public are returned, which is what makes the dedication
possible; nothing here bypasses a record's privacy settings.

Scope: EL only. Whether this ever reverse-ETLs to bioc-on-ice is the open
question #56 raises and does not settle — deliberately left unanswered here, and
publishing deferred per #63.

On ``ref`` vs. its own schema (#56's other open question), the same grain
argument #38 used for ``gene2pubmed``: the publication-crosswalk grain is one
row per PMID, and a researcher is not a paper. Landing in ``lake.orcid`` keeps the
rule simple — **a source's raw table lives in the source's schema; ``ref`` is
the transform layer's namespace** (``models/ref/*.sql``). A researcher
crosswalk, if ever wanted, belongs there as a model over this table. See
``sources/ror/ingest.py`` for the same reasoning on the institution side.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Sequence
from datetime import date
from pathlib import Path

import duckdb

from ... import ops
from ...config import Settings, get_settings
from ...connect import LAKE, lake_connect, raw_dir, upsert
from ...download import get_json
from ...log import logger

_RAW = "orcid"
_log = logger.bind(ctx="orcid")

# expanded-search's own JSON keys → (our column name, DuckDB type). Stated, not
# sniffed: a renamed upstream key lands as an all-NULL column, which the
# `orcid_id IS NOT NULL` guard in _select_sql turns into an empty load rather
# than a silently half-populated table.
COLUMNS: dict[str, tuple[str, str]] = {
    "orcid-id": ("orcid_id", "VARCHAR"),
    "given-names": ("given_names", "VARCHAR"),
    "family-names": ("family_name", "VARCHAR"),
    "credit-name": ("credit_name", "VARCHAR"),
    "other-name": ("other_names", "VARCHAR[]"),
    "email": ("emails", "VARCHAR[]"),
    "institution-name": ("institution_names", "VARCHAR[]"),
}


def _sql_str(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _today_version() -> str:
    """Snapshot label — the retrieval date (the registry is live, not released)."""
    return date.today().isoformat()


def _chunks(items: Sequence[str], size: int) -> Iterable[Sequence[str]]:
    """``itertools.batched`` is 3.12+; this package supports 3.11."""
    for start in range(0, len(items), size):
        yield items[start : start + size]


def fetch(
    orcids: Sequence[str], version: str, settings: Settings | None = None
) -> Path:
    """Fetch ``orcids`` via batched ``expanded-search`` → one raw NDJSON file.

    One request per :attr:`Settings.orcid_batch_size` iDs. The file is rewritten
    each call (unlike a versioned bulk download, the registry moves under us and
    the iD set is caller-chosen, so a same-day cache would be a trap).
    """
    s = settings or get_settings()
    dest = raw_dir(_RAW, s) / f"{version}-person.ndjson"
    found = 0
    with open(dest, "w", encoding="utf-8") as fh:
        for chunk in _chunks(orcids, s.orcid_batch_size):
            payload = get_json(
                f"{s.orcid_api_base}/expanded-search/",
                params={"q": "orcid:(" + " OR ".join(chunk) + ")", "rows": str(len(chunk))},
                headers={"Accept": "application/json"},
            )
            records = payload["expanded-result"] or []
            for record in records:
                fh.write(json.dumps(record) + "\n")
            found += len(records)
    _log.info("fetched {:,}/{:,} requested iDs → {}", found, len(orcids), dest.name)
    return dest


def _select_sql(path: Path, version: str, limit: int | None) -> str:
    columns_sql = ", ".join(f"'{src}': '{typ}'" for src, (_, typ) in COLUMNS.items())
    # Trim the scalar name columns (upstream is clean today; this is what keeps
    # a padded name from silently breaking a join later). List columns land
    # verbatim. `orcid_id` can never be NULL in a real response, and a NULL key
    # would be a meaningless row here, so drop those outright.
    projection = ",\n            ".join(
        f'trim("{src}") AS {dst}' if typ == "VARCHAR" else f'"{src}" AS {dst}'
        for src, (dst, typ) in COLUMNS.items()
    )
    limit_sql = f" LIMIT {int(limit)}" if limit else ""
    return f"""
        SELECT
            {projection},
            CAST({_sql_str(version)} AS VARCHAR) AS snapshot_version
        FROM read_json(
            {_sql_str(str(path))}, format = 'newline_delimited',
            columns = {{{columns_sql}}}, auto_detect = false
        )
        WHERE nullif(trim("orcid-id"), '') IS NOT NULL
        {limit_sql}
    """


def land(
    con: duckdb.DuckDBPyConnection,
    path: Path,
    version: str,
    *,
    schema: str = "orcid",
    limit: int | None = None,
) -> int:
    """MERGE-upsert a raw expanded-search NDJSON into ``lake.<schema>.person``."""
    return upsert(
        con, f"{LAKE}.{schema}.person", _select_sql(path, version, limit),
        key="orcid_id", exclude_change_cols=["snapshot_version"],
    )


def resolve_orcids(
    con: duckdb.DuckDBPyConnection,
    *,
    orcids: str | None,
    orcids_sql: str | None,
) -> list[str]:
    """The iD set to fetch: an explicit list, or the first column of ``orcids_sql``.

    Refuses to default. No table in this lake carries an ORCID iD yet (see the
    module docstring), so an implicit "everything referenced" would quietly mean
    "nothing" — the failure #56 warns against defaulting silently into.
    """
    if orcids:
        ids = [o.strip() for o in orcids.split(",") if o.strip()]
    elif orcids_sql:
        ids = [row[0] for row in con.execute(orcids_sql).fetchall() if row[0]]
    else:
        raise ValueError(
            "no ORCID iDs to fetch — pass orcids='0000-...,0000-...' or "
            "orcids_sql='SELECT DISTINCT <col> FROM lake....'. This source is "
            "demand-driven on purpose: the annual bulk file is ~863 GB of "
            "per-record XML and nothing in this lake carries an ORCID iD yet "
            "(see the module docstring)."
        )
    return sorted(set(ids))


def ingest(
    *,
    orcids: str | None = None,
    orcids_sql: str | None = None,
    file: str | None = None,
    version: str | None = None,
    schema: str = "orcid",
    limit: int | None = None,
    settings: Settings | None = None,
) -> dict:
    """End-to-end: resolve iDs → batched API fetch (unless ``file``) → upsert → summary.

    ``orcids`` is a comma-separated iD list; ``orcids_sql`` is any query against
    the attached lake returning one column of iDs. ``file`` loads a raw
    expanded-search NDJSON already on disk instead of calling the API.
    """
    s = settings or get_settings()
    version = version or _today_version()
    target = f"{LAKE}.{schema}.person"
    con = lake_connect(s)
    try:
        path = (
            Path(file) if file
            else fetch(resolve_orcids(con, orcids=orcids, orcids_sql=orcids_sql), version, s)
        )
        with ops.run(con, source="orcid", target=target, version=version) as r:
            r.rows = land(con, path, version, schema=schema, limit=limit)
            _log.info("person <- {:,} rows ({})", r.rows, version)
    finally:
        con.close()
    return r.summary()
