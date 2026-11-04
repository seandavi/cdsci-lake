"""PubTator3 -> ``lake.pubtator3.<kind>_annotation`` (EL only; bioc-on-ice#183).

PubTator3 (Lu lab, NCBI) publishes one complete headerless TSV per entity type,
regenerated together about monthly under ``ftp.ncbi.nlm.nih.gov/pub/lu/PubTator3/``:
PMID, Type, Concept ID, Mentions, Resource. 446,876,903 rows across the five
files bioc-on-ice landed on 2026-09-19 (gene 74.5M, disease 168.3M, chemical
147.0M, species 49.0M, mutation 8.1M; 5.4 GB gzipped at 2026-10-06). Ported
from bioc-on-ice's ``src/bioconice/pubtator3.py`` (its ``raw.pubtator3__<kind>``
landing); bioc-on-ice derives ``annotation.pubtator3__mention`` from these
tables at release time (bioc-on-ice ADR-0015).

**Unversioned**, like ``ncbi_gene``: the files are rewritten in place and none
is archived, so the version is the retrieval date. One table per file, so a
file lands, fails and re-lands on its own.

**Every row lands verbatim and nothing else is read as NULL but the empty
string.** ``-`` is the concept id of a mention PubTator3 recognised but could
not normalise (10.7M chemical rows), so NCBI's usual ``nullstr='-'`` would
erase real data. Quoting is off: mentions carry stray ``"`` ('Toll"-like
receptor'). Mentions are empty on rows a curated resource asserts with no
text hit.

**The files declare no key**, so each table is keyed by ``line_number``, the
1-based line in the file. (pmid, concept_id) was unique on 1.9M gene and 1.3M
mutation lines (prefixes, 2026-11-03) but is not checked on whole files.

**Checked whole before anything lands**: every line has exactly five fields
(DuckDB's strict read), the Type column is the file's own kind on every row,
every PMID is a number, and a file with no rows is refused.

**The load is sharded** by ``line_number % batches``, the memory lever
``ncbi_gene.land`` uses: each batch is one more pass over the gzip (~200 MB/s
of TSV measured on a gene prefix, ~90 s for all five files) and a stage of
~1/``batches`` of the file. ``mode="append"`` skips the MERGE's read of the
growing target for the initial bulk load, and refuses a slice that already
holds rows. ``mode="merge"`` (the default) is idempotent and prunes lines a
new dump no longer has; ``pubtator3_version`` is compared like any column, so
a new dump restamps every row and a table is one dump, never a mix.

License: US Government Work (NCBI/NLM), public domain, per the directory's
README.txt.
"""

from __future__ import annotations

import re
from datetime import date
from pathlib import Path

import duckdb

from ... import ops
from ...config import Settings, get_settings
from ...connect import LAKE, csv_source, lake_connect, raw_dir, upsert
from ...download import download
from ...log import logger

_RAW = "pubtator3"
_log = logger.bind(ctx="pubtator3")

# File stem -> the value its Type column carries on every row.
KINDS = {"gene": "Gene", "disease": "Disease", "chemical": "Chemical",
         "species": "Species", "mutation": "Mutation"}
# In file order; there is no header.
COLUMNS = ("pmid", "type", "concept_id", "mentions", "resource")


def file_name(kind: str) -> str:
    return f"{kind}2pubtator3.gz"


def download_dumps(version: str, settings: Settings | None = None) -> Path:
    """Download the five dumps into ``raw/pubtator3/<version>/`` under their upstream names."""
    s = settings or get_settings()
    dest = raw_dir(_RAW, s) / version
    for kind in KINDS:
        download(f"{s.pubtator3_base_url}{file_name(kind)}", dest / file_name(kind))
    return dest


def _read(path: Path) -> str:
    columns = "{" + ", ".join(f"'{c}': 'VARCHAR'" for c in COLUMNS) + "}"
    return (f"read_csv({csv_source([path])}, delim='\\t', header=false, auto_detect=false, "
            f"columns={columns}, quote='', escape='', nullstr='')")


def _check(con: duckdb.DuckDBPyConnection, kind: str, path: Path) -> None:
    """One pass over the whole file: field counts, the Type column, numeric PMIDs, not empty."""
    n, bad = con.execute(
        f"SELECT count(*), count(*) FILTER (type IS DISTINCT FROM '{KINDS[kind]}' "
        f"OR TRY_CAST(pmid AS BIGINT) IS NULL) FROM {_read(path)}").fetchone()
    if not n:
        raise ValueError(f"pubtator3: {path} has zero rows")
    if bad:
        raise ValueError(f"pubtator3: {bad:,} rows of {path} have a Type other than "
                         f"'{KINDS[kind]}' or a non-numeric PMID")


def _select_sql(path: Path, version: str, limit: int | None,
                batch: tuple[int, int] | None) -> str:
    # The CSV scan is order-preserving and row_number() OVER () streams in that order,
    # so line_number n is the n-th line (every line is one record, checked above).
    where = f" WHERE line_number % {int(batch[1])} = {int(batch[0])}" if batch else ""
    limit_sql = f" LIMIT {int(limit)}" if limit else ""
    return (f"SELECT * FROM (SELECT row_number() OVER ()::INTEGER AS line_number, *, "
            f"'{version}' AS pubtator3_version FROM {_read(path)}){where}{limit_sql}")


def land(
    con: duckdb.DuckDBPyConnection,
    kind: str,
    path: Path,
    version: str,
    *,
    schema: str = "pubtator3",
    limit: int | None = None,
    batch: tuple[int, int] | None = None,
    mode: str = "merge",
) -> int:
    """Land one batch of one dump into ``lake.<schema>.<kind>_annotation``; return its rows.

    A full merge load prunes lines the dump no longer has, scoped to its ``batch``
    slice; ``limit`` and ``mode="append"`` never prune.
    """
    target = f"{LAKE}.{schema}.{kind}_annotation"
    sql = _select_sql(path, version, limit, batch)
    slice_sql = f"line_number % {int(batch[1])} = {int(batch[0])}" if batch else "true"
    if mode == "merge":
        return upsert(con, target, sql, "line_number", prune=None if limit else slice_sql)
    if mode != "append":
        raise ValueError(f"mode must be 'merge' or 'append', got {mode!r}")
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {LAKE}.{schema};")
    con.execute(f"CREATE OR REPLACE TEMP TABLE _pubtator3_stage AS {sql};")
    con.execute(f"CREATE TABLE IF NOT EXISTS {target} AS "
                "SELECT * FROM _pubtator3_stage WHERE false;")
    # A re-run append after a failure part way would land the slice twice.
    if con.execute(f"SELECT count(*) FROM {target} WHERE {slice_sql}").fetchone()[0]:
        raise ValueError(f"pubtator3: {target} already holds rows where {slice_sql}; "
                         "append is for an empty table, re-run with mode='merge'")
    con.execute(f"INSERT INTO {target} SELECT * FROM _pubtator3_stage;")
    return con.execute(f"SELECT count(*) FROM {target}").fetchone()[0]


def ingest(
    *,
    directory: str | None = None,
    kind: str | None = None,
    version: str | None = None,
    schema: str = "pubtator3",
    limit: int | None = None,
    batches: int = 20,
    mode: str = "merge",
    settings: Settings | None = None,
) -> dict:
    """End-to-end: download the dumps (unless ``directory``) -> check all -> batched land.

    ``directory`` holds local ``<kind>2pubtator3.gz`` files under their upstream names
    and then needs ``version``: a local copy has no retrieval date. ``kind`` lands one
    file; omit it for all five. Every file is checked before the first lands.
    ``batches`` shards the load by ``line_number % batches``; 1 disables it.
    """
    if kind is not None and kind not in KINDS:
        raise ValueError(f"pubtator3: kind must be one of {list(KINDS)}, got {kind!r}")
    # limit=0 is falsy: it would read every row and prune as a full load
    if limit is not None and limit < 1:
        raise ValueError(f"pubtator3: limit must be positive, got {limit}")
    if directory and not version:
        raise ValueError("pubtator3: a local directory has no retrieval date; pass version")
    version = version or date.today().isoformat()
    # the version is interpolated into a path and the SQL
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", version):
        raise ValueError(f"pubtator3: version {version!r} is not a date YYYY-MM-DD")
    s = settings or get_settings()
    kinds = [kind] if kind else list(KINDS)
    root = Path(directory) if directory else download_dumps(version, s)
    con = lake_connect(s)
    try:
        counts: dict[str, int] = {}
        with ops.run(con, source="pubtator3", target=f"{LAKE}.{schema}", version=version) as r:
            for k in kinds:
                _check(con, k, root / file_name(k))
            for k in kinds:
                for i in range(batches):
                    counts[f"{k}_annotation"] = land(
                        con, k, root / file_name(k), version, schema=schema, limit=limit,
                        batch=(i, batches) if batches > 1 else None, mode=mode)
                _log.info("{}_annotation <- {:,} rows", k, counts[f"{k}_annotation"])
            r.rows = sum(counts.values())
    finally:
        con.close()
    return {**r.summary(), "schema": f"{LAKE}.{schema}", "counts": counts}
