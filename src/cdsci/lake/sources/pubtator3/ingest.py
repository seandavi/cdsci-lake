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

**Checked whole before anything lands**: every physical line has exactly five
fields (counted through the gzip, so a blank line fails: DuckDB's read would skip
it and renumber every line after it), the Type column is the file's own kind on
every row, every PMID is a number, and a file with no rows is refused. The checked
line count is the extent a full load is authoritative for: lines past it are
deleted first, so a shrinking dump also clears a batch slice it no longer reaches.
That delete and every batch's merge are one transaction per table (one snapshot):
a failure part way leaves the table as it was.

**One batch by default.** ``batches`` shards the load by ``line_number % batches``,
the memory lever ``ncbi_gene.land`` uses, but each batch is one more single-threaded
pass over the gzip and one more MERGE and prune read of the whole target. Measured
on species (49.0M rows, local scratch lake, 4 threads, 2026-11-04): an unchanged
re-run takes 65 s at 12.8 GB peak RSS with ``batches=1`` and 346 s at 2.0 GB with
20, of which 279 s is the 20 gzip passes; a new dump (every line rewritten) takes
102 s at 16.4 GB with 1 and 194 s at 8.2 GB with 4. Disease (168M) with 1: 233 s
at 47.8 GB unchanged, 367 s at 59.3 GB on a new dump.

``mode="append"`` skips the MERGE's read of the growing target for the initial
bulk load, and refuses a slice that already holds rows. ``mode="merge"`` (the default) is idempotent and prunes lines a
new dump no longer has. ``pubtator3_version`` is excluded from change detection
(ADR-0003), so identical content retrieved on another date writes nothing and the
stamp is the retrieval date of the dump a row last changed in.

License: US Government Work (NCBI/NLM), public domain, per the directory's
README.txt.
"""

from __future__ import annotations

import gzip
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


def _check(con: duckdb.DuckDBPyConnection, kind: str, path: Path) -> int:
    """Two passes over the whole file: physical field counts, then the Type column and
    numeric PMIDs. A file with no rows is refused. Returns the line count."""
    n = 0
    with gzip.open(path, "rb") as f:
        for n, line in enumerate(f, 1):
            if (fields := line.count(b"\t") + 1) != len(COLUMNS):
                raise ValueError(f"pubtator3: {path} line {n} has {fields} fields, "
                                 f"not {len(COLUMNS)}")
    if not n:
        raise ValueError(f"pubtator3: {path} has zero rows")
    bad = con.execute(
        f"SELECT count(*) FILTER (type IS DISTINCT FROM '{KINDS[kind]}' "
        f"OR TRY_CAST(pmid AS BIGINT) IS NULL) FROM {_read(path)}").fetchone()[0]
    if bad:
        raise ValueError(f"pubtator3: {bad:,} rows of {path} have a Type other than "
                         f"'{KINDS[kind]}' or a non-numeric PMID")
    return n


def _select_sql(path: Path, version: str, limit: int | None,
                batch: tuple[int, int] | None) -> str:
    # The CSV scan is order-preserving and row_number() OVER () streams in that order,
    # so line_number n is the n-th line (every line is one record, checked above).
    where = f" WHERE line_number % {int(batch[1])} = {int(batch[0])}" if batch else ""
    limit_sql = f" LIMIT {int(limit)}" if limit is not None else ""
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
    # limit=0 is falsy: tested by truth it would read every row and prune as a full load
    if limit is not None and limit < 1:
        raise ValueError(f"pubtator3: limit must be positive, got {limit}")
    target = f"{LAKE}.{schema}.{kind}_annotation"
    sql = _select_sql(path, version, limit, batch)
    slice_sql = f"line_number % {int(batch[1])} = {int(batch[0])}" if batch else "true"
    if mode == "merge":
        return upsert(con, target, sql, "line_number", exclude_change_cols=["pubtator3_version"],
                      prune=None if limit is not None else slice_sql)
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


def _drop_lines_past(con: duckdb.DuckDBPyConnection, target: str, n: int) -> None:
    """Delete the lines past a checked dump's ``n``, before its batches merge.

    A batch slice the dump no longer reaches (gene shrunk to 2 lines, ``batches=3``:
    ``line_number % 3 = 0``) loads zero rows, which ``upsert`` refuses to prune.
    Emptied here, the slice is an empty load over an empty slice: a no-op.
    """
    catalog, schema, table = target.split(".")
    if con.execute("SELECT 1 FROM information_schema.tables WHERE table_catalog = ? "
                   "AND table_schema = ? AND table_name = ?", [catalog, schema, table]).fetchone():
        con.execute(f"DELETE FROM {target} WHERE line_number > {int(n)};")


def ingest(
    *,
    directory: str | None = None,
    kind: str | None = None,
    version: str | None = None,
    schema: str = "pubtator3",
    limit: int | None = None,
    batches: int = 1,
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
    if limit is not None and limit < 1:
        raise ValueError(f"pubtator3: limit must be positive, got {limit}")
    # batches=0 would delete the extent and merge nothing
    if type(batches) is not int or batches < 1:
        raise ValueError(f"pubtator3: batches must be a positive int, got {batches!r}")
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
            lines = {k: _check(con, k, root / file_name(k)) for k in kinds}
            for k in kinds:
                # one transaction: the batches' upserts join it (attribute is re-entrant),
                # so the extent delete never commits without the merges that follow it
                with r.attribute(f"{k}_annotation"):
                    if mode == "merge" and limit is None:
                        _drop_lines_past(con, f"{LAKE}.{schema}.{k}_annotation", lines[k])
                    for i in range(batches):
                        counts[f"{k}_annotation"] = land(
                            con, k, root / file_name(k), version, schema=schema, limit=limit,
                            batch=(i, batches) if batches > 1 else None, mode=mode)
                _log.info("{}_annotation <- {:,} rows", k, counts[f"{k}_annotation"])
            r.rows = sum(counts.values())
    finally:
        con.close()
    return {**r.summary(), "schema": f"{LAKE}.{schema}", "counts": counts}
