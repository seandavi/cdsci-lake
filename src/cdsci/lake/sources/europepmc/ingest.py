"""Ingest Europe PMC **text-mined annotations** into one tidy lake table.

Europe PMC publishes a directory of per-database CSVs at
``europepmc.org/pub/databases/pmc/TextMinedTerms/`` — one file per annotated
resource (``uniprot.csv``, ``chebi.csv``, ``nct.csv``, ``gen.csv``, …). Every file
has the **same shape**:

    <database>,PMCID,EXTID,SOURCE
    "MINT-1777462",PMC3340672,22553621,MED

i.e. the first column is the accession/term id (its header is the database name),
then the PMC article id, then the article's external id and its namespace
(``SOURCE='MED'`` ⟹ ``EXTID`` is a PubMed id).

Because the files share a schema, they collapse into **one tidy table**
``lake.europepmc.annotations`` keyed by ``(database, accession, pmcid)`` — the
``database`` (= file stem) is promoted to a column so all resources live together.
``pmcid`` links to ``pmc.documents``; ``pmid`` (the MED ``EXTID``) bridges to
``icite`` / ``reporter.publink`` / omicidx. MERGE-upsert per file keeps monthly
refreshes to real deltas (ADR-0003); the run is recorded via ``ops.run`` (ADR-0006).
"""

from __future__ import annotations

import re
from datetime import date
from pathlib import Path

import duckdb
import httpx

from ... import ops
from ...config import Settings, get_settings
from ...connect import LAKE, csv_source, lake_connect, raw_dir, upsert
from ...download import download
from ...log import logger

_TABLE = "europepmc.annotations"  # lake table (schema.table)
_RAW = "europepmc"  # raw-download subdir name
_TIMEOUT = httpx.Timeout(60.0, read=120.0)
_log = logger.bind(ctx="europepmc")


def _sql_str(value: str) -> str:
    """Quote a value as a SQL string literal."""
    return "'" + value.replace("'", "''") + "'"


def _today_version() -> str:
    """Default snapshot label — the load date (override with ``--version``)."""
    return date.today().isoformat()


def list_databases(settings: Settings | None = None) -> list[str]:
    """Scrape the TextMinedTerms directory index for the available database CSVs.

    Returns the sorted file stems (e.g. ``['alphafold', 'arrayexpress', …]``), so
    a database added upstream is picked up without a code change.
    """
    s = settings or get_settings()
    with httpx.Client(timeout=_TIMEOUT, follow_redirects=True) as client:
        resp = client.get(s.europepmc_textmined_url)
        resp.raise_for_status()
        html = resp.text
    return sorted(set(re.findall(r'href="([^"/]+)\.csv"', html)))


def download_database(
    database: str, version: str, settings: Settings | None = None
) -> Path:
    """Download one database's CSV into the raw layer (resumably). Returns the path."""
    s = settings or get_settings()
    url = f"{s.europepmc_textmined_url.rstrip('/')}/{database}.csv"
    dest = raw_dir(_RAW, s) / f"{version}-{database}.csv"
    return download(url, dest)


def _select_sql(database: str, path: Path, version: str, limit: int | None) -> str:
    """Typed, deduped projection of one database CSV.

    Read positionally (``header=false, skip=1``) so the per-file first-column
    header (the database name) doesn't matter. Group by the natural key so the
    staged rows are unique (a file may repeat a term for an article); ``pmid`` is
    the MED external id (the PubMed id), NULL for non-MED annotations.
    """
    limit_sql = f" LIMIT {int(limit)}" if limit else ""
    return f"""
        SELECT
            {_sql_str(database)}                                       AS database,
            accession,
            pmcid,
            max(TRY_CAST(ext_id AS BIGINT)) FILTER (WHERE ext_source = 'MED') AS pmid,
            CAST({_sql_str(version)} AS VARCHAR)                       AS snapshot_version
        FROM read_csv(
            {csv_source([path])},
            header = false, skip = 1,
            columns = {{'accession': 'VARCHAR', 'pmcid': 'VARCHAR',
                       'ext_id': 'VARCHAR', 'ext_source': 'VARCHAR'}},
            quote = '"', escape = '"', ignore_errors = true
        )
        WHERE accession IS NOT NULL AND pmcid IS NOT NULL
        GROUP BY accession, pmcid{limit_sql}
    """


def curate(
    con: duckdb.DuckDBPyConnection,
    database: str,
    path: Path,
    version: str,
    *,
    target: str | None = None,
    limit: int | None = None,
) -> int:
    """Upsert one database's CSV into ``europepmc.annotations``; return its row count."""
    target = target or f"{LAKE}.{_TABLE}"
    upsert(
        con, target, _select_sql(database, path, version, limit),
        key=["database", "accession", "pmcid"], exclude_change_cols=["snapshot_version"],
    )
    return con.execute(
        f"SELECT count(*) FROM {target} WHERE database = ?", [database]
    ).fetchone()[0]


def ingest(
    *,
    database: str | None = None,
    file: str | None = None,
    version: str | None = None,
    schema: str = "europepmc",
    limit: int | None = None,
    settings: Settings | None = None,
) -> dict:
    """End-to-end: download + MERGE every database (or one) into one tidy table.

    ``database`` loads just that one; ``file`` loads a local CSV (stem = database
    unless ``database`` given); omit both to load the full directory.
    """
    s = settings or get_settings()
    version = version or _today_version()
    target = f"{LAKE}.{schema}.annotations"

    if file:
        plan: list[tuple[str, Path | None]] = [(database or Path(file).stem, Path(file))]
    elif database:
        plan = [(database, None)]
    else:
        plan = [(db, None) for db in list_databases(s)]

    con = lake_connect(s)
    try:
        counts: dict[str, int] = {}
        with ops.run(con, source="europepmc", target=target, version=version) as r:
            for db, path in plan:
                path = path or download_database(db, version, s)
                counts[db] = curate(con, db, path, version, target=target, limit=limit)
                _log.info("{} <- {:,} rows", db, counts[db])
            r.rows = sum(counts.values())
    finally:
        con.close()
    return {**r.summary(), "schema": schema, "databases": len(plan), "counts": counts}
