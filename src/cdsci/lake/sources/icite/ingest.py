"""Ingest the monthly iCite bulk snapshot into ``lake.icite``.

Three steps, medallion-style (ADR-0012):

#. **resolve** — find the latest snapshot on figshare (or accept an explicit
   version / a local file, so a mirror or an offline dump works too).
#. **download** — fetch the metadata archive into the raw layer, resumably, and
   unzip it. Re-running does not re-download.
#. **curate** — DuckDB scans the CSV and writes the typed ``lake.icite`` table.

``icite.metadata`` leads with the **join-focused** typed projection (identifiers +
the impact metrics); those 17 columns are a contract other readers depend on. The
rest of the snapshot's columns follow, typed (bioc-on-ice#183): ``authors``, the
translation fields, the PMID lists as ``BIGINT[]`` and ``last_modified``. It mirrors
the latest snapshot: a PMID iCite withdrew is pruned. ``icite.citation`` is the
graph exploded from ``references``.
"""

from __future__ import annotations

from pathlib import Path

import duckdb

from ... import ops
from ...config import Settings, get_settings
from ...connect import LAKE, csv_source, lake_connect, raw_dir, upsert
from ...download import download, get_json, unzip

_FIGSHARE = "https://api.figshare.com/v2"
_TABLE = "icite.metadata"  # lake table (schema.table)
SHARDS = 16  # icite.citation is merged one cited_pmid % SHARDS slice at a time

# Columns after the original 17 (bioc-on-ice#183): added in place to an older table.
ADDED = {"authors": "VARCHAR", "molecular_cellular": "DOUBLE", "provisional": "BOOLEAN",
         "x_coord": "DOUBLE", "y_coord": "DOUBLE", "cited_by_clin": "BIGINT[]",
         "cited_by": "BIGINT[]", "references": "BIGINT[]", "last_modified": "TIMESTAMPTZ"}
_RAW = "icite"  # raw-download subdir name


def _sql_str(value: str) -> str:
    """Quote a value as a SQL string literal (paths/versions are trusted, but safe)."""
    return "'" + value.replace("'", "''") + "'"


def resolve_latest(settings: Settings | None = None) -> dict:
    """Return ``{version, files}`` for the most recent iCite figshare snapshot.

    ``files`` is the figshare file list (each has ``name``/``download_url``/
    ``size``). ``version`` is the snapshot label, e.g. ``2026-05``.
    """
    s = settings or get_settings()
    articles = get_json(
        f"{_FIGSHARE}/collections/{s.icite_figshare_collection}/articles",
        params={"page_size": 50, "order": "published_date", "order_direction": "desc"},
    )
    if not isinstance(articles, list) or not articles:
        raise RuntimeError("No articles in the iCite figshare collection.")
    snap = next((a for a in articles if "icite" in a.get("title", "").lower()), articles[0])
    detail = get_json(f"{_FIGSHARE}/articles/{snap['id']}")
    version = snap.get("title", "").split()[-1] or str(snap["id"])
    return {"version": version, "files": detail.get("files", [])}


def _pick_metadata_file(files: list[dict]) -> dict:
    """Choose the metadata archive (the per-paper metrics) from a snapshot's files."""
    metas = [f for f in files if "metadata" in f.get("name", "").lower()]
    candidates = metas or files
    # Prefer an archive/CSV/TSV over a README.
    for ext in (".zip", ".csv.gz", ".tsv.gz", ".csv", ".tsv"):
        for f in candidates:
            if f.get("name", "").lower().endswith(ext):
                return f
    raise RuntimeError(f"No metadata file found among: {[f.get('name') for f in files]}")


def download_snapshot(
    version: str | None = None, settings: Settings | None = None
) -> tuple[str, list[Path]]:
    """Download + unzip the latest (or named) iCite metadata snapshot.

    Returns ``(version, csv_paths)`` — the data files DuckDB will scan.
    """
    s = settings or get_settings()
    latest = resolve_latest(s)
    version = version or latest["version"]
    meta = _pick_metadata_file(latest["files"])
    dest = raw_dir(_RAW, s) / f"{version}-{meta['name']}"
    archive = download(meta["download_url"], dest)
    if archive.suffix == ".zip":
        extract_to = raw_dir(_RAW, s) / version
        return version, [p for p in unzip(archive, extract_to) if _is_data(p)]
    return version, [archive]


def _is_data(path: Path) -> bool:
    return path.suffix.lower() in {".csv", ".tsv", ".gz"}


def _pmids(col: str) -> str:
    """A space-separated PMID list as BIGINT[]; NULL when upstream prints none."""
    return f"TRY_CAST(string_split(NULLIF({col}, ''), ' ') AS BIGINT[])"


def _select_sql(csv_paths: list[Path] | str, version: str, limit: int | None) -> str:
    """Typed, join-focused projection over the iCite metadata CSV.

    Reads every column as text (``all_varchar``) and casts in SQL, so a monthly
    schema tweak upstream never breaks the scan. ``max_line_size`` because
    ``cited_by`` is a paper's whole citing-PMID list on one line (2.3 MB for the
    most-cited paper in 2026-08), over DuckDB's default; without it
    ``ignore_errors`` silently drops those rows.
    """
    limit_sql = f" LIMIT {int(limit)}" if limit else ""
    return f"""
        SELECT
            TRY_CAST(pmid AS BIGINT)                          AS pmid,
            NULLIF(lower(doi), '')                            AS doi,
            TRY_CAST(year AS INTEGER)                         AS year,
            title,
            journal,
            TRY_CAST(relative_citation_ratio AS DOUBLE)       AS rcr,
            TRY_CAST(nih_percentile AS DOUBLE)                AS nih_percentile,
            TRY_CAST(citation_count AS BIGINT)                AS citation_count,
            TRY_CAST(citations_per_year AS DOUBLE)            AS citations_per_year,
            TRY_CAST(expected_citations_per_year AS DOUBLE)   AS expected_citations_per_year,
            TRY_CAST(field_citation_rate AS DOUBLE)           AS field_citation_rate,
            lower(CAST(is_research_article AS VARCHAR)) IN ('yes','true','1')
                                                              AS is_research_article,
            lower(CAST(is_clinical AS VARCHAR)) IN ('yes','true','1')
                                                              AS is_clinical,
            TRY_CAST(apt AS DOUBLE)                           AS apt,
            TRY_CAST(human AS DOUBLE)                         AS human,
            TRY_CAST(animal AS DOUBLE)                        AS animal,
            CAST({_sql_str(version)} AS VARCHAR)              AS snapshot_version,
            NULLIF(authors, '')                               AS authors,
            TRY_CAST(molecular_cellular AS DOUBLE)            AS molecular_cellular,
            lower(provisional) IN ('yes','true','1')          AS provisional,
            TRY_CAST(x_coord AS DOUBLE)                       AS x_coord,
            TRY_CAST(y_coord AS DOUBLE)                       AS y_coord,
            {_pmids("cited_by_clin")}                         AS cited_by_clin,
            {_pmids("cited_by")}                              AS cited_by,
            {_pmids('"references"')}                          AS "references",
            -- printed as '2026-08-30T03:38:37.156Z[Etc/UTC]'
            TRY_CAST(regexp_replace(last_modified, '\[.*\]$', '') AS TIMESTAMPTZ)
                                                              AS last_modified
        FROM read_csv(
            {csv_source(csv_paths)},
            header = true, all_varchar = true, sample_size = -1,
            quote = '"', escape = '"', ignore_errors = true, max_line_size = 268435456
        )
        WHERE TRY_CAST(pmid AS BIGINT) IS NOT NULL{limit_sql}
    """


def curate(
    con: duckdb.DuckDBPyConnection,
    csv_paths: list[Path] | str,
    version: str,
    *,
    target: str | None = None,
    limit: int | None = None,
) -> int:
    """Upsert the snapshot CSV(s) into ``icite.metadata`` on ``pmid``.

    A monthly snapshot mostly *updates* existing PMIDs (citations accrue) and
    inserts new ones — a textbook MERGE. Keying on ``pmid`` keeps DuckLake
    time-travel meaningful month over month (see :func:`cdsci.lake.upsert`).

    A full load prunes the PMIDs the snapshot no longer has, so the table is the
    snapshot (``limit`` loads do not prune). A table from before ``ADDED`` gains
    those columns in place first.
    """
    target = target or f"{LAKE}.{_TABLE}"
    exists = con.execute(
        "SELECT 1 FROM information_schema.tables WHERE table_catalog = ? "
        "AND table_schema = ? AND table_name = ?", target.split("."),
    ).fetchone()
    if exists:
        for name, sql_type in ADDED.items():
            con.execute(f'ALTER TABLE {target} ADD COLUMN IF NOT EXISTS "{name}" {sql_type}')
    return upsert(
        con, target, _select_sql(csv_paths, version, limit),
        key="pmid", exclude_change_cols=["snapshot_version"], prune=None if limit else "true",
    )


def curate_citations(
    con: duckdb.DuckDBPyConnection,
    *,
    metadata: str | None = None,
    target: str | None = None,
    prune: bool = True,
) -> int:
    """Explode ``metadata``'s ``references`` into ``icite.citation``.

    One row per distinct (citing_pmid, cited_pmid): the NIH
    Open Citation Collection (928,458,585 edges in 2026-08). ``cited_by`` is the
    same graph from the other end, short the edges whose cited paper has no iCite
    record, so it is not used. Exploded once, then merged ``SHARDS`` times on
    ``shard = cited_pmid % SHARDS`` so one MERGE holds ~1/16 of the graph; with
    ``prune`` each shard mirrors the load (pass False for partial loads).
    """
    metadata = metadata or f"{LAKE}.{_TABLE}"
    target = target or f"{LAKE}.icite.citation"
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE _icite_edges AS
        SELECT DISTINCT citing_pmid, cited_pmid, (cited_pmid % {SHARDS})::INTEGER AS shard
        FROM (SELECT pmid AS citing_pmid, unnest("references") AS cited_pmid FROM {metadata})
        WHERE cited_pmid IS NOT NULL
    """)
    rows = 0
    for shard in range(SHARDS):
        rows = upsert(
            con, target, f"SELECT * FROM _icite_edges WHERE shard = {shard}",
            key=["citing_pmid", "cited_pmid"], prune=f"shard = {shard}" if prune else None,
        )
    con.execute("DROP TABLE _icite_edges")
    return rows


def ingest(
    *,
    file: str | None = None,
    version: str | None = None,
    schema: str = "icite",
    limit: int | None = None,
    settings: Settings | None = None,
) -> dict:
    """End-to-end: (download unless ``file`` given) → upsert both tables → a summary."""
    s = settings or get_settings()
    if file:
        paths: list[Path] | str = file
        version = version or "local"
    else:
        version, paths = download_snapshot(version, s)
    target = f"{LAKE}.{schema}.metadata"
    con = lake_connect(s)
    try:
        with ops.run(con, source="icite", target=target, version=version) as r:
            r.rows = curate(con, paths, version, target=target, limit=limit)
        with ops.run(con, source="icite", target=f"{LAKE}.{schema}.citation",
                     version=version) as c:
            c.rows = curate_citations(con, metadata=target,
                                      target=f"{LAKE}.{schema}.citation", prune=not limit)
    finally:
        con.close()
    return {**r.summary(), "citation": c.summary()}
