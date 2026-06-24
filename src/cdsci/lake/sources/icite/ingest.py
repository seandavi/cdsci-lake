"""Ingest the monthly iCite bulk snapshot into ``lake.icite``.

Three steps, medallion-style (ADR-0012):

#. **resolve** — find the latest snapshot on figshare (or accept an explicit
   version / a local file, so a mirror or an offline dump works too).
#. **download** — fetch the metadata archive into the raw layer, resumably, and
   unzip it. Re-running does not re-download.
#. **curate** — DuckDB scans the CSV and writes the typed ``lake.icite`` table.

The curated projection is **join-focused**: identifiers + the impact metrics
(RCR, percentile, counts, field rate). The bulky ``cited_by`` / ``references``
PMID-list columns are intentionally left out of this table — they belong in a
separate citation-graph table if/when a consumer needs them.
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


def _select_sql(csv_paths: list[Path] | str, version: str, limit: int | None) -> str:
    """Typed, join-focused projection over the iCite metadata CSV.

    Reads every column as text (``all_varchar``) and casts in SQL, so a monthly
    schema tweak upstream never breaks the scan. The bulky ``cited_by`` /
    ``references`` PMID-list columns are intentionally dropped (they belong in a
    citation-graph / open-citations table, not the per-paper metrics table).
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
            CAST({_sql_str(version)} AS VARCHAR)              AS snapshot_version
        FROM read_csv(
            {csv_source(csv_paths)},
            header = true, all_varchar = true, sample_size = -1,
            quote = '"', escape = '"', ignore_errors = true
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
    """
    target = target or f"{LAKE}.{_TABLE}"
    return upsert(
        con, target, _select_sql(csv_paths, version, limit),
        key="pmid", exclude_change_cols=["snapshot_version"],
    )


def ingest(
    *,
    file: str | None = None,
    version: str | None = None,
    schema: str = "icite",
    limit: int | None = None,
    settings: Settings | None = None,
) -> dict:
    """End-to-end: (download unless ``file`` given) → upsert → return a summary."""
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
    finally:
        con.close()
    return r.summary()
