"""Ingest the Retraction Watch database into ``lake.retractionwatch.retractions``.

Retraction Watch is the reference catalogue of retractions, corrections, and
expressions of concern. Crossref hosts it as a **single rolling CSV** (CC0),
updated most weekdays, at
``gitlab.com/crossref/retraction-watch-data`` — ~70k rows, ~65 MB, keyed by
``Record ID``, with several semicolon-separated multi-value fields.

One tidy table, one row per notice (key ``record_id``). The multi-value fields
(subjects, authors, institutions, countries, reasons, urls) become
``VARCHAR[]`` arrays; dates parse from ``M/D/YYYY`` and DOIs are normalized
(lowercased, prefix-stripped) like ``openalex.works``. ``original_paper_doi`` /
``original_paper_pmid`` are the join keys to the retracted literature
(`icite` / `reporter.publink` / `openalex` / omicidx). MERGE-upsert on
``record_id`` is exactly the roadmap's "full-file diff on Record ID": a daily pull
updates changed notices and inserts new ones (ADR-0003). The rolling file has no
upstream version, so we keep each pull's CSV as bronze and tag by pull date.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb

from ... import ops
from ...config import Settings, get_settings
from ...connect import LAKE, csv_source, lake_connect, raw_dir, upsert
from ...download import download

_TABLE = "retractionwatch.retractions"  # lake table (schema.table)
_RAW = "retractionwatch"  # raw-download subdir name


def _sql_str(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _today_version() -> str:
    """Default snapshot label — the pull date (the file has no upstream version)."""
    return date.today().isoformat()


def _arr(col: str) -> str:
    """SQL: a ``;``-separated multi-value column → trimmed, non-empty ``VARCHAR[]``."""
    return (
        f'list_transform(list_filter(string_split(coalesce("{col}", \'\'), \';\'), '
        f"x -> trim(x) <> ''), x -> trim(x))"
    )


def _doi(col: str) -> str:
    """SQL: normalize a DOI — trim, strip resolver prefix, lowercase, NULL sentinels."""
    return (
        f"nullif(nullif(lower(regexp_replace(trim(\"{col}\"), "
        f"'^https?://(dx\\.)?doi\\.org/', '')), ''), 'unavailable')"
    )


def _d(col: str) -> str:
    """SQL: parse the ``M/D/YYYY H:MM`` date column to DATE (NULL on failure)."""
    return f"TRY_STRPTIME(split_part(\"{col}\", ' ', 1), ['%-m/%-d/%Y', '%m/%d/%Y'])::DATE"


def _pmid(col: str) -> str:
    """SQL: PMID as BIGINT, with 0/non-numeric → NULL."""
    return f'nullif(TRY_CAST("{col}" AS BIGINT), 0)'


def download_csv(version: str, settings: Settings | None = None) -> Path:
    """Download the rolling Retraction Watch CSV into the raw layer (kept as bronze)."""
    s = settings or get_settings()
    dest = raw_dir(_RAW, s) / f"{version}-retraction_watch.csv"
    return download(s.retractionwatch_url, dest)


def _select_sql(path: Path, version: str, limit: int | None) -> str:
    limit_sql = f" LIMIT {int(limit)}" if limit else ""
    return f"""
        SELECT
            TRY_CAST("Record ID" AS BIGINT)        AS record_id,
            "Title"                                 AS title,
            {_arr("Subject")}                       AS subjects,
            {_arr("Institution")}                   AS institutions,
            "Journal"                               AS journal,
            "Publisher"                             AS publisher,
            {_arr("Country")}                       AS countries,
            {_arr("Author")}                        AS authors,
            {_arr("URLS")}                          AS urls,
            "ArticleType"                           AS article_type,
            {_d("RetractionDate")}                  AS retraction_date,
            {_doi("RetractionDOI")}                 AS retraction_doi,
            {_pmid("RetractionPubMedID")}           AS retraction_pmid,
            {_d("OriginalPaperDate")}               AS original_paper_date,
            {_doi("OriginalPaperDOI")}              AS original_paper_doi,
            {_pmid("OriginalPaperPubMedID")}        AS original_paper_pmid,
            "RetractionNature"                      AS retraction_nature,
            {_arr("Reason")}                        AS reasons,
            nullif(lower(trim("Paywalled")), '') = 'yes'  AS paywalled,
            "Notes"                                 AS notes,
            CAST({_sql_str(version)} AS VARCHAR)    AS snapshot_version
        FROM read_csv(
            {csv_source([path])},
            header = true, all_varchar = true, sample_size = -1,
            quote = '"', escape = '"', ignore_errors = true
        )
        WHERE TRY_CAST("Record ID" AS BIGINT) IS NOT NULL{limit_sql}
    """


def curate(
    con: duckdb.DuckDBPyConnection,
    path: Path,
    version: str,
    *,
    target: str | None = None,
    limit: int | None = None,
) -> int:
    """MERGE-upsert the Retraction Watch CSV into ``retractions`` on ``record_id``."""
    target = target or f"{LAKE}.{_TABLE}"
    return upsert(
        con, target, _select_sql(path, version, limit),
        key="record_id", exclude_change_cols=["snapshot_version"],
    )


def ingest(
    *,
    file: str | None = None,
    version: str | None = None,
    schema: str = "retractionwatch",
    limit: int | None = None,
    settings: Settings | None = None,
) -> dict:
    """End-to-end: download (unless ``file``) → MERGE-upsert → return a summary."""
    s = settings or get_settings()
    version = version or _today_version()
    path = Path(file) if file else download_csv(version, s)
    target = f"{LAKE}.{schema}.retractions"
    con = lake_connect(s)
    try:
        with ops.run(con, source="retractionwatch", target=target, version=version) as r:
            r.rows = curate(con, path, version, target=target, limit=limit)
    finally:
        con.close()
    return r.summary()
