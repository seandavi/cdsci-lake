"""Ingest NIH RePORTER ExPORTER project files into ``lake.reporter_projects``.

Per-fiscal-year zips (one CSV each) land in the raw layer; the curated table is
rebuilt from **all** years present, so adding a year is a download + re-curate
with no re-fetch of the others (ADR-0012). One row per project-year award
(``appl_id``), matching how RePORTER itself models year-specific awards.

ExPORTER CSVs are UTF-8 and loosely quoted, and the column set drifts across
fiscal years (e.g. ``FOA_NUMBER`` became ``OPPORTUNITY NUMBER``). We scan as text
with lenient options and ``union_by_name`` (so a column absent from one year's
file reads as NULL rather than failing), then cast in SQL.
"""

from __future__ import annotations

from pathlib import Path

import duckdb

from ...config import Settings, get_settings
from ...download import download, post_json, unzip
from ...lake import LAKE, csv_source, lake_connect, raw_dir

_TABLE = "reporter_projects"
_FILE_GROUP = "PROJECT"


def list_files(settings: Settings | None = None) -> list[dict]:
    """Return ExPORTER's project-file catalog (one record per fiscal year).

    Each record carries ``fy``, ``file_name``, and the ``doc_type_code`` /
    ``doc_key_id`` pair the download endpoint keys on.
    """
    s = settings or get_settings()
    files = post_json(s.exporter_files_api, {"file_group": _FILE_GROUP})
    return files if isinstance(files, list) else []


def download_years(years: list[int], settings: Settings | None = None) -> list[Path]:
    """Download + unzip ExPORTER project files for ``years``; return CSV paths."""
    s = settings or get_settings()
    by_fy = {int(f["fy"]): f for f in list_files(s) if f.get("fy")}
    missing = [y for y in years if y not in by_fy]
    if missing:
        raise ValueError(f"No ExPORTER project file for fiscal year(s): {missing}")

    csvs: list[Path] = []
    for year in years:
        rec = by_fy[year]
        dest = raw_dir(_TABLE, s) / rec.get("file_name", f"RePORTER_PRJ_C_FY{year}.zip")
        archive = download(
            s.exporter_download_url,
            dest,
            params={"DocType": rec["doc_type_code"], "KeyId": rec["doc_key_id"]},
        )
        csvs.extend(p for p in unzip(archive, raw_dir(_TABLE, s) / str(year)) if _is_csv(p))
    return csvs


def _is_csv(path: Path) -> bool:
    return path.suffix.lower() == ".csv"


def curate(
    con: duckdb.DuckDBPyConnection,
    csv_paths: list[Path] | str,
    *,
    limit: int | None = None,
) -> int:
    """Build ``lake.reporter_projects`` from the ExPORTER CSV(s); return row count.

    Projects a stable subset of the (wide, drifting) ExPORTER schema. DuckDB
    identifiers are case-insensitive, so the upstream ``PI_NAMEs`` header is read
    as ``pi_names``.
    """
    source = csv_source(csv_paths)
    limit_sql = f" LIMIT {int(limit)}" if limit else ""
    con.execute(
        f"""
        CREATE OR REPLACE TABLE {LAKE}.{_TABLE} AS
        SELECT
            TRY_CAST(application_id AS BIGINT)   AS appl_id,
            core_project_num                     AS core_project_num,
            full_project_num                     AS project_num,
            TRY_CAST(fy AS INTEGER)              AS fiscal_year,
            activity                             AS activity_code,
            application_type                     AS application_type,
            administering_ic                     AS admin_ic,
            ic_name                              AS ic_name,
            funding_mechanism                    AS funding_mechanism,
            TRY_CAST(total_cost AS DOUBLE)       AS total_cost,
            TRY_CAST(direct_cost_amt AS DOUBLE)  AS direct_cost,
            TRY_CAST(indirect_cost_amt AS DOUBLE) AS indirect_cost,
            project_title                        AS project_title,
            project_start                        AS project_start,
            project_end                          AS project_end,
            org_name                             AS org_name,
            org_city                             AS org_city,
            org_state                            AS org_state,
            org_country                          AS org_country,
            "OPPORTUNITY NUMBER"                  AS foa_number,
            pi_ids                               AS pi_ids,
            pi_names                             AS pi_names,
            TRY_CAST(support_year AS INTEGER)    AS support_year
        FROM read_csv(
            {source},
            header = true, all_varchar = true, sample_size = -1,
            quote = '"', escape = '"', union_by_name = true,
            ignore_errors = true, null_padding = true
        )
        WHERE TRY_CAST(application_id AS BIGINT) IS NOT NULL{limit_sql};
        """
    )
    return con.execute(f"SELECT count(*) FROM {LAKE}.{_TABLE}").fetchone()[0]


def ingest(
    *,
    years: list[int] | None = None,
    files: list[str] | str | None = None,
    limit: int | None = None,
    settings: Settings | None = None,
) -> dict:
    """End-to-end: (download ``years`` unless ``files`` given) → curate → summary."""
    s = settings or get_settings()
    if files:
        paths: list[Path] | str = files if isinstance(files, str) else [Path(f) for f in files]
    elif years:
        paths = download_years(years, s)
    else:
        raise ValueError("Provide either years=[...] or files=[...].")
    con = lake_connect(s)
    try:
        rows = curate(con, paths, limit=limit)
        years_loaded = con.execute(
            f"SELECT DISTINCT fiscal_year FROM {LAKE}.{_TABLE} ORDER BY 1"
        ).fetchall()
    finally:
        con.close()
    return {
        "table": f"{LAKE}.{_TABLE}",
        "rows": rows,
        "fiscal_years": [y[0] for y in years_loaded],
    }
