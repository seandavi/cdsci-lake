"""Ingest ClinicalTrials.gov studies (full JSON) into the lake.

Medallion flow (ADR-0012): capture the full extract to a durable **raw** file
first, then curate the lake from it.

1. **download** — paginate the v2 API (body ``nextPageToken``) and stream every
   study to a local NDJSON file (one full JSON record per line).
2. **materialize raw** — DuckDB writes a compact **bronze Parquet** ``(nct_id,
   record)`` from the NDJSON — the faithful, re-curatable capture (and far cheaper
   to re-scan than 12 GB of NDJSON).
3. **curate** — two silver tables read from the bronze Parquet:
   * ``ctgov.studies`` — typed projection + ``record`` (full JSON), key ``nct_id``.
   * ``ctgov.references`` — ``(nct_id, pmid)`` crosswalk + ref type/citation.

Both MERGE-upsert, so a refresh records only real deltas (time-travel). The flat
CSV export is deliberately avoided — it drops references/PMIDs, results, and the
structured modules.
"""

from __future__ import annotations

import json
from pathlib import Path

import duckdb

from ...config import Settings, get_settings
from ...connect import LAKE, csv_source, lake_connect, raw_dir, upsert
from ...download import get_json

_RAW = "ctgov"
_NCT_J = "json_extract_string(json, '$.protocolSection.identificationModule.nctId')"


def download_studies(
    settings: Settings | None = None, *, max_pages: int | None = None
) -> tuple[Path, int]:
    """Paginate the v2 API and stream every study to a local NDJSON file.

    Returns ``(ndjson_path, n_studies)``. ``max_pages`` caps pages (partial run);
    omit for the full ~590k-study corpus.
    """
    s = settings or get_settings()
    out = raw_dir(_RAW, s) / "studies.ndjson"
    token: str | None = None
    pages = n = 0
    with open(out, "w") as fh:
        while True:
            params: dict[str, str | int] = {"format": "json", "pageSize": s.ctgov_page_size}
            if token:
                params["pageToken"] = token
            data = get_json(s.ctgov_api, params=params)
            for study in data.get("studies", []):
                fh.write(json.dumps(study, separators=(",", ":")) + "\n")
                n += 1
            token = data.get("nextPageToken")
            pages += 1
            if not token or (max_pages and pages >= max_pages):
                break
    return out, n


def materialize_raw(con: duckdb.DuckDBPyConnection, ndjson: Path | str) -> Path:
    """Bronze: write a compact Parquet ``(nct_id, record)`` from the NDJSON extract."""
    ndjson = Path(ndjson)
    raw = ndjson.with_name(ndjson.stem + ".parquet")
    con.execute(
        f"""
        COPY (
            SELECT {_NCT_J} AS nct_id, CAST(json AS VARCHAR) AS record
            FROM read_json_objects('{ndjson}')
            WHERE {_NCT_J} IS NOT NULL
        ) TO '{raw}' (FORMAT parquet);
        """
    )
    return raw


def _studies_sql(src: str, limit: int | None) -> str:
    """Typed projection over the bronze Parquet's ``record`` JSON (kept verbatim)."""
    limit_sql = f" LIMIT {int(limit)}" if limit else ""
    return f"""
        SELECT
            nct_id,
            json_extract_string(record, '$.protocolSection.identificationModule.briefTitle')    AS title,
            json_extract_string(record, '$.protocolSection.identificationModule.officialTitle')  AS official_title,
            json_extract_string(record, '$.protocolSection.identificationModule.organization.fullName') AS org_name,
            json_extract_string(record, '$.protocolSection.statusModule.overallStatus')          AS status,
            json_extract_string(record, '$.protocolSection.statusModule.startDateStruct.date')   AS start_date,
            json_extract_string(record, '$.protocolSection.statusModule.primaryCompletionDateStruct.date') AS primary_completion_date,
            json_extract_string(record, '$.protocolSection.statusModule.completionDateStruct.date')        AS completion_date,
            json_extract_string(record, '$.protocolSection.statusModule.lastUpdatePostDateStruct.date')    AS last_update_date,
            json_extract_string(record, '$.protocolSection.statusModule.resultsFirstPostDateStruct.date')  AS results_first_posted,
            json_extract_string(record, '$.protocolSection.sponsorCollaboratorsModule.leadSponsor.name')   AS lead_sponsor,
            json_extract_string(record, '$.protocolSection.sponsorCollaboratorsModule.leadSponsor.class')  AS sponsor_class,
            json_extract_string(record, '$.protocolSection.designModule.studyType')              AS study_type,
            json_extract_string(record, '$.protocolSection.designModule.phases')                 AS phases,
            TRY_CAST(json_extract_string(record, '$.protocolSection.designModule.enrollmentInfo.count') AS BIGINT) AS enrollment,
            json_extract_string(record, '$.protocolSection.conditionsModule.conditions')         AS conditions,
            json_extract_string(record, '$.protocolSection.armsInterventionsModule.interventions') AS interventions,
            TRY_CAST(json_extract(record, '$.hasResults') AS BOOLEAN)                            AS has_results,
            record
        FROM read_parquet({src}){limit_sql}
    """


def _references_sql(src: str, limit: int | None) -> str:
    inner = f"SELECT nct_id, record FROM read_parquet({src})"
    if limit:
        inner += f" LIMIT {int(limit)}"
    return f"""
        SELECT
            nct_id,
            TRY_CAST(json_extract_string(r.unnest, '$.pmid') AS BIGINT) AS pmid,
            json_extract_string(r.unnest, '$.type')         AS ref_type,
            json_extract_string(r.unnest, '$.citation')     AS citation
        FROM ({inner}),
             UNNEST(json_extract(record, '$.protocolSection.referencesModule.references[*]')) AS r
        WHERE json_extract_string(r.unnest, '$.pmid') IS NOT NULL
    """


def curate(
    con: duckdb.DuckDBPyConnection,
    raw_parquet: Path | str,
    *,
    schema: str = "ctgov",
    limit: int | None = None,
) -> dict[str, int]:
    """Upsert ``ctgov.studies`` (key nct_id) + ``ctgov.references`` (key nct_id,pmid)."""
    src = csv_source(str(raw_parquet))
    studies = upsert(con, f"{LAKE}.{schema}.studies", _studies_sql(src, limit), key="nct_id")
    refs = upsert(
        con, f"{LAKE}.{schema}.references", _references_sql(src, limit), key=["nct_id", "pmid"]
    )
    return {"studies": studies, "references": refs}


def ingest(
    *,
    file: str | None = None,
    schema: str = "ctgov",
    max_pages: int | None = None,
    limit: int | None = None,
    settings: Settings | None = None,
) -> dict:
    """End-to-end: download → materialize bronze Parquet → upsert both tables.

    ``file`` may be a local NDJSON extract or an already-materialized ``.parquet``
    (skips download/materialize).
    """
    s = settings or get_settings()
    con = lake_connect(s)
    try:
        if file and str(file).endswith(".parquet"):
            raw = Path(file)
        else:
            ndjson = Path(file) if file else download_studies(s, max_pages=max_pages)[0]
            raw = materialize_raw(con, ndjson)
        snap_before = con.execute(f"SELECT max(snapshot_id) FROM {LAKE}.snapshots()").fetchone()[0]
        counts = curate(con, raw, schema=schema, limit=limit)
        snap_after = con.execute(f"SELECT max(snapshot_id) FROM {LAKE}.snapshots()").fetchone()[0]
    finally:
        con.close()
    return {
        "studies_table": f"{LAKE}.{schema}.studies",
        "references_table": f"{LAKE}.{schema}.references",
        **counts,
        "changed": snap_after != snap_before,
    }
