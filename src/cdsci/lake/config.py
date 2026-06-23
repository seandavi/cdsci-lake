"""Configuration for the shared lake and its source ingestors.

Reads the **same** ``.env`` as the OpenAlex pipeline (``CU_OPENALEX_`` prefix) so
the storage landing pad and R2 credentials are shared across the whole platform,
and adds lake- and source-specific settings. This module has **no dependency on
``cu_openalex``**: the substrate is the base every source and project builds on
(ADR-0022/0023), so the dependency arrow points *into* here, never out.
"""

from __future__ import annotations

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime configuration for the lake substrate and ingestors."""

    model_config = SettingsConfigDict(
        env_prefix="CU_OPENALEX_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # --- Shared storage seam (identical vars to the OpenAlex pipeline) ---
    storage_base_uri: str = "file://./data"
    r2_endpoint_url: str | None = None
    r2_access_key_id: str | None = None
    r2_secret_access_key: str | None = None
    r2_region: str = "auto"

    # --- Politeness ---
    mailto: str = "seandavi@gmail.com"

    # --- DuckDB resource limits (bound work; spill to disk) ---
    duckdb_memory_limit: str | None = None  # None → ~70% RAM (see lake._auto_memory_limit)
    duckdb_threads: int = Field(default=4, ge=1)

    # --- Lake backend selection ---
    # "local"    → single-file DuckDB catalog + Parquet under the storage seam
    #              (dev/test/prototype; no server to run).
    # "postgres" → the shared platform DuckLake: Postgres catalog + R2 data,
    #              credentials read from Google Secret Manager (ADR-0024).
    lake_backend: str = "local"

    # --- Local catalog (lake_backend="local") ---
    # Path relative to the local ``./data`` root (or absolute); data Parquet
    # lands under the storage seam at ``lake_data_prefix``.
    lake_catalog: str = "lake/catalog.ducklake"
    lake_data_prefix: str = "lake/data"

    # --- Shared catalog (lake_backend="postgres") ---
    # Catalog is the Postgres ``lake`` DB; data is the R2 bucket recorded in the
    # catalog (data path inherited on ATTACH, not re-specified). All secrets come
    # from Google Secret Manager — see :mod:`cri.secrets`.
    gsm_project: str = "cdsci-infra"
    lake_pg_host: str = "100.74.53.55"
    lake_pg_port: int = 5432
    lake_pg_dbname: str = "lake"
    lake_pg_user: str = "postgres"  # admin for now; switch to lake_writer/reader role
    lake_pg_password_secret: str = "cdsci-postgres-admin-password"
    r2_account_id_secret: str = "cdsci-r2-account-id"
    r2_access_key_secret: str = "cdsci-r2-access-key-id"
    r2_secret_key_secret: str = "cdsci-r2-secret-access-key"

    # --- Source locations (configurable; defaults verified 2026-06-22) ---
    # iCite monthly bulk: figshare collection "iCite Database Snapshots". The
    # operator may point this at a mirror, or bypass it with --file / --url.
    icite_figshare_collection: int = 4586573
    # NIH RePORTER ExPORTER: a file-listing API (POST) + a doc-service download
    # (GET ?DocType=&KeyId=). Endpoints reverse-engineered from the ExPORTER SPA
    # and verified 2026-06-22; centralized here so a change is a one-line edit.
    exporter_files_api: str = "https://reporter.nih.gov/services/exporter/allFilesInfo"
    exporter_download_url: str = (
        "https://reporter.nih.gov/services/exporter/DownloadFromDocService"
    )
    # ClinicalTrials.gov API v2 — paginated full JSON (the flat CSV is lossy:
    # it drops references/PMIDs, results, and structured modules).
    ctgov_api: str = "https://clinicaltrials.gov/api/v2/studies"
    ctgov_page_size: int = 1000
    # State Cancer Profiles — the maintainer's monthly GitHub *releases* (not a
    # live scrape). The latest release's tag is the snapshot_version and its
    # ``.csv.gz`` assets are the per-domain bulk dumps (kept verbatim = bronze).
    scp_repo: str = "seandavi/state-cancer-profile-scraper"
    # BioC-PMC full text — bulk per-PMCID-range tarballs (json-unicode = JSON
    # content, one BioC collection per article) + a per-article REST API for
    # incrementals. One-time bulk load, then API top-ups; MERGE on pmcid.
    biocpmc_ftp: str = "https://ftp.ncbi.nlm.nih.gov/pub/wilbur/BioC-PMC/"
    biocpmc_variant: str = "json_unicode"
    biocpmc_api: str = "https://www.ncbi.nlm.nih.gov/research/bionlp/RESTful/pmcoa.cgi/BioC_json"

    @property
    def scp_releases_api(self) -> str:
        """GitHub API URL for the latest State Cancer Profiles release."""
        return f"https://api.github.com/repos/{self.scp_repo}/releases/latest"

    @property
    def writes_to_r2(self) -> bool:
        """True when the landing pad is an S3/R2 URI (vs. local ``file://``)."""
        return self.storage_base_uri.startswith("s3://")


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return the process-wide cached :class:`Settings` instance."""
    return Settings()
