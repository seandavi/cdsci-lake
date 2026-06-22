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

    # --- Lake catalog (single-file DuckDB catalog, ALWAYS local; ADR-0022) ---
    # Path is relative to the local ``./data`` root (or absolute). Start with a
    # single-file catalog to preserve "no server to run"; swap to Postgres only
    # when concurrent multi-project writers require it.
    lake_catalog: str = "lake/catalog.ducklake"
    # Lake data (Parquet) lives under the storage seam at this prefix.
    lake_data_prefix: str = "lake/data"

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

    @property
    def writes_to_r2(self) -> bool:
        """True when the landing pad is an S3/R2 URI (vs. local ``file://``)."""
        return self.storage_base_uri.startswith("s3://")


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return the process-wide cached :class:`Settings` instance."""
    return Settings()
