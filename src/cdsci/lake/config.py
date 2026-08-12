"""Configuration for the shared lake and its source ingestors.

Reads the **same** ``.env`` as the OpenAlex pipeline (``CU_OPENALEX_`` prefix) so
the storage landing pad and R2 credentials are shared across the whole platform,
and adds lake- and source-specific settings. This module has **no dependency on
``cu_openalex``**: the substrate is the base every source and project builds on
(ADR-0022/0023), so the dependency arrow points *into* here, never out.
"""

from __future__ import annotations

from functools import lru_cache
from typing import Literal

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
    # Where DuckDB spills when memory_limit is exceeded. Big explosions (the PMC
    # passages unnest is the worst) can spill 100+ GB, so point this at a large
    # volume — exhausting the catalog disk is what aborts a multi-hour load with an
    # IO error. None → ``<storage_root>/lake/duckdb_tmp`` (dev default).
    duckdb_temp_directory: str | None = None

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
    # Credential source for the postgres backend (ADR-0011 §6): "gsm" reads R2 +
    # Postgres password from Google Secret Manager (this repo's gcloud context);
    # "env" reads them from the env-backed fields below (omicidx's Prefect workers,
    # which have no gcloud). Two real backends = a real seam.
    cred_source: Literal["gsm", "env"] = "gsm"
    # Env-backed credentials (used only when cred_source="env"); GSM path ignores
    # these and reads the *_secret names below instead.
    r2_account_id: str | None = None
    lake_pg_password: str | None = None

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
    # US Census cartographic boundary shapefiles — canonical FIPS + geometry.
    # Read via DuckDB's spatial extension (ST_Read) into ref.geo_* ; geometry is
    # stored as WKB (consumers use ST_GeomFromWKB). CRS: NAD83 / EPSG:4269.
    census_geo_year: int = 2023
    census_geo_url: str = (
        "https://www2.census.gov/geo/tiger/GENZ{year}/shp/cb_{year}_us_{layer}_500k.zip"
    )
    # OpenAlex snapshot — the public S3 bucket, read anonymously over its https
    # endpoint (no credentials, no AWS account). The snapshot IS the durable raw
    # layer (immutable monthly release, partitioned by updated_date), so we read +
    # project + filter directly from it with DuckDB and never re-stage 639 GB
    # locally (see ADR-0005). Works are pruned by domain and the abstract is
    # reconstructed from the inverted index on read; referenced_works become a
    # separate edge table. ``openalex_domains`` are the OpenAlex domain numbers we
    # keep — 1 = Life Sciences, 4 = Health Sciences (3 = Physical, 2 = Social are
    # dropped). ``openalex_batch_files`` bounds memory: parts are processed in
    # batches of this many (each ~300 MB gz / ~230k works) so a temp table never
    # holds the whole corpus. ``openalex_max_files`` caps the part count for a
    # laptop subset (None = full run); same code path either way.
    openalex_s3_base: str = "https://openalex.s3.amazonaws.com"
    openalex_domains: list[str] = ["1", "4"]
    openalex_batch_files: int = Field(default=50, ge=1)
    openalex_max_files: int | None = None
    openalex_max_object_bytes: int = 67_108_864  # 64 MiB — hyperauthorship records
    # OBO ontologies — semantic-sql's per-ontology SQLite builds, published to the
    # public ``bbop-sqlite`` S3 bucket (anonymous https; no credentials). The bucket
    # listing IS the registry (``available_ontologies`` reads ListObjectsV2), so a
    # new OBO ontology is picked up with no code change. Each ``<stem>.db.gz`` is the
    # relational-graph SQLite encoding of one OWL ontology, scanned via DuckDB's
    # sqlite extension; snapshot_version = the object's S3 Last-Modified date.
    # Verified 2026-06-26 (332 ontologies available).
    semsql_base_url: str = "https://s3.amazonaws.com/bbop-sqlite"
    # Europe PMC text-mined annotations — a directory of per-database CSVs (one
    # file per annotated resource: uniprot, chebi, nct, …), all the same shape
    # (accession, PMCID, EXTID, SOURCE). Loaded into one tidy table keyed by the
    # database (= file stem). The directory index is scraped for the file list.
    europepmc_textmined_url: str = "https://europepmc.org/pub/databases/pmc/TextMinedTerms/"
    # Retraction Watch — the Crossref-hosted CSV (CC0), updated weekday-daily. A
    # single rolling ~65 MB file (~70k rows); keyed Record ID, multi-value fields
    # semicolon-separated. The rolling file has no upstream version, so we tag the
    # snapshot by pull date and keep the downloaded CSV as the bronze copy.
    retractionwatch_url: str = (
        "https://gitlab.com/crossref/retraction-watch-data/-/raw/main/retraction_watch.csv"
    )
    # NLM MeSH (Medical Subject Headings) — descriptor + qualifier XML, released
    # annually (ADR-0010). ``desc{year}.gz`` is the controlled vocabulary + tree
    # hierarchy; ``qual{year}.xml`` the ~80 subheadings. Supplementary Concept
    # Records (``supp``) are deferred. ``mesh_year`` selects the annual edition.
    mesh_xml_base: str = "https://nlmpubs.nlm.nih.gov/projects/mesh/MESH_FILES/xmlmesh/"
    mesh_year: int = 2026
    # Reliance on Science (Marx) — patent↔paper links, keyed by OpenAlex Work ID.
    # **CC BY-NC 4.0**: internal non-commercial use only, NOT for redistribution;
    # the license is carried forward in the lake_ops source registry. A pinned
    # Zenodo record (v63, 2024 ed.) for reproducibility; `_pcs_oa.csv` = patent→
    # paper citations, `_patent_paper_pairs.csv` = same-team matched pairs.
    reliance_zenodo_record: str = "11461587"
    reliance_files: list[str] = ["_pcs_oa.csv", "_patent_paper_pairs.csv"]
    # BugSigDB — manually curated microbial signatures (CC BY 4.0), the Waldron
    # Lab exports repo. Versioned by release *tag*, not retrieval date: the repo
    # re-renders from bugsigdb.org hourly, so only tagged releases (each with a
    # Zenodo DOI) are reproducible/citable. Ported from bioc-on-ice, which is
    # retiring its own copy of this source (bioc-on-ice#67 / cdsci-lake#31).
    bugsigdb_repo: str = "https://raw.githubusercontent.com/waldronlab/bugsigdbexports"
    bugsigdb_version: str = "v1.3.1"

    # Bioregistry — canonical identifier prefixes/patterns/synonyms (CC0). The
    # TSV consensus export, auto-regenerated by GitHub Actions on every merge
    # to main; no version tag to pin to, unlike bugsigdb's Zenodo-DOI releases.
    bioregistry_registry_url: str = (
        "https://raw.githubusercontent.com/biopragmatics/bioregistry/main/"
        "exports/registry/registry.tsv"
    )

    # UniProt ID mapping — the single un-split whole-of-UniProt ``idmapping_selected
    # .tab.gz`` dump (CC BY 4.0, confirmed at uniprot.org/help/license 2026-08-11),
    # tab-delimited, no header, 22 columns per UniProt's own README (UniProtKB-AC,
    # UniProtKB-ID, GeneID, ... see ``sources/uniprot/ingest.py``). NOT the
    # per-organism ``idmapping/by_organism/`` files — that directory only covers a
    # curated reference set (not UniProt's full breadth) and taxon-scoping the
    # *load* violates the lake's "land raw whole" convention; ``ncbi_taxon`` is
    # already a per-row column in the un-split file. "current_release" has no
    # version tag in the URL, so (like retractionwatch) we tag the snapshot by pull
    # date. ~6.6 GiB gzipped (confirmed via HEAD against the ftp.expasy.org /
    # ftp.ebi.ac.uk mirrors, 2026-08-11 — ftp.uniprot.org itself times out
    # intermittently for large GETs in this environment); ``ingest(batches=...)``
    # shards the load to bound peak memory (see ``sources/uniprot/ingest.py``).
    uniprot_idmapping_url: str = (
        "https://ftp.uniprot.org/pub/databases/uniprot/current_release/"
        "knowledgebase/idmapping/idmapping_selected.tab.gz"
    )

    # NCBI Gene — the bulk dump directory (US government work, public domain).
    # `gene_info.gz` alone is 1.56 GiB gzipped / ~71.5M rows (confirmed via HEAD,
    # 2026-08-11). No release number anywhere: the dumps are regenerated nightly,
    # so Last-Modified and any checksum change daily on identical content and the
    # snapshot is tagged by retrieval date (see ``sources/ncbi_gene/ingest.py``).
    ncbi_gene_base_url: str = "https://ftp.ncbi.nlm.nih.gov/gene/DATA/"
    # UCSC Genome Browser goldenPath — per-assembly MySQL table dumps
    # (``<base>/<build>/database/<table>.txt.gz``, tab-delimited, no header; the
    # column order comes from the sibling ``<table>.sql``). Used for ``kgXref`` +
    # ``knownToLocusLink`` (the UCSCKG↔Entrez mapping, issue #55). No license
    # needed for UCSC's table data (genome.ucsc.edu/license, 2026-08-11) — see
    # ``sources/ucsc_kg/ingest.py``. The dumps carry no version tag, so (like
    # retractionwatch) the snapshot is tagged by pull date.
    ucsc_goldenpath_base: str = "https://hgdownload.soe.ucsc.edu/goldenPath"

    # --- Reverse-ETL: publish to bioc-on-ice's Iceberg catalog via icegate ---
    # bioc-on-ice is a *publication* layer (ADR-0015 §"iceberg target"): the
    # REST catalog gateway already exists, live in production, so this is
    # config, not infra. `endpoint` is icegate's own deployed URL (a client
    # attaches through the gateway, never straight to R2 Data Catalog);
    # `catalog` is the warehouse name icegate's config registers it under.
    # Token is a write-scoped icegate API key, a distinct principal from
    # bioc-on-ice's own ingest key, read from GSM like every other lake secret.
    bioconice_icegate_endpoint: str = "https://icegate-bioconice.seandavi.workers.dev"
    bioconice_icegate_catalog: str = "bioconice"
    bioconice_icegate_token_secret: str = "bioconice-icegate-key-cdsci-publish"

    @property
    def reliance_base_url(self) -> str:
        """Zenodo file-download base for the pinned Reliance on Science record."""
        return f"https://zenodo.org/records/{self.reliance_zenodo_record}/files"

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
