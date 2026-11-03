"""CELLxGENE Discover dataset listing -> ``lake.cellxgene.dataset`` (EL only; bioc-on-ice#183).

CELLxGENE Discover (CZI, CC BY 4.0) publishes every public single-cell dataset
in one JSON array, ``curation/v1/datasets?visibility=PUBLIC``: one record per
dataset version (2,228 on 2026-09-18). Expression matrices are never landed;
the records only point at them (``assets``). Ported from bioc-on-ice's
``src/bioconice/cellxgene.py`` (its ``raw.cellxgene__dataset`` landing);
bioc-on-ice derives ``resource.cellxgene__dataset`` from this table at release
time (bioc-on-ice ADR-0015).

**Versioned by retrieval date.** The listing is a live current-state view with
no archived copies and no version of its own, so the version is the UTC date it
was fetched, ``retrieval_date``. A past crawl can only be re-landed from a
local ``file``, whose date must then be given.

The read is the lander's, verbatim: keys by name, explicitly typed. Scalars land
typed (``cell_count`` BIGINT, ``mean_genes_per_cell`` DOUBLE, ``tombstone`` and
``is_pre_analysis`` BOOLEAN, the rest VARCHAR); the multi-valued and nested
fields (``organism``, ``assets``, ``spatial``, ...) land as their minified JSON
text, unexploded. A key a record omits (``batch_condition`` often is) and JSON
null both read as NULL. Keyed ``dataset_version_id``: raw's docs say one row per
dataset version.

**Each row is stamped with the Census build it joins through**, ``census_release``:
the Census release manifest's ``stable`` alias (its LTS build), resolved at
ingest, or given. It is compared like any column, so a new LTS restamps every
row on the next load.

**The shape is checked on the whole file before anything lands.** Reading by
name turns a renamed key into a silent all-NULL column, so every declared key
must occur in some record, every record must be an object with a
``dataset_version_id``, and an empty array is refused.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import httpx

from ... import ops
from ...config import Settings, get_settings
from ...connect import LAKE, csv_source, lake_connect, raw_dir, upsert
from ...download import download

_RAW = "cellxgene"

_STRUCT_LIST_FIELDS = (
    "organism", "assay", "tissue", "disease", "cell_type", "development_stage",
    "self_reported_ethnicity", "sex",
)
_OTHER_JSON_FIELDS = (
    "donor_id", "suspension_type", "is_primary_data", "assets", "spatial",
    "batch_condition", "perturbation_types", "genetic_perturbation_strategy",
)
# Top-level key -> read_json type, in raw's column order. JSON fields land as text.
COLUMNS = {
    "dataset_id": "VARCHAR", "dataset_version_id": "VARCHAR", "collection_id": "VARCHAR",
    "collection_version_id": "VARCHAR", "collection_name": "VARCHAR", "collection_doi": "VARCHAR",
    "collection_doi_label": "VARCHAR", "title": "VARCHAR", "citation": "VARCHAR",
    "schema_version": "VARCHAR", "cell_count": "BIGINT", "primary_cell_count": "BIGINT",
    "mean_genes_per_cell": "DOUBLE", "published_at": "VARCHAR", "revised_at": "VARCHAR",
    "explorer_url": "VARCHAR", "processing_status": "VARCHAR", "tombstone": "BOOLEAN",
    "visibility": "VARCHAR", "is_pre_analysis": "BOOLEAN",
    "revision_of_collection": "VARCHAR", "revision_of_dataset": "VARCHAR",
    "x_approximate_distribution": "VARCHAR",
    **{f: "JSON" for f in _STRUCT_LIST_FIELDS},
    **{f: "JSON" for f in _OTHER_JSON_FIELDS},
}


def download_listing(version: str, settings: Settings | None = None) -> Path:
    """Download the PUBLIC dataset listing into the raw layer, keyed by ``version``."""
    s = settings or get_settings()
    return download(s.cellxgene_datasets_url, raw_dir(_RAW, s) / f"{version}-datasets.json")


def stable_census_release(settings: Settings | None = None) -> str:
    """The Census release manifest's ``stable`` alias: the LTS build datasets join through."""
    s = settings or get_settings()
    resp = httpx.get(s.cellxgene_census_release_url, timeout=60, follow_redirects=True)
    resp.raise_for_status()
    return resp.json()["stable"]


def _check(path: Path) -> None:
    """Every record an object with a ``dataset_version_id``; every key present somewhere."""
    with open(path, encoding="utf-8") as f:
        records = json.load(f)
    if not isinstance(records, list):
        raise ValueError(f"cellxgene: {path} is not a JSON array")
    for n, r in enumerate(records):
        if not isinstance(r, dict) or not isinstance(r.get("dataset_version_id"), str):
            raise ValueError(f"cellxgene: {path} record {n} is not an object with a "
                             "dataset_version_id")
    if not records:
        raise ValueError(f"cellxgene: {path} has zero rows")
    missing = set(COLUMNS) - {k for r in records for k in r}
    if missing:
        raise ValueError(f"cellxgene: {path} expected keys missing upstream: {sorted(missing)}")


def _select_sql(path: Path, version: str, census_release: str, limit: int | None) -> str:
    colstr = "{" + ", ".join(f"'{k}':'{v}'" for k, v in COLUMNS.items()) + "}"
    select = ", ".join(f"{k}::VARCHAR AS {k}" if t == "JSON" else k for k, t in COLUMNS.items())
    limit_sql = f" LIMIT {int(limit)}" if limit else ""
    return f"""
        SELECT {select}, '{version}' AS retrieval_date, '{census_release}' AS census_release
        FROM read_json({csv_source([path])}, format='array', columns={colstr})
        {limit_sql}
    """


def curate(
    con: duckdb.DuckDBPyConnection,
    path: Path,
    version: str,
    census_release: str,
    *,
    target: str | None = None,
    limit: int | None = None,
) -> int:
    """MERGE-upsert one crawl into ``dataset`` on ``dataset_version_id``; return the row count.

    A full load prunes dataset versions gone from the listing (revised or
    tombstoned); ``limit`` never prunes. ``retrieval_date`` is compared like any
    column, so a new crawl restamps every row: the table is one crawl, never a mix.
    """
    # limit=0 is falsy: it would read every row and prune as a full load
    if limit is not None and limit < 1:
        raise ValueError(f"cellxgene: limit must be positive, got {limit}")
    _check(path)
    target = target or f"{LAKE}.cellxgene.dataset"
    return upsert(
        con, target, _select_sql(path, version, census_release, limit),
        key="dataset_version_id", prune=None if limit else "true",
    )


def ingest(
    *,
    file: str | None = None,
    version: str | None = None,
    census_release: str | None = None,
    schema: str = "cellxgene",
    limit: int | None = None,
    settings: Settings | None = None,
) -> dict:
    """End-to-end: download today's listing (unless ``file``) -> MERGE-upsert -> summary.

    ``version`` defaults to today's UTC date for a download; a local ``file``
    needs it given, since nothing in the listing dates it. ``census_release``
    defaults to the Census manifest's ``stable`` alias today.
    """
    s = settings or get_settings()
    if file:
        if not version:
            raise ValueError(f"cellxgene: {file} carries no date; pass version")
        path = Path(file)
    else:
        version = version or datetime.now(UTC).date().isoformat()
        path = download_listing(version, s)
    census_release = census_release or stable_census_release(s)
    target = f"{LAKE}.{schema}.dataset"
    con = lake_connect(s)
    try:
        with ops.run(con, source="cellxgene", target=target, version=version) as r:
            r.rows = curate(con, path, version, census_release, target=target, limit=limit)
    finally:
        con.close()
    return {**r.summary(), "census_release": census_release}
