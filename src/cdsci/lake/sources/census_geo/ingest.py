"""Ingest US Census cartographic boundary shapefiles into ``ref.geo_*``.

DuckDB's ``spatial`` extension reads the shapefile directly (``ST_Read``), so there
is no parser code — the attribute table already carries FIPS / abbrev / name and
the polygon. Geometry is stored as **WKB** (``ST_AsWKB``): portable in Parquet, and
(unlike native GEOMETRY) it compares in the MERGE so boundary revisions are caught.
``snapshot_version`` (the cb vintage) is excluded from change-detection.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import duckdb

from ...config import Settings, get_settings
from ...connect import LAKE, lake_connect, raw_dir, upsert
from ...download import download, unzip


@dataclass(frozen=True)
class Layer:
    """One Census boundary layer → one ``ref`` table."""

    table: str  # ref table name
    layer: str  # cb filename infix ("state" | "county")
    key: tuple[str, ...]
    projection: str  # SELECT body over ST_Read (must end with geom_wkb)


_STATE = Layer(
    table="geo_state",
    layer="state",
    key=("fips",),
    projection="""
        STATEFP                       AS fips,
        STUSPS                        AS abbrev,
        NAME                          AS name,
        TRY_CAST(ALAND AS BIGINT)     AS land_m2,
        TRY_CAST(AWATER AS BIGINT)    AS water_m2,
        ST_AsWKB(geom)                AS geom_wkb
    """,
)

_COUNTY = Layer(
    table="geo_county",
    layer="county",
    key=("fips",),
    projection="""
        GEOID                         AS fips,
        STATEFP                       AS state_fips,
        COUNTYFP                      AS county_fips,
        STUSPS                        AS state_abbrev,
        STATE_NAME                    AS state_name,
        NAME                          AS name,
        NAMELSAD                      AS name_long,
        TRY_CAST(ALAND AS BIGINT)     AS land_m2,
        TRY_CAST(AWATER AS BIGINT)    AS water_m2,
        ST_AsWKB(geom)                AS geom_wkb
    """,
)

LAYERS: dict[str, Layer] = {layer.table: layer for layer in (_STATE, _COUNTY)}


def download_layer(layer: str, year: int, settings: Settings | None = None) -> Path:
    """Download + unzip a cartographic boundary layer; return its ``.shp`` path."""
    s = settings or get_settings()
    spec = LAYERS[layer]
    url = s.census_geo_url.format(year=year, layer=spec.layer)
    root = raw_dir("census_geo", s)
    archive = download(url, root / f"cb_{year}_us_{spec.layer}_500k.zip")
    extracted = unzip(archive, root / f"{spec.layer}_{year}")
    return next(p for p in extracted if p.suffix.lower() == ".shp")


def _select_sql(layer: str, shp: Path | str, year: int) -> str:
    spec = LAYERS[layer]
    return (
        f"SELECT {spec.projection}, CAST('{year}' AS VARCHAR) AS snapshot_version "
        f"FROM ST_Read('{shp}')"
    )


def curate(
    con: duckdb.DuckDBPyConnection,
    layer: str,
    shp: Path | str,
    *,
    schema: str = "ref",
    year: int = 2023,
    target: str | None = None,
) -> int:
    """Upsert a boundary layer into ``ref.<table>`` on ``fips`` (geometry as WKB)."""
    con.execute("INSTALL spatial; LOAD spatial;")
    spec = LAYERS[layer]
    target = target or f"{LAKE}.{schema}.{spec.table}"
    return upsert(
        con, target, _select_sql(layer, shp, year),
        key=list(spec.key), exclude_change_cols=["snapshot_version"],
    )


def ingest(
    *,
    layers: list[str] | None = None,
    schema: str = "ref",
    year: int | None = None,
    settings: Settings | None = None,
) -> dict:
    """Download + curate the requested boundary layers (default: all)."""
    s = settings or get_settings()
    year = year or s.census_geo_year
    con = lake_connect(s)
    counts: dict[str, int] = {}
    try:
        for layer in layers or list(LAYERS):
            shp = download_layer(layer, year, s)
            counts[layer] = curate(con, layer, shp, schema=schema, year=year)
    finally:
        con.close()
    return {"schema": schema, "year": year, "counts": counts}
