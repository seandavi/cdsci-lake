"""US Census cartographic boundaries — the canonical FIPS reference (with geometry).

The Census Bureau is authoritative for US FIPS codes and boundaries (NIST retired
FIPS 6-4 in 2008). Two ``ref`` tables, read straight from the Census cartographic
boundary shapefiles via DuckDB's ``spatial`` extension (no parser code):

* ``ref.geo_state``  — key ``fips`` (STATEFP); ``abbrev`` (STUSPS), ``name``, areas,
  ``geom_wkb``.
* ``ref.geo_county`` — key ``fips`` (5-digit GEOID); ``state_fips``, ``state_abbrev``,
  ``name``, areas, ``geom_wkb``.

These are the cross-source geographic anchor: ``scp.fips`` ⋈ ``ref.geo_state.fips``
and ``reporter.org_state`` ⋈ ``ref.geo_state.abbrev`` — a real key instead of an
inline state-name map — plus polygons for choropleths. Geometry is WKB (EPSG:4269);
reconstruct with ``ST_GeomFromWKB(geom_wkb)`` (consumers `LOAD spatial`).
"""

from .ingest import LAYERS, curate, download_layer, ingest

__all__ = ["LAYERS", "curate", "download_layer", "ingest"]
