"""WikiPathways — community-curated pathways as gene sets (CC0).

One table ``lake.wikipathways.gene_set``: every species GMT of one monthly
release landed whole, one row per GMT line, keyed (``file_name``,
``line_number``), stamped ``wikipathways_version``. Ported from bioc-on-ice's
``raw.wikipathways__gmt`` (bioc-on-ice#183).
"""

from .ingest import FILE, curate, download_release, ingest, latest

__all__ = ["FILE", "curate", "download_release", "ingest", "latest"]
