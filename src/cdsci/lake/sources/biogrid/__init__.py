"""BioGRID — curated physical and genetic interactions, every organism (MIT).

One table ``lake.biogrid.interaction``: a release's ALL tab3 file landed whole,
every column VARCHAR, keyed ``biogrid_interaction_id``, stamped
``biogrid_version``. Ported from bioc-on-ice's ``raw.biogrid__interactions``
(bioc-on-ice#183).
"""

from .ingest import COLUMNS, curate, download_release, ingest, latest

__all__ = ["COLUMNS", "curate", "download_release", "ingest", "latest"]
