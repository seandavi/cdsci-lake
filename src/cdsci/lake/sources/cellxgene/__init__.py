"""CELLxGENE Discover — the public single-cell dataset listing (CC BY 4.0).

One table ``lake.cellxgene.dataset``: the PUBLIC listing landed whole, scalars
typed and nested fields as JSON text, keyed ``dataset_version_id``, stamped
``retrieval_date``. Ported from bioc-on-ice's ``raw.cellxgene__dataset``
(bioc-on-ice#183).
"""

from .ingest import COLUMNS, curate, download_listing, ingest

__all__ = ["COLUMNS", "curate", "download_listing", "ingest"]
