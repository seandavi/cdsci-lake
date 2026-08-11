"""BugSigDB — manually curated microbial signatures.

One tidy table ``lake.bugsigdb.signatures`` from the tag-pinned Waldron Lab
export (CC BY 4.0), keyed ``bsdb_id``. ``metaphlan_taxon_names`` /
``ncbi_taxonomy_ids`` land as raw delimited strings; the transform layer
explodes them into ``bugsigdb.signature_taxon``
(``models/bugsigdb/signature_taxon.sql``). Ported from bioc-on-ice, which
retires its own copy in favor of this (bioc-on-ice#67 / cdsci-lake#31).
"""

from .ingest import curate, download_csv, ingest

__all__ = ["curate", "download_csv", "ingest"]
