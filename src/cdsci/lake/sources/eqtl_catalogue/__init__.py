"""eQTL Catalogue — the dataset catalog and its summary-statistics FTP paths (CC BY 4.0).

Two tables, one Catalogue release each, every column VARCHAR, keyed
``dataset_id``, stamped ``eqtlcatalogue_release``: ``lake.eqtl_catalogue.dataset``
(``dataset_metadata_r<N>.tsv``) and ``lake.eqtl_catalogue.tabix_ftp_path``
(``tabix_ftp_paths.tsv``). Ported from bioc-on-ice's
``raw.eqtlcatalogue__{dataset,tabix_ftp_paths}`` (bioc-on-ice#183).
"""

from .ingest import DATASET_COLUMNS, PATH_COLUMNS, RELEASE, curate, download_release, ingest

__all__ = ["DATASET_COLUMNS", "PATH_COLUMNS", "RELEASE", "curate", "download_release", "ingest"]
