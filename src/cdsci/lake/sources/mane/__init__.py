"""MANE — matched RefSeq/Ensembl human transcripts (NCBI and EMBL-EBI).

One table ``lake.mane.transcript``: the release's summary file landed whole,
every column VARCHAR, keyed ``(refseq_nuc, ensembl_nuc)``, stamped
``mane_version``. Ported from bioc-on-ice's ``raw.ncbi__mane_summary``
(bioc-on-ice#183).
"""

from .ingest import COLUMNS, curate, download_summary, ingest, latest

__all__ = ["COLUMNS", "curate", "download_summary", "ingest", "latest"]
