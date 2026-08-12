"""Ensembl — per-species GTF gene annotation (issue #35).

One raw table ``lake.ensembl.gtf``: the GTF verbatim, partitioned by
``(ncbitaxon_id, ensembl_release)`` and idempotent under re-ingest (an Ensembl
release is immutable). The derived ``ensembl.genome`` / ``gene`` / ``transcript``
/ ``exon`` tables are SQL transform models under ``models/ensembl/``, not code
here (ADR-0015).
"""

from .ingest import curate, download_gtf, gtf_url, ingest, species_info

__all__ = ["curate", "download_gtf", "gtf_url", "ingest", "species_info"]
