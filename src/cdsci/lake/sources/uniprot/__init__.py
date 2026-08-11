"""UniProt — accession ↔ EntrezGene ID mapping (organism-scoped bulk dumps).

One tidy table ``lake.uniprot.idmapping`` keyed ``(accession, gene_id)`` — GeneID
is many-to-many with UniProtKB-AC, so the pair is the natural key. Feeds
bioc-on-ice's ``annotation.identifier_mapping`` (``ENTREZID``<->``UNIPROT``) via
the same reverse-ETL path BugSigDB uses, once that publish path is unblocked
(cdsci-lake#32, cdsci-lake#53).
"""

from .ingest import curate, download_idmapping, ingest

__all__ = ["curate", "download_idmapping", "ingest"]
