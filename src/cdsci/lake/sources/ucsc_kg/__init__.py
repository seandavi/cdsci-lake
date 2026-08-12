"""UCSC Known Gene cross-references — the ``UCSCKG`` identifier (issue #55).

One table ``lake.ucsc.known_gene_xref`` per genome build, from UCSC's ``kgXref``
+ ``knownToLocusLink`` MySQL dumps, keyed ``(genome_build, kg_id)``. ``gene_id``
is the direct Entrez mapping from ``knownToLocusLink``. See ``ingest.py``.
"""

from .ingest import curate, download_table, ingest

__all__ = ["curate", "download_table", "ingest"]
