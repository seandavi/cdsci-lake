"""NCBI ``gene2pubmed`` — the gene↔PMID edge, every organism (issue #38).

Raw landing table ``lake.ncbi_gene2pubmed.gene2pubmed`` (~40M rows); the derived
``ncbi_gene2pubmed.gene_publication`` is a SQL transform model under
``models/ncbi_gene2pubmed/`` (ADR-0015), not part of this ingest.

Download + load are ``ncbi_gene``'s dump-agnostic helpers; see
:mod:`cdsci.lake.sources.ncbi_gene2pubmed.ingest` for the column spec, the key,
and why this is its own edge table rather than a per-PMID alias row.

License: US government work (NCBI/NLM), public domain.
"""

from .ingest import COLUMNS, DUMP, KEY, ingest

__all__ = ["COLUMNS", "DUMP", "KEY", "ingest"]
