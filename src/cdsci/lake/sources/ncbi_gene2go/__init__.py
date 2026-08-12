"""NCBI ``gene2go`` — GO annotations per Entrez gene, all organisms (issue #39).

Raw landing table ``lake.ncbi_gene2go.gene2go`` (~48M rows); the derived
``ncbi_gene2go.gene_go`` table is a SQL transform model under
``models/ncbi_gene2go/`` (ADR-0015), not part of this ingest.

Registered separately from ``ncbi_gene`` (own ledger row) even though it shares
that source's download/land helpers -- the two ingests run independently.

License: US government work (NCBI/NLM), public domain.
"""

from .ingest import COLUMNS, KEY, ingest

__all__ = ["COLUMNS", "KEY", "ingest"]
