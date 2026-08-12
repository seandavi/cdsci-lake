"""NCBI ``gene2accession`` — Entrez gene ↔ RNA/protein/genomic accessions, every organism (#37).

Raw landing table ``lake.ncbi_gene2accession.gene2accession`` (284.8M rows
measured 2026-08-11 — the lake's largest landing); the derived
``ncbi_gene2accession.mapping`` is a SQL transform model under
``models/ncbi_gene2accession/`` (ADR-0015), not part of this ingest.

Download + load are ``ncbi_gene``'s dump-agnostic helpers; see
:mod:`cdsci.lake.sources.ncbi_gene2accession.ingest` for the column spec, the
key, and the scale/spill reasoning that is the real content of this source.

License: US government work (NCBI/NLM), public domain.
"""

from .ingest import COLUMNS, DUMP, KEY, ingest

__all__ = ["COLUMNS", "DUMP", "KEY", "ingest"]
