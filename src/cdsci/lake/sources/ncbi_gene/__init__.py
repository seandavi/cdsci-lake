"""NCBI Gene — ``gene_info`` + ``gene2ensembl`` bulk dumps (issue #36).

Raw landing tables ``lake.ncbi_gene.gene_info`` (~71.5M rows, every organism)
and ``lake.ncbi_gene.gene2ensembl``; the derived ``ncbi_gene.gene`` and
``ncbi_gene.mapping`` tables are SQL transform models under ``models/ncbi_gene/``
(ADR-0015), not part of this ingest.

Unversioned source: the dumps regenerate nightly, so the snapshot label is the
retrieval date. :func:`download_dump` and :func:`land` are the dump-agnostic
helpers the gene2accession/gene2pubmed/gene2go migrations (#37/#38/#39) should
import instead of restating.

License: US government work (NCBI/NLM), public domain.
"""

from .ingest import DUMP_COLUMNS, DUMP_KEYS, download_dump, ingest, land

__all__ = ["DUMP_COLUMNS", "DUMP_KEYS", "download_dump", "ingest", "land"]
