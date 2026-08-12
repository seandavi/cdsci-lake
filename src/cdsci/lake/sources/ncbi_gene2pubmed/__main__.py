"""CLI: ``python -m cdsci.lake.sources.ncbi_gene2pubmed`` — land NCBI gene2pubmed."""

from __future__ import annotations

from .._cli import build_app
from .ingest import ingest

app = build_app(
    "ncbi_gene2pubmed", ingest,
    help="Land NCBI's gene2pubmed dump into lake.ncbi_gene2pubmed.gene2pubmed.",
)

if __name__ == "__main__":
    app()
