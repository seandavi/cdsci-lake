"""CLI: ``python -m cdsci.lake.sources.ncbi_gene`` — land NCBI Gene bulk dumps."""

from __future__ import annotations

from .._cli import build_app
from .ingest import ingest

app = build_app(
    "ncbi_gene", ingest,
    help="Land NCBI Gene's gene_info/gene2ensembl dumps into lake.ncbi_gene.*.",
)

if __name__ == "__main__":
    app()
