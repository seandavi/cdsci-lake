"""CLI: ``python -m cdsci.lake.sources.ncbi_gene2go`` — land NCBI's gene2go dump."""

from __future__ import annotations

from .._cli import build_app
from .ingest import ingest

app = build_app(
    "ncbi_gene2go", ingest,
    help="Land NCBI's gene2go dump (all organisms) into lake.ncbi_gene2go.gene2go.",
)

if __name__ == "__main__":
    app()
