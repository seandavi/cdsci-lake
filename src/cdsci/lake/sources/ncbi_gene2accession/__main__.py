"""CLI: ``python -m cdsci.lake.sources.ncbi_gene2accession`` — land NCBI gene2accession."""

from __future__ import annotations

from .._cli import build_app
from .ingest import ingest

app = build_app(
    "ncbi_gene2accession", ingest,
    help="Land NCBI's gene2accession dump into lake.ncbi_gene2accession.gene2accession.",
)

if __name__ == "__main__":
    app()
