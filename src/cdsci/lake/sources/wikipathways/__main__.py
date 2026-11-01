"""CLI: ``python -m cdsci.lake.sources.wikipathways`` — load a WikiPathways GMT release."""

from __future__ import annotations

from .._cli import build_app
from .ingest import ingest

app = build_app(
    "wikipathways", ingest,
    help="Ingest a WikiPathways GMT release into lake.wikipathways.gene_set.",
)

if __name__ == "__main__":
    app()
