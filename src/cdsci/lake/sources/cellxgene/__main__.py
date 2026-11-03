"""CLI: ``python -m cdsci.lake.sources.cellxgene`` — load the CELLxGENE dataset listing."""

from __future__ import annotations

from .._cli import build_app
from .ingest import ingest

app = build_app(
    "cellxgene", ingest,
    help="Ingest the CELLxGENE Discover PUBLIC dataset listing into lake.cellxgene.dataset.",
)

if __name__ == "__main__":
    app()
