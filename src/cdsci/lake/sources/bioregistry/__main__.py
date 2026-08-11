"""CLI: ``python -m cdsci.lake.sources.bioregistry`` — load the bioregistry."""

from __future__ import annotations

from .._cli import build_app
from .ingest import ingest

app = build_app(
    "bioregistry", ingest, help="Ingest the Bioregistry consensus export into lake.ref.bioregistry."
)

if __name__ == "__main__":
    app()
