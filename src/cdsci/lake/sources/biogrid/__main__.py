"""CLI: ``python -m cdsci.lake.sources.biogrid`` — load a BioGRID release."""

from __future__ import annotations

from .._cli import build_app
from .ingest import ingest

app = build_app(
    "biogrid", ingest, help="Ingest a BioGRID ALL tab3 release into lake.biogrid.interaction.",
)

if __name__ == "__main__":
    app()
