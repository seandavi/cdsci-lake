"""CLI: ``python -m cdsci.lake.sources.complexportal`` — load a Complex Portal release."""

from __future__ import annotations

from .._cli import build_app
from .ingest import ingest

app = build_app(
    "complexportal", ingest,
    help="Ingest a Complex Portal complextab release into lake.complexportal.complex.",
)

if __name__ == "__main__":
    app()
