"""CLI: ``python -m cdsci.lake.sources.cellosaurus`` — load a Cellosaurus release."""

from __future__ import annotations

from .._cli import build_app
from .ingest import ingest

app = build_app(
    "cellosaurus", ingest,
    help="Ingest Cellosaurus' flat file into lake.cellosaurus.line.",
)

if __name__ == "__main__":
    app()
