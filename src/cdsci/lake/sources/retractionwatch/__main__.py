"""CLI: ``python -m cdsci.lake.sources.retractionwatch`` — load Retraction Watch."""

from __future__ import annotations

from .._cli import build_app
from .ingest import ingest

app = build_app(
    "retractionwatch", ingest,
    help="Ingest the Retraction Watch CSV into lake.retractionwatch.retractions.",
)

if __name__ == "__main__":
    app()
