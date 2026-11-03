"""CLI: ``python -m cdsci.lake.sources.encode`` — load the ENCODE experiment and file reports."""

from __future__ import annotations

from .._cli import build_app
from .ingest import ingest

app = build_app(
    "encode", ingest,
    help="Ingest the ENCODE portal's Experiment and File reports into lake.encode.",
)

if __name__ == "__main__":
    app()
