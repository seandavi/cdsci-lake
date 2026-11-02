"""CLI: ``python -m cdsci.lake.sources.mane`` — load a MANE release's summary."""

from __future__ import annotations

from .._cli import build_app
from .ingest import ingest

app = build_app("mane", ingest, help="Ingest a MANE release summary into lake.mane.transcript.")

if __name__ == "__main__":
    app()
