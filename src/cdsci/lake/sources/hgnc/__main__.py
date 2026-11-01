"""CLI: ``python -m cdsci.lake.sources.hgnc`` — load the HGNC complete set."""

from __future__ import annotations

from .._cli import build_app
from .ingest import ingest

app = build_app("hgnc", ingest, help="Ingest the HGNC complete set into lake.hgnc.gene.")

if __name__ == "__main__":
    app()
