"""CLI: ``python -m cdsci.lake.sources.obo`` — load OBO ontologies' OBO Graphs JSON."""

from __future__ import annotations

from .._cli import build_app
from .ingest import ingest

app = build_app(
    "obo", ingest, help="Ingest OBO ontologies' OBO Graphs JSON into lake.obo.element.",
)

if __name__ == "__main__":
    app()
