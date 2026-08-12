"""CLI: ``python -m cdsci.lake.sources.reactome`` — land Reactome's pathway TSVs."""

from __future__ import annotations

from .._cli import build_app
from .ingest import ingest

app = build_app(
    "reactome", ingest,
    help="Land Reactome's gene→pathway / pathway / hierarchy TSVs into lake.reactome.*.",
)

if __name__ == "__main__":
    app()
