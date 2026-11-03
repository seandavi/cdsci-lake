"""CLI: ``python -m cdsci.lake.sources.intact`` — load an IntAct release."""

from __future__ import annotations

from .._cli import build_app
from .ingest import ingest

app = build_app(
    "intact", ingest, help="Ingest an IntAct MITAB release into lake.intact.interaction.",
)

if __name__ == "__main__":
    app()
