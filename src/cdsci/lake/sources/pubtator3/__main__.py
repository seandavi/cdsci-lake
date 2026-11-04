"""CLI: ``python -m cdsci.lake.sources.pubtator3`` — load the PubTator3 dumps."""

from __future__ import annotations

from .._cli import build_app
from .ingest import ingest

app = build_app(
    "pubtator3", ingest,
    help="Ingest PubTator3's five <kind>2pubtator3.gz into lake.pubtator3.<kind>_annotation.",
)

if __name__ == "__main__":
    app()
