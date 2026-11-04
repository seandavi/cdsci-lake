"""CLI: ``python -m cdsci.lake.sources.rnacentral`` — load an RNAcentral release."""

from __future__ import annotations

from .._cli import build_app
from .ingest import ingest

app = build_app(
    "rnacentral", ingest,
    help="Ingest an RNAcentral id_mapping release into lake.rnacentral.id_mapping.",
)

if __name__ == "__main__":
    app()
