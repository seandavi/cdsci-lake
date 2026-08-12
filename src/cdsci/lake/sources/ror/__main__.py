"""CLI: ``python -m cdsci.lake.sources.ror`` — land the ROR organization dump."""

from __future__ import annotations

from .._cli import build_app
from .ingest import ingest

app = build_app(
    "ror", ingest,
    help="Land the versioned ROR bulk dump into lake.ror.organization (keyed on ror_id).",
)

if __name__ == "__main__":
    app()
