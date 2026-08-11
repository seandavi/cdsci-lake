"""CLI: ``python -m cdsci.lake.sources.reliance`` — load Reliance on Science.

CC BY-NC 4.0 — internal non-commercial use only; do not redistribute.
"""

from __future__ import annotations

from .._cli import build_app
from .ingest import ingest

app = build_app(
    "reliance", ingest,
    help="Ingest Reliance on Science (patent↔paper links) into lake.reliance.* "
    "[CC BY-NC 4.0 — internal, non-commercial; do not redistribute].",
)

if __name__ == "__main__":
    app()
