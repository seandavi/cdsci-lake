"""CLI: ``python -m cdsci.lake.sources.bugsigdb`` — load BugSigDB."""

from __future__ import annotations

from .._cli import build_app
from .ingest import ingest

app = build_app(
    "bugsigdb", ingest, help="Ingest a BugSigDB release tag into lake.bugsigdb.signatures."
)

if __name__ == "__main__":
    app()
