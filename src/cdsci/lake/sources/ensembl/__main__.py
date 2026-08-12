"""CLI: ``python -m cdsci.lake.sources.ensembl`` — land an Ensembl GTF."""

from __future__ import annotations

from .._cli import build_app
from .ingest import ingest

app = build_app(
    "ensembl", ingest,
    help="Land an Ensembl per-species GTF into lake.ensembl.gtf (one release/species per run).",
)

if __name__ == "__main__":
    app()
