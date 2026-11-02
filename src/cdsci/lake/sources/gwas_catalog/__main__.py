"""CLI: ``python -m cdsci.lake.sources.gwas_catalog`` — load a GWAS Catalog release."""

from __future__ import annotations

from .._cli import build_app
from .ingest import ingest

app = build_app(
    "gwas_catalog", ingest,
    help="Ingest a GWAS Catalog release into lake.gwas_catalog.{association,study}.",
)

if __name__ == "__main__":
    app()
