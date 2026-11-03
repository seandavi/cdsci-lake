"""CLI: ``python -m cdsci.lake.sources.eqtl_catalogue`` — load an eQTL Catalogue release."""

from __future__ import annotations

from .._cli import build_app
from .ingest import ingest

app = build_app(
    "eqtl_catalogue", ingest,
    help="Ingest an eQTL Catalogue release into lake.eqtl_catalogue.{dataset,tabix_ftp_path}.",
)

if __name__ == "__main__":
    app()
