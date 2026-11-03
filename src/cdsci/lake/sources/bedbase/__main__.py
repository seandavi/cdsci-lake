"""CLI: ``python -m cdsci.lake.sources.bedbase`` — load a BEDbase monthly snapshot."""

from __future__ import annotations

from .._cli import build_app
from .ingest import ingest

app = build_app(
    "bedbase", ingest,
    help="Ingest a BEDbase monthly Parquet snapshot into lake.bedbase.{bed,bedset,"
         "bedset_membership}.",
)

if __name__ == "__main__":
    app()
