"""CLI: ``python -m cdsci.lake.sources.uniprot`` — load UniProt ID mapping."""

from __future__ import annotations

from .._cli import build_app
from .ingest import ingest

app = build_app(
    "uniprot", ingest,
    help="Ingest UniProt's organism-scoped ID-mapping dumps into lake.uniprot.idmapping.",
)

if __name__ == "__main__":
    app()
