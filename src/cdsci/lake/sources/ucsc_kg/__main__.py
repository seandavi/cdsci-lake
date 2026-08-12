"""CLI: ``python -m cdsci.lake.sources.ucsc_kg`` — load UCSC Known Gene xrefs."""

from __future__ import annotations

from .._cli import build_app
from .ingest import ingest

app = build_app(
    "ucsc_kg", ingest,
    help="Ingest UCSC kgXref + knownToLocusLink into lake.ucsc.known_gene_xref.",
)

if __name__ == "__main__":
    app()
