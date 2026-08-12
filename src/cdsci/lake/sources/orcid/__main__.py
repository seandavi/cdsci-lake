"""CLI: ``python -m cdsci.lake.sources.orcid`` — land ORCID identity records."""

from __future__ import annotations

from .._cli import build_app
from .ingest import ingest

app = build_app(
    "orcid", ingest,
    help=(
        "Land ORCID identity records into lake.orcid.person (keyed on orcid_id). "
        "Demand-driven: pass --orcids or --orcids-sql; the annual bulk file is "
        "deliberately not used (see cdsci.lake.sources.orcid.ingest)."
    ),
)

if __name__ == "__main__":
    app()
