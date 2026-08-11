"""CLI: ``python -m cdsci.lake.sources.ctgov`` — load ClinicalTrials.gov into the lake."""

from __future__ import annotations

import typer

from ...config import get_settings
from ...download import get_json
from .._cli import build_app
from .ingest import ingest

app = build_app(
    "ctgov", ingest,
    help="Ingest ClinicalTrials.gov studies (full JSON) into lake.ctgov.{studies,references}.",
)


@app.command("count")
def count() -> None:
    """Show how many studies the API currently has."""
    s = get_settings()
    data = get_json(s.ctgov_api, params={"pageSize": 1, "countTotal": "true", "format": "json"})
    typer.echo(f"ClinicalTrials.gov studies available: {data.get('totalCount'):,}")


if __name__ == "__main__":
    app()
