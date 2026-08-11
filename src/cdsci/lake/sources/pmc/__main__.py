"""CLI: ``python -m cdsci.lake.sources.pmc`` — load BioC-PMC full text into the lake."""

from __future__ import annotations

import typer

from .._cli import build_app
from .ingest import ingest, list_ranges

app = build_app(
    "pmc", ingest, help="Ingest BioC-PMC full text into lake.pmc.documents + lake.pmc.passages."
)


@app.command("ranges")
def ranges() -> None:
    """List the BioC-PMC range tarballs available (json-unicode)."""
    fs = list_ranges()
    for f in fs:
        typer.echo(f"  {f}")
    typer.echo(f"  ({len(fs)} ranges)")


if __name__ == "__main__":
    app()
