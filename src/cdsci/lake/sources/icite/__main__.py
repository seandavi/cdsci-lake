"""CLI: ``python -m cdsci.lake.sources.icite`` — load the iCite snapshot into the lake."""

from __future__ import annotations

import typer

from .._cli import build_app
from .ingest import ingest, resolve_latest

app = build_app(
    "icite", ingest, help="Ingest the monthly iCite bulk snapshot into lake.icite.metadata."
)


@app.command("latest")
def latest() -> None:
    """Show the most recent iCite snapshot available on figshare."""
    info = resolve_latest()
    typer.echo(f"latest snapshot: {info['version']}")
    for f in info["files"]:
        typer.echo(f"  {f.get('name')}  {round(f.get('size', 0) / 1e6, 1)} MB")


if __name__ == "__main__":
    app()
