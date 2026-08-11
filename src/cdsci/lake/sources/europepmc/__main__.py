"""CLI: ``python -m cdsci.lake.sources.europepmc`` — load Europe PMC annotations."""

from __future__ import annotations

import typer

from .._cli import build_app
from .ingest import ingest, list_databases

app = build_app(
    "europepmc", ingest,
    help="Ingest Europe PMC text-mined annotations into lake.europepmc.annotations.",
)


@app.command("databases")
def databases() -> None:
    """List the annotation databases available in the TextMinedTerms directory."""
    names = list_databases()
    for name in names:
        typer.echo(f"  {name}")
    typer.echo(f"{len(names)} databases")


if __name__ == "__main__":
    app()
