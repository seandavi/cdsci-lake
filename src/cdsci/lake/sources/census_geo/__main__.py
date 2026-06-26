"""CLI: ``python -m cdsci.lake.sources.census_geo`` — load Census FIPS boundaries → ref.*."""

from __future__ import annotations

import typer

from ...log import configure
from .ingest import LAYERS, ingest

app = typer.Typer(
    help="Load US Census cartographic boundaries into ref.geo_state / ref.geo_county.",
    add_completion=False,
)


@app.callback()
def _main(log_level: str = typer.Option("INFO", "--log-level", help="loguru level.")) -> None:
    configure(log_level)


@app.command("layers")
def layers() -> None:
    """List the boundary layers."""
    for name, spec in LAYERS.items():
        typer.echo(f"  {name:<12} cb_*_{spec.layer}_500k  key={','.join(spec.key)}")


@app.command("run")
def run(
    layer: list[str] = typer.Option(
        None, "--layer", "-l", help="Layer(s) to load; omit for all (state + county)."
    ),
    schema: str = typer.Option("ref", "--schema", help="Target lake schema."),
    year: int | None = typer.Option(None, "--year", help="Cartographic boundary vintage."),
) -> None:
    """Download + MERGE-upsert Census boundaries into the ref schema."""
    summary = ingest(layers=layer or None, schema=schema, year=year)
    typer.echo(f"ref geo loaded (cb {summary['year']}): {summary['counts']}")


if __name__ == "__main__":
    app()
