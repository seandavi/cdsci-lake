"""CLI: ``python -m cdsci.lake.sources.census_geo`` — load Census FIPS boundaries → ref.*."""

from __future__ import annotations

import typer

from .._cli import build_app
from .ingest import LAYERS, ingest

app = build_app(
    "census_geo", ingest,
    help="Load US Census cartographic boundaries into ref.geo_state / ref.geo_county.",
)


@app.command("layers")
def layers() -> None:
    """List the boundary layers."""
    for name, spec in LAYERS.items():
        typer.echo(f"  {name:<12} cb_*_{spec.layer}_500k  key={','.join(spec.key)}")


if __name__ == "__main__":
    app()
