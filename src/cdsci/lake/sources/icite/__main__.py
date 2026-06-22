"""CLI: ``python -m cri.sources.icite`` — load the iCite snapshot into the lake."""

from __future__ import annotations

import typer

from .ingest import ingest, resolve_latest

app = typer.Typer(
    help="Ingest the monthly iCite bulk snapshot into lake.icite.", add_completion=False
)


@app.command("latest")
def latest() -> None:
    """Show the most recent iCite snapshot available on figshare."""
    info = resolve_latest()
    typer.echo(f"latest snapshot: {info['version']}")
    for f in info["files"]:
        typer.echo(f"  {f.get('name')}  {round(f.get('size', 0) / 1e6, 1)} MB")


@app.command("run")
def run(
    file: str | None = typer.Option(
        None, "--file", help="Local CSV/TSV (glob ok) to load instead of downloading."
    ),
    version: str | None = typer.Option(None, "--version", help="Label this snapshot version."),
    limit: int | None = typer.Option(None, "--limit", help="Cap rows (smoke test)."),
) -> None:
    """Download (unless --file) and curate the snapshot into lake.icite."""
    summary = ingest(file=file, version=version, limit=limit)
    typer.echo(f"{summary['table']} <- {summary['rows']:,} rows (snapshot {summary['version']})")


if __name__ == "__main__":
    app()
