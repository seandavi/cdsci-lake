"""CLI: ``python -m cdsci.lake.sources.icite`` — load the iCite snapshot into the lake."""

from __future__ import annotations

import typer

from ...log import configure
from .ingest import ingest, resolve_latest

app = typer.Typer(
    help="Ingest the monthly iCite bulk snapshot into lake.icite.metadata.",
    add_completion=False,
)


@app.callback()
def _main(log_level: str = typer.Option("INFO", "--log-level", help="loguru level.")) -> None:
    configure(log_level)


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
    schema: str = typer.Option(
        "icite", "--schema", help="Target lake schema (e.g. _dev to stage before promoting)."
    ),
    limit: int | None = typer.Option(None, "--limit", help="Cap rows (smoke test)."),
) -> None:
    """Download (unless --file) and MERGE-upsert the snapshot into lake.icite.metadata."""
    summary = ingest(file=file, version=version, schema=schema, limit=limit)
    delta = "changed" if summary["changed"] else "no change (idempotent)"
    typer.echo(
        f"{summary['table']} <- {summary['rows']:,} rows "
        f"(snapshot {summary['version']}) — {delta}"
    )


if __name__ == "__main__":
    app()
