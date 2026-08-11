"""CLI: ``python -m cdsci.lake.sources.bioregistry`` — load the bioregistry."""

from __future__ import annotations

import typer

from ...log import configure
from .ingest import ingest

app = typer.Typer(
    help="Ingest the Bioregistry consensus export into lake.ref.bioregistry.",
    add_completion=False,
)


@app.callback()
def main(log_level: str = typer.Option("INFO", "--log-level", help="loguru level.")) -> None:
    """Bioregistry ingestor (keeps the ``run`` subcommand explicit)."""
    configure(log_level)


@app.command("run")
def run(
    file: str | None = typer.Option(
        None, "--file", help="Local TSV to load instead of downloading."
    ),
    schema: str = typer.Option("ref", "--schema", help="Target lake schema."),
    limit: int | None = typer.Option(None, "--limit", help="Cap rows (smoke test)."),
) -> None:
    """Download (unless --file) and MERGE-upsert the current registry export."""
    summary = ingest(file=file, schema=schema, limit=limit)
    delta = "changed" if summary["changed"] else "no change (idempotent)"
    typer.echo(f"{summary['table']} <- {summary['rows']:,} rows — {delta}")


if __name__ == "__main__":
    app()
