"""CLI: ``python -m cdsci.lake.sources.retractionwatch`` — load Retraction Watch."""

from __future__ import annotations

import typer

from ...log import configure
from .ingest import ingest

app = typer.Typer(
    help="Ingest the Retraction Watch CSV into lake.retractionwatch.retractions.",
    add_completion=False,
)


@app.callback()
def main(log_level: str = typer.Option("INFO", "--log-level", help="loguru level.")) -> None:
    """Retraction Watch ingestor (keeps the ``run`` subcommand explicit)."""
    configure(log_level)


@app.command("run")
def run(
    file: str | None = typer.Option(
        None, "--file", help="Local CSV to load instead of downloading."
    ),
    version: str | None = typer.Option(
        None, "--version", help="Snapshot label (default: today's pull date)."
    ),
    schema: str = typer.Option("retractionwatch", "--schema", help="Target lake schema."),
    limit: int | None = typer.Option(None, "--limit", help="Cap rows (smoke test)."),
) -> None:
    """Download (unless --file) and MERGE-upsert the Retraction Watch database."""
    summary = ingest(file=file, version=version, schema=schema, limit=limit)
    delta = "changed" if summary["changed"] else "no change (idempotent)"
    typer.echo(
        f"{summary['table']} <- {summary['rows']:,} rows "
        f"(snapshot {summary['version']}) — {delta}"
    )


if __name__ == "__main__":
    app()
