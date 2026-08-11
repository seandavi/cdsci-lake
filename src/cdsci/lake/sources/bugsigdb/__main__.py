"""CLI: ``python -m cdsci.lake.sources.bugsigdb`` — load BugSigDB."""

from __future__ import annotations

import typer

from ...log import configure
from .ingest import ingest

app = typer.Typer(
    help="Ingest a BugSigDB release tag into lake.bugsigdb.signatures.",
    add_completion=False,
)


@app.callback()
def main(log_level: str = typer.Option("INFO", "--log-level", help="loguru level.")) -> None:
    """BugSigDB ingestor (keeps the ``run`` subcommand explicit)."""
    configure(log_level)


@app.command("run")
def run(
    file: str | None = typer.Option(
        None, "--file", help="Local CSV to load instead of downloading."
    ),
    version: str | None = typer.Option(
        None, "--version", help="BugSigDB release tag (default: Settings.bugsigdb_version)."
    ),
    schema: str = typer.Option("bugsigdb", "--schema", help="Target lake schema."),
    limit: int | None = typer.Option(None, "--limit", help="Cap rows (smoke test)."),
) -> None:
    """Download (unless --file) and MERGE-upsert one BugSigDB release tag."""
    summary = ingest(file=file, version=version, schema=schema, limit=limit)
    delta = "changed" if summary["changed"] else "no change (idempotent)"
    typer.echo(
        f"{summary['table']} <- {summary['rows']:,} rows "
        f"(release {summary['version']}) — {delta}"
    )


if __name__ == "__main__":
    app()
