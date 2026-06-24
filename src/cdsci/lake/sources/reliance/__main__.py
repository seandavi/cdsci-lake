"""CLI: ``python -m cdsci.lake.sources.reliance`` — load Reliance on Science.

CC BY-NC 4.0 — internal non-commercial use only; do not redistribute.
"""

from __future__ import annotations

import typer

from ...log import configure
from .ingest import DATASETS, ingest

app = typer.Typer(
    help="Ingest Reliance on Science (patent↔paper links) into lake.reliance.* "
    "[CC BY-NC 4.0 — internal, non-commercial; do not redistribute].",
    add_completion=False,
)


@app.callback()
def main(log_level: str = typer.Option("INFO", "--log-level", help="loguru level.")) -> None:
    """Reliance on Science ingestor (keeps the ``run`` subcommand explicit)."""
    configure(log_level)


@app.command("run")
def run(
    dataset: str | None = typer.Option(
        None, "--dataset", "-d", help=f"One of {list(DATASETS)}; omit for both."
    ),
    file: str | None = typer.Option(None, "--file", help="Local CSV for --dataset."),
    version: str | None = typer.Option(None, "--version", help="Snapshot label."),
    schema: str = typer.Option("reliance", "--schema", help="Target lake schema."),
    limit: int | None = typer.Option(None, "--limit", help="Cap rows per file (smoke test)."),
) -> None:
    """Download + MERGE-upsert Reliance on Science into lake.reliance.*."""
    summary = ingest(dataset=dataset, file=file, version=version, schema=schema, limit=limit)
    delta = "changed" if summary["changed"] else "no change (idempotent)"
    typer.echo(
        f"lake.{schema}.* <- {summary['rows']:,} rows "
        f"(snapshot {summary['version']}) — {delta}: {summary['counts']}"
    )


if __name__ == "__main__":
    app()
