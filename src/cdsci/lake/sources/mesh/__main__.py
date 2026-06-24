"""CLI: ``python -m cdsci.lake.sources.mesh`` — load NLM MeSH into ``mesh.*``."""

from __future__ import annotations

import typer

from ...log import configure
from .ingest import ingest

app = typer.Typer(
    help="Ingest NLM MeSH descriptors + tree hierarchy + qualifiers into lake.mesh.*.",
    add_completion=False,
)


@app.callback()
def _main(log_level: str = typer.Option("INFO", "--log-level", help="loguru level.")) -> None:
    configure(log_level)


@app.command("run")
def run(
    year: int | None = typer.Option(
        None, "--year", help="MeSH edition year (default: config mesh_year)."
    ),
    descriptor_file: str | None = typer.Option(
        None, "--descriptor-file", help="Local desc XML/.gz/.parquet instead of downloading."
    ),
    qualifier_file: str | None = typer.Option(
        None, "--qualifier-file", help="Local qual XML/.parquet instead of downloading."
    ),
    schema: str = typer.Option("mesh", "--schema", help="Target lake schema."),
    limit: int | None = typer.Option(None, "--limit", help="Cap descriptors (smoke test)."),
) -> None:
    """Download (unless --*-file) and MERGE-upsert the five MeSH tables."""
    s = ingest(
        year=year, descriptor_file=descriptor_file, qualifier_file=qualifier_file,
        schema=schema, limit=limit,
    )
    delta = "changed" if s["changed"] else "no change (idempotent)"
    counts = {k: s[k] for k in
              ("descriptor", "tree", "qualifier", "descriptor_qualifier", "entry_term")}
    typer.echo(f"{s['schema']} <- {s['rows']:,} rows — {delta}: {counts}")


if __name__ == "__main__":
    app()
