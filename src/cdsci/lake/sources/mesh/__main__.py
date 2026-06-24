"""CLI: ``python -m cdsci.lake.sources.mesh`` — load NLM MeSH into ``mesh.*``."""

from __future__ import annotations

import typer

from ...log import configure
from .ingest import (
    ingest,
    ingest_headings,
    ingest_pharmacological_actions,
    ingest_supplemental,
)

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


@app.command("headings")
def headings(
    version: str | None = typer.Option(
        None, "--version", help="Snapshot label (default: YYYY-MM)."
    ),
    source_table: str = typer.Option(
        "lake.omicidx.pubmed_article", "--source-table", help="In-lake PubMed source."
    ),
    schema: str = typer.Option("mesh", "--schema", help="Target lake schema."),
    limit: int | None = typer.Option(None, "--limit", help="Cap source articles (smoke test)."),
) -> None:
    """Build mesh.article_heading (pmid↔descriptor↔qualifier) from omicidx PubMed MeSH."""
    s = ingest_headings(version=version, source_table=source_table, schema=schema, limit=limit)
    delta = "changed" if s["changed"] else "no change (idempotent)"
    typer.echo(f"{s['table']} <- {s['rows']:,} rows — {delta}")


@app.command("supplemental")
def supplemental(
    year: int | None = typer.Option(None, "--year", help="MeSH edition (default: config)."),
    file: str | None = typer.Option(
        None, "--file", help="Local supp XML/.gz/.parquet instead of downloading."
    ),
    schema: str = typer.Option("mesh", "--schema", help="Target lake schema."),
    limit: int | None = typer.Option(None, "--limit", help="Cap SCRs (smoke test)."),
) -> None:
    """Load Supplementary Concept Records (chemicals/substances) — supp{year}.gz is ~786 MB."""
    s = ingest_supplemental(year=year, file=file, schema=schema, limit=limit)
    delta = "changed" if s["changed"] else "no change (idempotent)"
    counts = {k: s[k] for k in ("supplemental", "supplemental_descriptor", "supplemental_term")}
    typer.echo(f"{s['schema']} <- {s['rows']:,} rows — {delta}: {counts}")


@app.command("actions")
def actions(
    year: int | None = typer.Option(None, "--year", help="MeSH edition (default: config)."),
    file: str | None = typer.Option(
        None, "--file", help="Local pa XML/.parquet instead of downloading."
    ),
    schema: str = typer.Option("mesh", "--schema", help="Target lake schema."),
) -> None:
    """Load the pharmacological-action cross-reference (substance ↔ mechanism/effect)."""
    s = ingest_pharmacological_actions(year=year, file=file, schema=schema)
    delta = "changed" if s["changed"] else "no change (idempotent)"
    typer.echo(f"{s['table']} <- {s['rows']:,} rows — {delta}")


if __name__ == "__main__":
    app()
