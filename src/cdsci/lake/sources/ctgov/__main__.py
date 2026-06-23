"""CLI: ``python -m cdsci.lake.sources.ctgov`` — load ClinicalTrials.gov into the lake."""

from __future__ import annotations

import typer

from .ingest import ingest

app = typer.Typer(
    help="Ingest ClinicalTrials.gov studies (full JSON) into lake.ctgov.{studies,references}.",
    add_completion=False,
)


@app.command("run")
def run(
    file: str | None = typer.Option(
        None, "--file", help="Local NDJSON extract or .parquet to load instead of downloading."
    ),
    schema: str = typer.Option(
        "ctgov", "--schema", help="Target lake schema (e.g. _dev to stage before promoting)."
    ),
    max_pages: int | None = typer.Option(
        None, "--max-pages", help="Cap API pages (~1000 studies each) for a partial run."
    ),
    limit: int | None = typer.Option(None, "--limit", help="Cap rows (smoke test)."),
) -> None:
    """Download → materialize bronze Parquet → upsert ctgov.studies + ctgov.references."""
    s = ingest(file=file, schema=schema, max_pages=max_pages, limit=limit)
    delta = "changed" if s["changed"] else "no change (idempotent)"
    typer.echo(
        f"{s['studies_table']} <- {s['studies']:,} studies; "
        f"{s['references_table']} <- {s['references']:,} refs — {delta}"
    )


if __name__ == "__main__":
    app()
