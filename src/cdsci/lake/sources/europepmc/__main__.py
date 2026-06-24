"""CLI: ``python -m cdsci.lake.sources.europepmc`` — load Europe PMC annotations."""

from __future__ import annotations

import typer

from .ingest import ingest, list_databases

app = typer.Typer(
    help="Ingest Europe PMC text-mined annotations into lake.europepmc.annotations.",
    add_completion=False,
)


@app.command("databases")
def databases() -> None:
    """List the annotation databases available in the TextMinedTerms directory."""
    names = list_databases()
    for name in names:
        typer.echo(f"  {name}")
    typer.echo(f"{len(names)} databases")


@app.command("run")
def run(
    database: str | None = typer.Option(
        None, "--database", "-d", help="Single database (file stem); omit for all."
    ),
    file: str | None = typer.Option(None, "--file", help="Local CSV to load instead."),
    version: str | None = typer.Option(
        None, "--version", help="Snapshot label (default: today's date)."
    ),
    schema: str = typer.Option("europepmc", "--schema", help="Target lake schema."),
    limit: int | None = typer.Option(None, "--limit", help="Cap rows per file (smoke test)."),
) -> None:
    """Download (unless --file) and MERGE-upsert annotations into one tidy table."""
    summary = ingest(database=database, file=file, version=version, schema=schema, limit=limit)
    delta = "changed" if summary["changed"] else "no change (idempotent)"
    typer.echo(
        f"{summary['table']} <- {summary['rows']:,} rows across "
        f"{summary['databases']} database(s) (snapshot {summary['version']}) — {delta}"
    )


if __name__ == "__main__":
    app()
