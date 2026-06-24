"""CLI: ``python -m cdsci.lake.sources.reporter`` — load ExPORTER groups into the lake."""

from __future__ import annotations

import typer

from ...log import configure
from .ingest import GROUPS, available_years, ingest

app = typer.Typer(
    help="Ingest NIH RePORTER ExPORTER groups (projects/abstracts/publications/publink).",
    add_completion=False,
)


@app.callback()
def _main(log_level: str = typer.Option("INFO", "--log-level", help="loguru level.")) -> None:
    configure(log_level)


@app.command("groups")
def groups() -> None:
    """List the importable ExPORTER groups."""
    for name, g in GROUPS.items():
        typer.echo(f"  {name:<13} {g.file_group:<12} {g.doc_type:<7} key={','.join(g.key)}")


@app.command("years")
def years(group: str = typer.Argument(..., help="Group name (see `groups`).")) -> None:
    """List the years available for a group."""
    typer.echo(", ".join(str(y) for y in available_years(group)))


@app.command("run")
def run(
    group: str = typer.Option("projects", "--group", "-g", help="Group to load (see `groups`)."),
    year: list[int] = typer.Option(
        None, "--year", "-y", help="Year(s) to load; omit to load ALL available years."
    ),
    file: list[str] = typer.Option(
        None, "--file", help="Local CSV(s) to load instead of downloading; repeatable."
    ),
    schema: str = typer.Option(
        "reporter", "--schema", help="Target lake schema (e.g. _dev to stage before promoting)."
    ),
    limit: int | None = typer.Option(None, "--limit", help="Cap rows (smoke test)."),
) -> None:
    """Download (unless --file) and MERGE-upsert an ExPORTER group into the lake."""
    summary = ingest(
        group=group, years=year or None, files=file or None, schema=schema, limit=limit
    )
    delta = f"snapshot {summary['snapshot']}" if summary["changed"] else "no change (idempotent)"
    typer.echo(f"{summary['table']} <- {summary['rows']:,} rows — {delta}")


if __name__ == "__main__":
    app()
