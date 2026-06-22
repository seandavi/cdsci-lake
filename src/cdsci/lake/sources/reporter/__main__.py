"""CLI: ``python -m cri.sources.reporter`` — load ExPORTER projects into the lake."""

from __future__ import annotations

import typer

from .ingest import ingest, list_files

app = typer.Typer(
    help="Ingest NIH RePORTER ExPORTER project files into lake.reporter_projects.",
    add_completion=False,
)


@app.command("files")
def files() -> None:
    """List the ExPORTER project files available (by fiscal year)."""
    for f in sorted(list_files(), key=lambda r: r.get("fy", 0), reverse=True):
        typer.echo(f"  FY{f.get('fy')}  {f.get('file_name')}  {f.get('file_size')}")


@app.command("run")
def run(
    years: list[int] = typer.Option(
        None, "--year", "-y", help="Fiscal year(s) to load; repeat the flag for several."
    ),
    file: list[str] = typer.Option(
        None, "--file", help="Local CSV(s) to load instead of downloading; repeatable."
    ),
    limit: int | None = typer.Option(None, "--limit", help="Cap rows (smoke test)."),
) -> None:
    """Download (unless --file) and curate ExPORTER projects into the lake."""
    summary = ingest(years=years or None, files=file or None, limit=limit)
    fys = ", ".join(str(y) for y in summary["fiscal_years"])
    typer.echo(f"{summary['table']} <- {summary['rows']:,} rows (FY {fys})")


if __name__ == "__main__":
    app()
