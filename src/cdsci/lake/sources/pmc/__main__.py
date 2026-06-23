"""CLI: ``python -m cdsci.lake.sources.pmc`` — load BioC-PMC full text into the lake."""

from __future__ import annotations

import typer

from .ingest import ingest, list_ranges

app = typer.Typer(help="Ingest BioC-PMC full text into lake.pmc.fulltext.", add_completion=False)


@app.command("ranges")
def ranges() -> None:
    """List the BioC-PMC range tarballs available (json-unicode)."""
    fs = list_ranges()
    for f in fs:
        typer.echo(f"  {f}")
    typer.echo(f"  ({len(fs)} ranges)")


@app.command("run")
def run(
    range_: list[str] = typer.Option(
        None, "--range", "-r", help="Tarball filename(s) to load; omit for ALL ranges."
    ),
    file: str | None = typer.Option(
        None, "--file", help="Local NDJSON or .parquet to load instead of downloading."
    ),
    schema: str = typer.Option("pmc", "--schema", help="Target lake schema (e.g. _dev to stage)."),
    snapshot: str = typer.Option(
        "bulk", "--snapshot", help="snapshot_version label stamped on rows (e.g. 2026-06)."
    ),
    mode: str = typer.Option(
        "merge", "--mode", help="'append' for the initial bulk; 'merge' for incrementals."
    ),
    limit: int | None = typer.Option(None, "--limit", help="Cap rows (smoke test)."),
    keep_raw: bool = typer.Option(
        False, "--keep-raw", help="Keep the local tarball + NDJSON per range (default: delete)."
    ),
) -> None:
    """Download (unless --file) and MERGE-upsert BioC-PMC into pmc.fulltext, per range."""
    s = ingest(
        ranges=range_ or None, file=file, schema=schema,
        snapshot=snapshot, mode=mode, limit=limit, keep_raw=keep_raw,
    )
    typer.echo(f"{s['table']} <- {s['rows']:,} rows; ranges: {s['ranges']}")


if __name__ == "__main__":
    app()
