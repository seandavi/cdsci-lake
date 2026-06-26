"""CLI: ``python -m cdsci.lake.sources.scp`` — load State Cancer Profiles into the lake."""

from __future__ import annotations

import typer

from ...log import configure
from .ingest import DOMAINS, ingest, resolve_latest

app = typer.Typer(
    help="Ingest State Cancer Profiles (GitHub releases) into lake.scp.{incidence,mortality,risk,demographics}.",
    add_completion=False,
)


@app.callback()
def _main(log_level: str = typer.Option("INFO", "--log-level", help="loguru level.")) -> None:
    configure(log_level)


@app.command("latest")
def latest() -> None:
    """Show the latest release tag and its domain assets (name + size)."""
    rel = resolve_latest()
    typer.echo(f"latest release: {rel['tag']}")
    wanted = {d.asset for d in DOMAINS.values()}
    for asset in rel["assets"]:
        name = asset.get("name", "")
        mark = "*" if name in wanted else " "
        size_mb = (asset.get("size") or 0) / 1e6
        typer.echo(f"  {mark} {name:<45} {size_mb:>8.1f} MB")


@app.command("run")
def run(
    schema: str = typer.Option(
        "scp", "--schema", help="Target lake schema (e.g. _dev to stage before promoting)."
    ),
    file: list[str] = typer.Option(
        None,
        "--file",
        help="Local CSV(s) as domain=path (e.g. risk=/tmp/risk.csv.gz); repeatable, skips download.",
    ),
    tag: str | None = typer.Option(None, "--tag", help="Override the snapshot_version tag."),
    limit: int | None = typer.Option(None, "--limit", help="Cap rows per domain (smoke test)."),
) -> None:
    """Download the latest release (unless --file) and MERGE-upsert all four domains."""
    files: dict[str, str] | None = None
    if file:
        files = {}
        for spec in file:
            if "=" not in spec:
                raise typer.BadParameter(f"--file expects domain=path, got {spec!r}")
            domain, path = spec.split("=", 1)
            files[domain] = path

    summary = ingest(files=files, tag=tag, schema=schema, limit=limit)
    delta = f"snapshot {summary['snapshot']}" if summary["changed"] else "no change (idempotent)"
    typer.echo(f"scp @ {summary['tag']} -> schema {summary['schema']} — {delta}")
    for domain, rows in summary["counts"].items():
        typer.echo(f"  {domain:<13} {rows:,} rows")


if __name__ == "__main__":
    app()
