"""CLI: ``python -m cdsci.lake.maintenance_cli`` — DuckLake space reclamation.

Admin surface for the maintenance primitives (expire / cleanup / compact) plus
the surgical :func:`cdsci.lake.maintenance.purge_schema` used to roll back a
single schema (e.g. a broken PMC load) without disturbing any other publisher's
time-travel. Every command logs via loguru and defaults to ``--dry-run`` for the
destructive ones — pass ``--no-dry-run`` to actually mutate the shared lake.
"""

from __future__ import annotations

import typer

from . import maintenance
from .connect import lake_connect, snapshots
from .log import configure, logger

app = typer.Typer(
    help="DuckLake maintenance: expire snapshots, clean files, purge a schema.",
    add_completion=False,
)


@app.callback()
def _main(log_level: str = typer.Option("INFO", "--log-level", help="loguru level.")) -> None:
    configure(log_level)


@app.command("snapshots")
def snapshots_cmd(limit: int = typer.Option(20, "--limit", help="Show the most recent N.")) -> None:
    """List the lake's snapshots (newest last)."""
    con = lake_connect(read_only=True)
    try:
        rows = snapshots(con)
    finally:
        con.close()
    for r in rows[-limit:]:
        typer.echo(f"  {r}")
    typer.echo(f"  ({len(rows)} snapshots total)")


@app.command("schema-snapshots")
def schema_snapshots_cmd(schema: str = typer.Argument(..., help="Schema name, e.g. pmc.")) -> None:
    """List the snapshot ids whose every change belongs to SCHEMA (expire-safe set)."""
    con = lake_connect()
    try:
        ids = maintenance.schema_snapshot_ids(con, schema)
    finally:
        con.close()
    typer.echo(f"{schema}: {len(ids)} snapshot(s)" + (f" [{min(ids)}…{max(ids)}]" if ids else ""))
    typer.echo(f"  {ids}")


@app.command("purge-schema")
def purge_schema_cmd(
    schema: str = typer.Argument(..., help="Schema to retire fully, e.g. pmc."),
    dry_run: bool = typer.Option(True, "--dry-run/--no-dry-run", help="Preview vs. execute."),
) -> None:
    """Drop SCHEMA, expire only its snapshots, and clean its files (surgical rollback)."""
    con = lake_connect()
    try:
        result = maintenance.purge_schema(con, schema, dry_run=dry_run)
    finally:
        con.close()
    logger.info("purge-schema result: {}", result)
    typer.echo(result)


@app.command("cleanup")
def cleanup_cmd(
    dry_run: bool = typer.Option(True, "--dry-run/--no-dry-run", help="Preview vs. execute."),
) -> None:
    """Delete data files no longer referenced by any live snapshot."""
    con = lake_connect()
    try:
        rows = maintenance.cleanup_files(con, cleanup_all=True, dry_run=dry_run)
    finally:
        con.close()
    typer.echo(f"{'would delete' if dry_run else 'deleted'} {len(rows)} file(s)")


@app.command("vacuum")
def vacuum_cmd(
    older_than: str = typer.Option(
        None, "--older-than", help="GLOBAL expiry cutoff (affects all sources!)."
    ),
    dry_run: bool = typer.Option(True, "--dry-run/--no-dry-run", help="Preview vs. execute."),
) -> None:
    """Compact → (optionally GLOBAL-expire) → clean. Omit --older-than to skip expiry."""
    con = lake_connect()
    try:
        out = maintenance.vacuum(con, older_than=older_than, dry_run=dry_run)
    finally:
        con.close()
    typer.echo(out)


if __name__ == "__main__":
    app()
