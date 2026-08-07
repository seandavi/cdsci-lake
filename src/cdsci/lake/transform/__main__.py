"""CLI: ``python -m cdsci.lake.transform`` — run SQL-file transform models (ADR-0015).

Mirrors ``maintenance_cli.py``'s shape: a typer app, ``--log-level`` via loguru,
``lake_connect()`` per command. ``--models-dir`` (default ``models``, the repo's
top-level SQL-file directory) is the one piece of shared state, threaded through
``typer.Context``.
"""

from __future__ import annotations

import typer

from ..connect import lake_connect
from ..log import configure
from . import runner
from .graph import build_graph, topological_order
from .models import load_models

app = typer.Typer(
    help="SQL-file transform + reverse-ETL models: run, inspect the dependency graph.",
    add_completion=False,
)


@app.callback()
def _main(
    ctx: typer.Context,
    log_level: str = typer.Option("INFO", "--log-level", help="loguru level."),
    models_dir: str = typer.Option("models", "--models-dir", help="Directory of *.sql models."),
) -> None:
    configure(log_level)
    ctx.obj = models_dir


@app.command("list")
def list_cmd(ctx: typer.Context) -> None:
    """List discovered models and their source files."""
    models = load_models(ctx.obj)
    for target in sorted(models):
        typer.echo(f"  {target}  ({models[target].path})")
    typer.echo(f"  ({len(models)} model(s))")


@app.command("graph")
def graph_cmd(ctx: typer.Context) -> None:
    """Print each model's dependencies and the topological execution order."""
    models = load_models(ctx.obj)
    g = build_graph(models)
    for target in sorted(g):
        deps = ", ".join(sorted(g[target])) or "(none)"
        typer.echo(f"  {target} <- {deps}")
    typer.echo(f"  order: {' -> '.join(topological_order(g))}")


@app.command("run")
def run_cmd(
    ctx: typer.Context,
    target: str = typer.Argument(..., help="Model target, e.g. ref.id_crosswalk."),
) -> None:
    """Run one model — no dependency check; use ``run-all`` for the full graph."""
    models = load_models(ctx.obj)
    if target not in models:
        raise typer.BadParameter(f"no such model: {target!r} (known: {sorted(models)})")
    con = lake_connect()
    try:
        rows = runner.run_model(con, models[target])
    finally:
        con.close()
    typer.echo(f"  {target}: {rows} rows")


@app.command("run-all")
def run_all_cmd(ctx: typer.Context) -> None:
    """Run every model in dependency order."""
    models = load_models(ctx.obj)
    con = lake_connect()
    try:
        results = runner.run_all(con, models)
    finally:
        con.close()
    for target, rows in results.items():
        typer.echo(f"  {target}: {rows} rows")


if __name__ == "__main__":
    app()
