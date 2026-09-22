"""CLI: ``python -m cdsci.lake.transform`` — run SQL-file transform models (ADR-0015).

Mirrors ``maintenance_cli.py``'s shape: a typer app, ``--log-level`` via loguru,
``lake_connect()`` per command. ``--models-dir`` (default ``models``, the repo's
top-level SQL-file directory) is the one piece of shared state, threaded through
``typer.Context``.
"""

from __future__ import annotations

import json

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
    log_json: bool = typer.Option(
        False, "--log-json", help="Emit structured JSON log events (design §8.2)."
    ),
    models_dir: str = typer.Option("models", "--models-dir", help="Directory of *.sql models."),
) -> None:
    configure(log_level, json=log_json)
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
    target: str = typer.Argument(..., help="Model target, e.g. ncbi_gene2pubmed.gene_publication."),
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


@app.command("sync")
def sync_cmd() -> None:
    """Sync SQLMesh state into ``lake_ops`` (the transform sync seam, cdsci-lake#85).

    Builds the SQLMesh ``Context`` from ``transform/config.py`` (relative to the
    working directory, same convention the ``sqlmesh`` CLI itself uses) and opens
    the lake read-only -- this command only writes ``lake_ops``, never the lake
    catalog. ``sqlmesh`` stays behind the ``[transform]`` extra: imported here,
    not at module load, so ``list``/``graph``/``run``/``run-all`` don't need it.
    """
    from sqlmesh.core.context import Context

    from .sync import sync

    context = Context(paths="transform")
    con = lake_connect(read_only=True, with_ops=True)
    try:
        report = sync(con, context)
    finally:
        con.close()
    typer.echo(json.dumps(report.to_dict()))


if __name__ == "__main__":
    app()
