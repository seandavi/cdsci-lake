"""``cdsci.lake.transform.runner`` — execute a model inside ``ops.run`` (ADR-0015 §1/§4).

This is ADR-0013's parked ``rebuild`` verb, scoped to this module only — the EL
write path (:func:`cdsci.lake.connect.upsert`) stays ``upsert``-only. Reuses
``ops.run``/``Run.attribute`` exactly as ``upsert`` does today: one
``lake_ops.run`` row and one self-describing DuckLake snapshot per model.
"""

from __future__ import annotations

import duckdb

from .. import ops
from ..connect import LAKE
from ..log import logger
from .graph import build_graph, topological_order
from .lineage import model_lineage
from .models import Model


class ModelTestFailure(Exception):
    """A model's ``.test.sql`` assertion returned rows (it must return zero)."""


def run_model(con: duckdb.DuckDBPyConnection, model: Model) -> int:
    """``CREATE OR REPLACE {TABLE|VIEW} {LAKE}.{model.target} AS (model.sql)``; returns row count.

    Self-registers ``model.target`` as a ``lake_ops.source`` on every call
    (cheap delete-then-insert, ADR-0011 §4's "idempotent and self-healing"
    pattern) — a transform model isn't in the built-in ``SOURCES`` tuple, so
    without this every run would fall back to unattributed ``<source>:<source>``
    with a warning.

    Any declared ``model.tests`` run after the write commits, still inside
    ``ops.run``'s block — a failing test raises :class:`ModelTestFailure`,
    which ``ops.run`` catches and records as a normal ``error`` run, same
    treatment as a write that raised.
    """
    schema, table = model.target.split(".", 1)
    target = f"{LAKE}.{model.target}"
    ops.register_sources(
        con,
        writer="cdsci",
        sources=(
            ops.Source(
                model.target, schema, model.description,
                "on-demand", "sql-transform", model.license,
            ),
        ),
    )
    kind = "VIEW" if model.materialized == "view" else "TABLE"
    with ops.run(con, source=model.target, target=target) as r:
        with r.attribute(table):
            con.execute(f"CREATE SCHEMA IF NOT EXISTS {LAKE}.{schema};")
            con.execute(f"CREATE OR REPLACE {kind} {target} AS ({model.sql});")
        r.rows = con.execute(f"SELECT count(*) FROM {target}").fetchone()[0]
        ops.ensure_column_comments(con, target, model.column_comments)
        _run_tests(con, model, target)
    _log_lineage(model)
    return r.rows


def _run_tests(con: duckdb.DuckDBPyConnection, model: Model, target: str) -> None:
    """Run every ``model.tests`` query; each must return zero rows to pass."""
    for name, query in model.tests.items():
        rows = con.execute(query).fetchall()
        if rows:
            sample = rows[:5]
            raise ModelTestFailure(
                f"{model.target}: test {name!r} failed -- {len(rows)} row(s) violate it "
                f"(showing up to 5): {sample}"
            )
        logger.bind(ctx=f"transform:{model.target}").info("test {!r}: pass", name)


def _log_lineage(model: Model) -> None:
    """Log every best-effort lineage edge for ``model`` (ADR-0015 §2).

    No ``lake_ops.lineage`` table exists yet (ADR-0014) to persist these into
    — the log line *is* the record for now, so every edge is logged, not a
    summary count. Lineage computation can't fail a run (see
    :func:`cdsci.lake.transform.lineage.model_lineage`'s own try/except), so
    this always runs after a successful write.
    """
    bound = logger.bind(ctx=f"transform:{model.target}")
    edges = model_lineage(model)
    for edge in edges:
        bound.info(
            "lineage: {}.{} <- {}.{}",
            edge.target, edge.target_column, edge.source_table, edge.source_column,
        )
    if not edges:
        bound.info("lineage: no resolvable edges")


def run_all(con: duckdb.DuckDBPyConnection, models: dict[str, Model]) -> dict[str, int]:
    """Run every model in dependency order; return ``{target: row_count}``.

    Order comes from :func:`cdsci.lake.transform.graph.topological_order` — a
    dependency always runs before anything reading it.
    """
    order = topological_order(build_graph(models))
    logger.info("transform: running {} model(s) in order: {}", len(order), order)
    return {target: run_model(con, models[target]) for target in order}
