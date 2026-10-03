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
from .lineage import catalog_schema, column_lineage, table_dependencies
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
    treatment as a write that raised. Once tests pass, the model is registered
    as an asset and its lineage recorded under the same ``run_id`` (ADR-0021).
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
    with ops.run(con, source=model.target, target=target, version=model.fingerprint) as r:
        with r.attribute(table):
            con.execute(f"CREATE SCHEMA IF NOT EXISTS {LAKE}.{schema};")
            con.execute(f"CREATE OR REPLACE {kind} {target} AS ({model.sql});")
        r.rows = con.execute(f"SELECT count(*) FROM {target}").fetchone()[0]
        ops.ensure_column_comments(con, target, model.column_comments)
        _run_tests(con, model, target)
        ops.register_asset(
            con, ref=target, writer="cdsci", asset_type="lake_table", name=model.target
        )
        _record_lineage(con, model, target)
    return r.rows


def is_stale(con: duckdb.DuckDBPyConnection, model: Model) -> bool:
    """True when ``model`` needs a rebuild: never built successfully, SQL changed
    (fingerprint differs), its table is missing, or an input table changed in a
    snapshot after the model's last successful build.
    """
    last = ops.last_run(con, model.target, status="success")
    if last is None or last["version"] != model.fingerprint:
        return True
    schema, table = model.target.split(".", 1)
    exists = con.execute(
        "SELECT 1 FROM information_schema.tables "
        "WHERE table_catalog = ? AND table_schema = ? AND table_name = ?",
        [LAKE, schema, table],
    ).fetchone()
    if exists is None:
        return True
    built = last["snapshot_after"]
    for dep in table_dependencies(model):
        changed = ops.last_change_snapshot(con, f"{LAKE}.{dep}")
        if changed is not None and (built is None or changed > built):
            return True
    return False


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


def _record_lineage(con: duckdb.DuckDBPyConnection, model: Model, target: str) -> None:
    """Replace ``model``'s table + column lineage in ``lake_ops`` (ADR-0021 §3-4).

    Input schemas come from the lake catalog so ``SELECT *`` resolves. Lineage
    is observability: any failure here logs and never fails the model run.
    """
    bound = logger.bind(ctx=f"transform:{model.target}")
    try:
        deps = table_dependencies(model)
        edges = column_lineage(model, catalog_schema(con, deps, catalog=LAKE))
        ops.replace_model_lineage(
            con,
            dst_ref=target,
            src_refs=(f"{LAKE}.{d}" for d in deps),
            columns=((e.target_column, f"{LAKE}.{e.source_table}", e.source_column)
                     for e in edges),
        )
    except Exception as exc:
        bound.warning("lineage: not recorded: {}", exc)
        return
    bound.info("lineage: {} table(s), {} column edge(s)", len(deps), len(edges))


def run_all(con: duckdb.DuckDBPyConnection, models: dict[str, Model]) -> dict[str, int]:
    """Run every model in dependency order; return ``{target: row_count}``.

    Order comes from :func:`cdsci.lake.transform.graph.topological_order` — a
    dependency always runs before anything reading it.
    """
    order = topological_order(build_graph(models))
    logger.info("transform: running {} model(s) in order: {}", len(order), order)
    return {target: run_model(con, models[target]) for target in order}
