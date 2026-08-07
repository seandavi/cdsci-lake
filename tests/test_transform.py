"""Offline tests for ``cdsci.lake.transform`` (ADR-0015).

Exercise model discovery, the dependency graph/topo sort, model execution
against a local DuckLake, and the parquet/duckdb reverse-ETL adapters. No
network, no Postgres — the ``iceberg`` adapter needs a live REST catalog and
is exercised only by manual smoke test (targets.py's docstring notes this).
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from cdsci.lake import Settings, lake_connect
from cdsci.lake.transform.graph import build_graph, topological_order
from cdsci.lake.transform.models import Model, load_models
from cdsci.lake.transform.runner import run_all, run_model
from cdsci.lake.transform.targets import Target, publish


@pytest.fixture
def lake_settings(tmp_path: Path) -> Settings:
    return Settings(storage_base_uri=f"file://{tmp_path / 'lake'}")


@pytest.fixture
def models_dir(tmp_path: Path) -> Path:
    root = tmp_path / "models"
    (root / "a").mkdir(parents=True)
    (root / "a" / "t1.sql").write_text("SELECT 1 AS x, 'a' AS label")
    (root / "a" / "t2.sql").write_text("SELECT x * 2 AS y FROM lake.a.t1")
    return root


def test_load_models_derives_target_from_path(models_dir: Path):
    models = load_models(models_dir)
    assert set(models) == {"a.t1", "a.t2"}
    assert models["a.t1"].sql == "SELECT 1 AS x, 'a' AS label"


def test_load_models_rejects_empty_file(tmp_path: Path):
    (tmp_path / "empty.sql").write_text("   ")
    with pytest.raises(ValueError, match="empty transform model"):
        load_models(tmp_path)


def test_graph_and_topological_order(models_dir: Path):
    models = load_models(models_dir)
    graph = build_graph(models)
    assert graph == {"a.t1": set(), "a.t2": {"a.t1"}}
    assert topological_order(graph) == ["a.t1", "a.t2"]


def test_topological_order_raises_on_cycle():
    graph = {"a": {"b"}, "b": {"a"}}
    with pytest.raises(ValueError, match="cycle"):
        topological_order(graph)


def test_unresolved_reference_is_a_leaf_not_a_dependency(tmp_path: Path):
    """A read_parquet(...)/external-table ref never matches a known model target."""
    (tmp_path / "t.sql").write_text("SELECT * FROM read_parquet('s3://bucket/f.parquet')")
    models = load_models(tmp_path)
    assert build_graph(models) == {"t": set()}


def test_run_model_creates_table_and_records_run(lake_settings: Settings):
    con = lake_connect(lake_settings)
    try:
        model = Model("xf.t1", "SELECT 1 AS x, 'a' AS label", Path("xf/t1.sql"))
        rows = run_model(con, model)
        assert rows == 1
        assert con.execute("SELECT * FROM lake.xf.t1").fetchall() == [(1, "a")]

        run_row = con.execute(
            "SELECT source, status, rows_after FROM ops.lake_ops.run WHERE source = 'xf.t1'"
        ).fetchone()
        assert run_row == ("xf.t1", "success", 1)
        # The model self-registered as a lake_ops.source (not in the built-in SOURCES).
        assert con.execute(
            "SELECT writer FROM ops.lake_ops.source WHERE name = 'xf.t1'"
        ).fetchone() == ("cdsci",)
    finally:
        con.close()


def test_run_model_logs_lineage(lake_settings: Settings):
    """run_model logs every resolvable lineage edge -- no lake_ops.lineage table
    yet (ADR-0014) to persist into, so the log line is the only record today."""
    from loguru import logger as loguru_logger

    con = lake_connect(lake_settings)
    try:
        con.execute(
            "CREATE SCHEMA lake.src; "
            "CREATE TABLE lake.src.t AS SELECT 1 AS id, 'x' AS val"
        )
        model = Model("xf.derived", "SELECT id, val FROM lake.src.t", Path("xf/derived.sql"))

        lines: list[str] = []
        sink_id = loguru_logger.add(lambda msg: lines.append(msg.record["message"]), level="INFO")
        try:
            run_model(con, model)
        finally:
            loguru_logger.remove(sink_id)

        lineage_lines = [line for line in lines if line.startswith("lineage:")]
        assert "lineage: xf.derived.id <- src.t.id" in lineage_lines
        assert "lineage: xf.derived.val <- src.t.val" in lineage_lines
    finally:
        con.close()


def test_run_model_is_a_real_replace_not_upsert(lake_settings: Settings):
    """CREATE OR REPLACE — a second run with different data fully replaces, no merge."""
    con = lake_connect(lake_settings)
    try:
        model = Model("xf.t1", "SELECT * FROM (VALUES (1),(2)) v(x)", Path("xf/t1.sql"))
        run_model(con, model)
        model2 = Model("xf.t1", "SELECT * FROM (VALUES (9)) v(x)", Path("xf/t1.sql"))
        run_model(con, model2)
        assert con.execute("SELECT * FROM lake.xf.t1").fetchall() == [(9,)]
    finally:
        con.close()


def test_run_all_respects_dependency_order(lake_settings: Settings, models_dir: Path):
    con = lake_connect(lake_settings)
    try:
        models = load_models(models_dir)
        results = run_all(con, models)
        assert results == {"a.t1": 1, "a.t2": 1}
        assert con.execute("SELECT * FROM lake.a.t2").fetchall() == [(2,)]
    finally:
        con.close()


def test_publish_parquet_dated_and_latest(lake_settings: Settings, tmp_path: Path):
    con = lake_connect(lake_settings)
    try:
        con.execute("CREATE SCHEMA lake.a; CREATE TABLE lake.a.t1 AS SELECT 1 AS x")
        target = Target(
            "parquet",
            {
                "path": str(tmp_path / "pub" / "v{date}" / "t1.parquet"),
                "latest_path": str(tmp_path / "pub" / "latest" / "t1.parquet"),
            },
        )
        publish(con, "lake.a.t1", target, date="2026-08-07")
        dated = duckdb.sql(
            f"SELECT * FROM read_parquet('{tmp_path}/pub/v2026-08-07/t1.parquet')"
        ).fetchall()
        latest = duckdb.sql(
            f"SELECT * FROM read_parquet('{tmp_path}/pub/latest/t1.parquet')"
        ).fetchall()
        assert dated == [(1,)]
        assert latest == [(1,)]
    finally:
        con.close()


def test_publish_duckdb_and_lake_table_noop(lake_settings: Settings, tmp_path: Path):
    con = lake_connect(lake_settings)
    try:
        con.execute("CREATE SCHEMA lake.a; CREATE TABLE lake.a.t1 AS SELECT 1 AS x")
        mart_path = tmp_path / "mart.duckdb"
        publish(con, "lake.a.t1", Target("duckdb", {"path": str(mart_path)}), date="2026-08-07")
        mart = duckdb.connect(str(mart_path))
        try:
            assert mart.execute("SELECT * FROM t1").fetchall() == [(1,)]
        finally:
            mart.close()

        # lake_table is a documented no-op: the model's own write already is the publish.
        publish(con, "lake.a.t1", Target("lake_table"), date="2026-08-07")
    finally:
        con.close()
