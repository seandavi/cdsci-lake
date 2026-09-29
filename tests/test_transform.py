"""Offline tests for ``cdsci.lake.transform`` (ADR-0015).

Exercise model discovery, the dependency graph/topo sort, model execution
against a local DuckLake, and the parquet/duckdb reverse-ETL adapters. No
network, no Postgres — the ``iceberg`` target is disabled (cdsci-lake#63) and
raises before any catalog connection; see ``test_publish_iceberg_disabled``.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from cdsci.lake import Settings, lake_connect, ops
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


def test_topological_order_is_deterministic_by_level():
    graph = {"z": set(), "a": set(), "m": {"z", "a"}, "b": {"a"}}
    assert topological_order(graph) == ["a", "z", "b", "m"]


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


def test_run_model_records_and_replaces_lineage(lake_settings: Settings):
    """ADR-0021 / #114: table + column lineage land in lake_ops under the run's
    run_id, SELECT * resolves via the catalog, and a re-run with changed SQL
    drops the stale edges."""
    con = lake_connect(lake_settings)
    try:
        con.execute(
            "CREATE SCHEMA lake.src; "
            "CREATE TABLE lake.src.t AS SELECT 1 AS id, 'x' AS val; "
            "CREATE TABLE lake.src.u AS SELECT 1 AS id, 2 AS n"
        )
        base = Model("xf.base", "SELECT * FROM lake.src.t", Path("xf/base.sql"))
        joined = Model(
            "xf.joined",
            "SELECT b.id, b.val, u.n FROM lake.xf.base b JOIN lake.src.u u USING (id)",
            Path("xf/joined.sql"),
        )
        run_all(con, {"xf.base": base, "xf.joined": joined})

        def run_id(source: str) -> str:
            return con.execute(
                "SELECT run_id FROM ops.lake_ops.run WHERE source = ? "
                "ORDER BY started_at DESC LIMIT 1", [source]
            ).fetchone()[0]

        assert {(e["src_ref"], e["edge_type"], e["run_id"])
                for e in ops.lineage_for(con, "lake.xf.joined")} == {
            ("lake.xf.base", "sqlglot", run_id("xf.joined")),
            ("lake.src.u", "sqlglot", run_id("xf.joined")),
        }
        base_cols = ops.column_lineage_for(con, "lake.xf.base")
        assert [(c["dst_column"], c["src_ref"], c["src_column"]) for c in base_cols] == [
            ("id", "lake.src.t", "id"), ("val", "lake.src.t", "val"),
        ]
        assert {c["run_id"] for c in base_cols} == {run_id("xf.base")}
        assert [(c["dst_ref"], c["dst_column"]) for c in
                ops.column_lineage_for(con, "lake.xf.base", direction="downstream")] == [
            ("lake.xf.joined", "id"), ("lake.xf.joined", "val"),
        ]
        assert {a["ref"] for a in ops.list_assets(con)} >= {"lake.xf.base", "lake.xf.joined"}

        # Drop the join: the lake.src.u edges must disappear, not linger.
        run_model(con, Model("xf.joined", "SELECT id, val FROM lake.xf.base",
                             Path("xf/joined.sql")))
        assert {e["src_ref"] for e in ops.lineage_for(con, "lake.xf.joined")} == {
            "lake.xf.base"
        }
        assert {c["src_ref"] for c in ops.column_lineage_for(con, "lake.xf.joined")} == {
            "lake.xf.base"
        }
    finally:
        con.close()


def test_lineage_failure_never_fails_the_run(lake_settings: Settings, monkeypatch):
    con = lake_connect(lake_settings)
    try:
        def boom(*a, **kw):
            raise RuntimeError("lineage store down")

        monkeypatch.setattr(ops, "replace_model_lineage", boom)
        run_model(con, Model("xf.t1", "SELECT 1 AS x", Path("xf/t1.sql")))
        assert con.execute(
            "SELECT status FROM ops.lake_ops.run WHERE source = 'xf.t1'"
        ).fetchone() == ("success",)
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


def test_publish_date_optional_for_non_parquet_targets(lake_settings: Settings, tmp_path: Path):
    """date is parquet-only — the CLI's iceberg publish path never supplies one."""
    con = lake_connect(lake_settings)
    try:
        con.execute("CREATE SCHEMA lake.a; CREATE TABLE lake.a.t1 AS SELECT 1 AS x")
        mart_path = tmp_path / "mart.duckdb"
        publish(con, "lake.a.t1", Target("duckdb", {"path": str(mart_path)}))
        publish(con, "lake.a.t1", Target("lake_table"))
    finally:
        con.close()


def test_publish_parquet_requires_date(lake_settings: Settings):
    con = lake_connect(lake_settings)
    try:
        con.execute("CREATE SCHEMA lake.a; CREATE TABLE lake.a.t1 AS SELECT 1 AS x")
        with pytest.raises(ValueError, match="date is required"):
            publish(con, "lake.a.t1", Target("parquet", {"path": "/tmp/{date}/t1.parquet"}))
    finally:
        con.close()


def test_publish_iceberg_disabled(lake_settings: Settings):
    """cdsci-lake#63: the iceberg target must fail closed, before any catalog connection.

    Config deliberately omits endpoint/token/catalog — if this ever tried to
    connect, it would raise a KeyError or a connection error instead of this
    NotImplementedError, so the exact error type/message doubles as proof no
    catalog attach was attempted.
    """
    con = lake_connect(lake_settings)
    try:
        con.execute("CREATE SCHEMA lake.a; CREATE TABLE lake.a.t1 AS SELECT 1 AS x")
        with pytest.raises(NotImplementedError, match="cdsci-lake#63"):
            publish(con, "lake.a.t1", Target("iceberg", {}))
    finally:
        con.close()


def test_repo_models_load_as_plain_sql_with_metadata_and_tests():
    """ADR-0021 / #112: every shipped model is plain SQL with its directives and tests."""
    models = load_models(Path(__file__).parents[1] / "transform" / "models")
    assert len(models) == 15
    for target, m in models.items():
        assert not m.sql.lstrip().upper().startswith("MODEL"), target
        assert not m.license.startswith("UNSPECIFIED"), target
        assert m.description != f"cdsci-lake transform model: {target}", target
        assert m.tests, target
    assert sum(len(m.tests) for m in models.values()) == 35
    build_graph(models)  # parses, no cycles
