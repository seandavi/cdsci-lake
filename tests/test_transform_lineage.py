"""sqlglot table + column lineage extraction (ADR-0021, cdsci-lake#113). Offline."""

from __future__ import annotations

from pathlib import Path

import duckdb

from cdsci.lake.transform.lineage import (
    LineageEdge,
    catalog_schema,
    column_lineage,
    table_dependencies,
)
from cdsci.lake.transform.models import Model


def _model(sql: str, target: str = "m.out") -> Model:
    return Model(target=target, sql=sql, path=Path(f"{target}.sql"))


def _lake() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("ATTACH ':memory:' AS lake")
    con.execute("CREATE SCHEMA lake.s")
    con.execute("CREATE TABLE lake.s.a (id INTEGER, w INTEGER)")
    con.execute("CREATE TABLE lake.s.b (id INTEGER, v VARCHAR)")
    return con


def test_table_dependencies_include_external_inputs_but_not_ctes_or_functions():
    m = _model(
        "WITH x AS (SELECT id FROM lake.s.a) "
        "SELECT x.id, p.v FROM x JOIN s.b p ON x.id = p.id "
        "UNION ALL SELECT 1, 'z' FROM read_parquet('f.parquet')"
    )
    assert table_dependencies(m) == {"s.a", "s.b"}


def test_catalog_schema_reads_only_requested_existing_tables():
    schema = catalog_schema(_lake(), ["s.a", "s.missing"])
    assert schema == {"lake": {"s": {"a": {"id": "INTEGER", "w": "INTEGER"}}}}


def test_column_lineage_through_cte_join_and_expression():
    m = _model(
        "WITH x AS (SELECT a.id, b.v FROM lake.s.a a JOIN lake.s.b b ON a.id = b.id) "
        "SELECT id, upper(v) AS vu, 1 AS one FROM x"
    )
    assert column_lineage(m) == [
        LineageEdge("m.out", "id", "s.a", "id"),
        LineageEdge("m.out", "vu", "s.b", "v"),
    ]


def test_select_star_resolves_only_with_schema():
    con = _lake()
    m = _model("SELECT * FROM lake.s.b")
    assert column_lineage(m) == []
    schema = catalog_schema(con, table_dependencies(m))
    assert column_lineage(m, schema) == [
        LineageEdge("m.out", "id", "s.b", "id"),
        LineageEdge("m.out", "v", "s.b", "v"),
    ]


def test_unqualified_join_column_resolves_with_schema():
    con = _lake()
    m = _model("SELECT v, w FROM lake.s.a JOIN lake.s.b USING (id)")
    schema = catalog_schema(con, table_dependencies(m))
    assert set(column_lineage(m, schema)) == {
        LineageEdge("m.out", "v", "s.b", "v"),
        LineageEdge("m.out", "w", "s.a", "w"),
    }


def test_unparseable_sql_yields_no_edges_instead_of_raising():
    assert column_lineage(_model("SELECT FROM WHERE (")) == []
