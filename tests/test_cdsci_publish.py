"""cdsci.publish (ADR-0027): offline round trip against a tiny in-memory DuckDB source,
manifest schema validation, and concordance."""

import json
from pathlib import Path

import duckdb
import jsonschema
import pytest
from cdsci.publish import concordance, manifest_schema, publish

HERE = Path(__file__).parent
TABLES = [("annotation", "widget", "SELECT * FROM src.widget")]


def _docs(name):
    if name == "annotation.widget":
        return "Widgets.", {"id": "The key."}
    return None, {}


@pytest.fixture
def con():
    c = duckdb.connect()
    c.execute("ATTACH ':memory:' AS src")
    c.execute("CREATE TABLE src.widget AS SELECT * FROM (VALUES (1, 'a'), (2, 'b')) t(id, name)")
    return c


def test_publish_round_trip(con, tmp_path):
    out = tmp_path / "release"
    m = publish(con, out, tables=TABLES, docs=_docs, base_url=out.as_uri(),
                db_file="product.duckdb", code_dir=HERE)

    assert json.loads((out / "manifest.json").read_text()) == json.loads(json.dumps(m))
    assert m["manifest_version"] == 1
    assert m["provenance"] == []
    widget = m["tables"][0]
    assert widget["row_count"] == 2
    assert widget["comment"] == "Widgets."
    assert {c["name"]: c["doc"] for c in widget["columns"]} == {"id": "The key.", "name": None}
    assert widget["bytes"] == (out / "annotation/widget.parquet").stat().st_size
    assert len(widget["sha256"]) == 64

    db = duckdb.connect(str(out / "product.duckdb"), read_only=True)
    assert db.sql("SELECT count(*) FROM annotation.widget").fetchone()[0] == 2

    local = duckdb.connect()
    local.execute((out / "restore.sql").read_text())
    assert local.sql("SELECT count(*) FROM annotation.widget").fetchone()[0] == 2


def test_manifest_matches_schema(con, tmp_path):
    out = tmp_path / "release"
    m = publish(con, out, tables=TABLES, docs=_docs, base_url=out.as_uri(),
                db_file="product.duckdb", code_dir=HERE)
    jsonschema.validate(m, manifest_schema())


def test_unknown_table_is_refused(con, tmp_path):
    with pytest.raises(ValueError, match="not a published table"):
        publish(con, tmp_path, tables=TABLES, docs=_docs, base_url=tmp_path.as_uri(),
                db_file="product.duckdb", code_dir=HERE, only=["annotation.nope"])


def test_concordance_reports_nothing_for_identical_input(con, tmp_path):
    a_out, b_out = tmp_path / "a", tmp_path / "b"
    a = publish(con, a_out, tables=TABLES, docs=_docs, base_url=a_out.as_uri(),
                db_file="product.duckdb", code_dir=HERE)
    b = publish(con, b_out, tables=TABLES, docs=_docs, base_url=b_out.as_uri(),
                db_file="product.duckdb", code_dir=HERE)
    assert concordance(a, b) == []


def test_concordance_detects_row_count_column_type_and_dropped_table(con, tmp_path):
    old_out, new_out = tmp_path / "old", tmp_path / "new"
    old = publish(con, old_out, tables=TABLES, docs=_docs, base_url=old_out.as_uri(),
                  db_file="product.duckdb", code_dir=HERE)

    con.execute("INSERT INTO src.widget VALUES (3, 'c')")
    con.execute("ALTER TABLE src.widget ALTER id TYPE BIGINT")
    new = publish(con, new_out, tables=TABLES, docs=_docs, base_url=new_out.as_uri(),
                  db_file="product.duckdb", code_dir=HERE)

    diffs = concordance(old, new)
    assert any("row_count" in d for d in diffs)
    assert any("columns" in d for d in diffs)

    new["tables"] = []
    diffs = concordance(old, new)
    assert diffs == ["annotation.widget: dropped"]


def test_provenance_dates_serialize(con, tmp_path):
    con.execute("CREATE TABLE src.release AS SELECT 'acs' AS source, DATE '2026-11-01' AS fetched")
    out = tmp_path / "release"
    m = publish(con, out, tables=TABLES, docs=_docs, base_url=out.as_uri(),
                db_file="product.duckdb", code_dir=HERE,
                provenance_sql="SELECT * FROM src.release")
    written = json.loads((out / "manifest.json").read_text())
    assert written["provenance"] == [{"source": "acs", "fetched": "2026-11-01"}]
    assert m["provenance"][0]["source"] == "acs"


def test_non_empty_out_is_refused(con, tmp_path):
    (tmp_path / "stale.parquet").write_text("left over")
    with pytest.raises(ValueError, match="not empty"):
        publish(con, tmp_path, tables=TABLES, docs=_docs, base_url=tmp_path.as_uri(),
                db_file="product.duckdb", code_dir=HERE)


def test_unknown_table_creates_nothing(con, tmp_path):
    out = tmp_path / "release"
    with pytest.raises(ValueError, match="not a published table"):
        publish(con, out, tables=TABLES, docs=_docs, base_url=out.as_uri(),
                db_file="product.duckdb", code_dir=HERE, only=["annotation.nope"])
    assert not out.exists()


def test_a_product_layout_drives_files_views_and_restore(con, tmp_path):
    # omicidx: main-schema tables at the folder's top level, marts under their schema
    out = tmp_path / "release"
    m = publish(con, out, tables=TABLES + [("main", "gadget", "SELECT * FROM src.widget")],
                docs=_docs, base_url=out.as_uri(), db_file="product.duckdb", code_dir=HERE,
                path=lambda s, t: f"{t}.parquet" if s == "main" else f"{s}/{t}.parquet")
    assert [t["path"] for t in m["tables"]] == ["annotation/widget.parquet", "gadget.parquet"]
    assert (out / "gadget.parquet").exists()
    assert f"read_parquet('{out.as_uri()}/gadget.parquet')" in (out / "views.sql").read_text()
    db = duckdb.connect(str(out / "product.duckdb"), read_only=True)
    assert db.sql("SELECT count(*) FROM main.gadget").fetchone()[0] == 2
    local = duckdb.connect()
    local.execute((out / "restore.sql").read_text())
    assert local.sql("SELECT count(*) FROM main.gadget").fetchone()[0] == 2
