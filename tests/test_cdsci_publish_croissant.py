"""cdsci.publish.croissant (ADR-0027 step 6): Croissant 1.1 JSON-LD from manifest.json, offline;
the generated bundle file validated and read back with mlcroissant."""

import json
from pathlib import Path

import duckdb
import pytest
from cdsci.publish import publish
from cdsci.publish.croissant import (
    CONFORMS_TO,
    PARQUET,
    UNMAPPED,
    croissant,
    field,
    relocate,
    unmapped,
)

HERE = Path(__file__).parent
DATASET = {"name": "product", "description": "A product.", "url": "https://example.org/product",
           "license": "https://creativecommons.org/licenses/by/4.0/",
           "creator": {"@type": "Person", "name": "Sean Davis"}, "citeAs": "Davis (2026)",
           "keywords": ["genomics"]}


def _manifest(columns, **table):
    t = {"schema": "annotation", "table": "gene", "path": "annotation/gene.parquet",
         "row_count": 3, "bytes": 1234, "sha256": "ab" * 32, "comment": "Genes.",
         "columns": columns, **table}
    return {"manifest_version": 1, "release": "2026-11-04",
            "published_at": "2026-11-04T10:00:00+00:00", "git_sha": None,
            "base_url": "https://example.org/data/latest/", "tables": [t], "provenance": []}


def _col(name, type_, doc=None):
    return {"name": name, "type": type_, "doc": doc}


# --- type mapping: every scalar type the three products' manifests carry, plus DuckDB's
# other scalars (parametrised ones match on their head)


@pytest.mark.parametrize("duckdb_type,data_type", [
    ("BIGINT", "sc:Integer"), ("INTEGER", "sc:Integer"), ("SMALLINT", "sc:Integer"),
    ("TINYINT", "sc:Integer"), ("HUGEINT", "sc:Integer"), ("UBIGINT", "sc:Integer"),
    ("DOUBLE", "sc:Float"), ("FLOAT", "sc:Float"), ("DECIMAL(18,3)", "sc:Float"),
    ("BOOLEAN", "sc:Boolean"),
    ("VARCHAR", "sc:Text"), ("UUID", "sc:Text"), ("JSON", "sc:Text"), ("ENUM('a', 'b')", "sc:Text"),
    ("DATE", "sc:Date"), ("TIME", "sc:Time"), ("TIME WITH TIME ZONE", "sc:Time"),
    ("TIMESTAMP", "sc:DateTime"), ("TIMESTAMP WITH TIME ZONE", "sc:DateTime"),
    ("TIMESTAMP_NS", "sc:DateTime"),
])
def test_scalar_types_map(duckdb_type, data_type):
    f = field("annotation.gene", _col("c", duckdb_type, "Doc."), "annotation/gene.parquet")
    assert f["dataType"] == data_type and f["description"] == "Doc." and "isArray" not in f
    assert f["source"] == {"fileObject": {"@id": "annotation/gene.parquet"},
                           "extract": {"column": "c"}}


@pytest.mark.parametrize("duckdb_type", ["BLOB", "MAP(VARCHAR, INTEGER)", "INTERVAL", "BIT",
                                         "UNION(a INTEGER, b VARCHAR)", "GEOMETRY"])
def test_unmappable_types_become_text_with_the_raw_type_in_the_description(duckdb_type):
    f = field("annotation.gene", _col("c", duckdb_type, "Doc."), "annotation/gene.parquet")
    assert f["dataType"] == "sc:Text"
    assert f["description"] == f"Doc. (DuckDB type {duckdb_type}; no Croissant data type)"
    doc = croissant(_manifest([_col("c", duckdb_type), _col("d", "INTEGER")]), dataset=DATASET)
    assert unmapped(doc) == ["annotation.gene/c"]
    assert doc["recordSet"][0]["field"][0]["description"].startswith("(DuckDB type")


def test_list_and_nested_list_are_arrays():
    f = field("rs", _col("ids", "VARCHAR[]"), "p")
    assert f["dataType"] == "sc:Text" and f["isArray"] is True and "arrayShape" not in f
    f = field("rs", _col("m", "DOUBLE[][]"), "p")
    assert f["isArray"] is True and f["arrayShape"] == "-1,-1" and f["dataType"] == "sc:Float"
    assert field("rs", _col("v", "INTEGER[3]"), "p")["isArray"] is True


def test_struct_becomes_sub_fields_and_a_struct_list_is_an_array():
    f = field("raw.pubmed", _col("authors", 'STRUCT(lastname VARCHAR, "orc id" VARCHAR, '
                                 "pos STRUCT(x DOUBLE, tags VARCHAR[]))[]", "Authors."), "p")
    assert "dataType" not in f and f["isArray"] is True and f["description"] == "Authors."
    assert f["source"]["extract"] == {"column": "authors"}
    subs = f["subField"]
    assert [(s["@id"], s["name"], s.get("dataType")) for s in subs] == [
        ("raw.pubmed/authors/lastname", "lastname", "sc:Text"),
        ("raw.pubmed/authors/orc_id", "orc id", "sc:Text"),
        ("raw.pubmed/authors/pos", "pos", None)]
    assert all("source" not in s for s in subs)
    pos = subs[2]["subField"]
    assert pos[0]["dataType"] == "sc:Float"
    assert pos[1]["dataType"] == "sc:Text" and pos[1]["isArray"] is True


def test_field_semantics_append_data_types_and_set_properties_and_context_prefixes():
    semantics = {"annotation.gene": {
        "gene_id": {"equivalentProperty": "wdt:P594", "dataType": "wd:Q7187"},
        "symbol": {"bioregistry:prefix": "hgnc.symbol"}}}
    doc = croissant(_manifest([_col("gene_id", "VARCHAR"), _col("symbol", "VARCHAR"),
                               _col("n", "INTEGER")]),
                    dataset={**DATASET, "@context": {"wd": "http://www.wikidata.org/entity/"}},
                    field_semantics=semantics)
    assert doc["@context"]["wd"] == "http://www.wikidata.org/entity/"
    assert doc["@context"]["@vocab"] == "https://schema.org/"
    gene_id, symbol, n = doc["recordSet"][0]["field"]
    assert gene_id["dataType"] == ["sc:Text", "wd:Q7187"]
    assert gene_id["equivalentProperty"] == "wdt:P594"
    assert symbol["bioregistry:prefix"] == "hgnc.symbol" and symbol["dataType"] == "sc:Text"
    assert "equivalentProperty" not in n


def test_dataset_file_objects_and_record_sets():
    doc = croissant(_manifest([_col("id", "BIGINT", "The key."), _col("x", "VARCHAR")]),
                    dataset=DATASET)
    assert doc["@type"] == "sc:Dataset" and doc["conformsTo"] == CONFORMS_TO
    assert doc["version"] == "2026-11-04"
    assert doc["datePublished"] == "2026-11-04T10:00:00+00:00"
    for k, v in DATASET.items():
        assert doc[k] == v
    assert doc["distribution"] == [{
        "@type": "cr:FileObject", "@id": "annotation/gene.parquet",
        "name": "annotation/gene.parquet",
        "contentUrl": "https://example.org/data/latest/annotation/gene.parquet",
        "encodingFormat": PARQUET, "contentSize": "1234 B", "sha256": "ab" * 32}]
    rs = doc["recordSet"][0]
    assert rs["@id"] == rs["name"] == "annotation.gene" and rs["description"] == "Genes."
    assert [f["@id"] for f in rs["field"]] == ["annotation.gene/id", "annotation.gene/x"]
    assert rs["field"][0]["description"] == "The key." and "description" not in rs["field"][1]
    assert "provenance" not in json.dumps(doc)


def test_dataset_may_override_version_and_date_and_a_missing_comment_is_no_description():
    doc = croissant(_manifest([], comment=None),
                    dataset={**DATASET, "version": "1.2.0", "datePublished": "2026-11-05"})
    assert doc["version"] == "1.2.0" and doc["datePublished"] == "2026-11-05"
    assert "description" not in doc["recordSet"][0]


def test_whitespace_in_a_column_name_stays_out_of_the_id_and_collisions_are_refused():
    f = field("raw.tri", _col("5.1 - FUGITIVE AIR", "DOUBLE"), "p")
    assert f["@id"] == "raw.tri/5.1_-_FUGITIVE_AIR" and f["name"] == "5.1 - FUGITIVE AIR"
    assert f["source"]["extract"]["column"] == "5.1 - FUGITIVE AIR"
    with pytest.raises(ValueError, match="collide.*a_b"):
        croissant(_manifest([_col("a b", "INTEGER"), _col("a_b", "INTEGER")]), dataset=DATASET)


def test_relocate_rewrites_only_table_file_objects():
    doc = croissant(_manifest([_col("id", "BIGINT")]), dataset=DATASET)
    doc["distribution"].append({"@type": "cr:FileSet", "@id": "other", "includes": "*.csv"})
    moved = relocate(doc, "https://example.org/data/v2026-11-04/")
    assert moved["distribution"][0]["contentUrl"] == (
        "https://example.org/data/v2026-11-04/annotation/gene.parquet")
    assert moved["distribution"][1] == doc["distribution"][1]
    assert relocate(doc, ".")["distribution"][0]["contentUrl"] == "./annotation/gene.parquet"
    assert doc["distribution"][0]["contentUrl"].startswith("https://example.org/data/latest/")


# --- publish() and mlcroissant


@pytest.fixture
def con():
    c = duckdb.connect()
    c.execute("ATTACH ':memory:' AS src")
    c.execute("CREATE TABLE src.widget AS SELECT * FROM (VALUES "
              "(1, 'a', ['x', 'y'], {'pmid': '1', 'n': 2}), "
              "(2, 'b', [], {'pmid': NULL, 'n': 3})) t(id, name, tags, ref)")
    return c


def _publish(con, out, **kw):
    return publish(con, out, tables=[("annotation", "widget", "SELECT * FROM src.widget")],
                   docs=lambda n: ("Widgets.", {"id": "The key."}), base_url=out.as_uri(),
                   db_file="product.duckdb", code_dir=HERE, **kw)


def test_publish_writes_croissant_json_only_when_asked(con, tmp_path):
    _publish(con, tmp_path / "plain")
    assert not (tmp_path / "plain/croissant.json").exists()

    out = tmp_path / "with"
    semantics = {"annotation.widget": {"id": {"equivalentProperty": "sc:identifier"}}}
    m = _publish(con, out, dataset=DATASET, field_semantics=semantics)
    doc = json.loads((out / "croissant.json").read_text())
    assert doc == croissant(m, dataset=DATASET, field_semantics=semantics)
    assert doc["distribution"][0]["sha256"] == m["tables"][0]["sha256"]
    assert [f["dataType"] for f in doc["recordSet"][0]["field"][:3]] == [
        "sc:Integer", "sc:Text", "sc:Text"]
    assert doc["recordSet"][0]["field"][2]["isArray"] is True
    assert [s["name"] for s in doc["recordSet"][0]["field"][3]["subField"]] == ["pmid", "n"]


def test_mlcroissant_validates_and_reads_the_bundle_offline(con, tmp_path):
    mlc = pytest.importorskip("mlcroissant")
    out = tmp_path / "release"
    _publish(con, out, dataset=DATASET)
    # The bundle's contentUrls are file:// (the test base_url); mlcroissant resolves a relative
    # contentUrl against the JSON-LD file's directory, the Hugging Face copy's form.
    local = out / "croissant.local.json"
    local.write_text(json.dumps(relocate(json.loads((out / "croissant.json").read_text()), ".")))
    ds = mlc.Dataset(jsonld=local)
    assert not ds.metadata.issues.errors, ds.metadata.issues.report()
    rows = list(ds.records("annotation.widget"))
    assert [r["annotation.widget/id"] for r in rows] == [1, 2]
    assert [r["annotation.widget/name"] for r in rows] == [b"a", b"b"]
    assert list(rows[0]["annotation.widget/tags"]) == [b"x", b"y"]


def test_mlcroissant_accepts_field_semantics_and_a_struct_list():
    mlc = pytest.importorskip("mlcroissant")
    columns = [_col("gene_id", "VARCHAR", "HGNC id."), _col("authors", "STRUCT(a VARCHAR)[]"),
               _col("blob", "BLOB"), _col("5.1 - FUGITIVE AIR", "DOUBLE")]
    doc = croissant(_manifest(columns),
                    dataset={**DATASET, "@context": {"wdt": "http://www.wikidata.org/prop/direct/"}},
                    field_semantics={"annotation.gene": {"gene_id": {"equivalentProperty":
                                                                       "wdt:P354"}}})
    issues = mlc.Dataset(jsonld=doc).metadata.issues
    assert not issues.errors, issues.report()
    assert UNMAPPED in doc["recordSet"][0]["field"][2]["description"]
