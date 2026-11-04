"""Offline tests for the OBO source (bioc-on-ice#183).

The fixture is written at test time in the real files' dialect (probed on EFO
v3.95.0, HsapDv and MmusDv 2025-01-23, 2026-11-03): one pretty-printed JSON
document (Jackson's ``"key" : value``), ``graphs[0]`` with ``id``, ``meta``
(``version`` an IRI), ``nodes``, ``edges`` and axiom arrays that do not land;
nodes with or without ``lbl``, ``type`` and ``meta``; ``deprecated`` a JSON
boolean; newlines inside values; edges with or without ``meta``.
"""

from __future__ import annotations

import importlib
import json
import shutil
from pathlib import Path

import pytest
from typer.testing import CliRunner

from cdsci.lake import Settings, lake_connect
from cdsci.lake.sources import obo

# the module itself: the package's `ingest` attribute is the function
INGEST = importlib.import_module("cdsci.lake.sources.obo.ingest")

SNAP = "SELECT max(snapshot_id) FROM lake.snapshots()"
ROWS = ("SELECT ontology, kind, coalesce(id, sub || ' ' || pred || ' ' || obj), release_version "
        "FROM lake.obo.element ORDER BY ALL")
CL_V1 = "http://purl.obolibrary.org/obo/cl/releases/2026-06-08/cl.json"
CL_V2 = "http://purl.obolibrary.org/obo/cl/releases/2026-10-01/cl.json"
GO_V1 = "http://purl.obolibrary.org/obo/go/releases/2026-07-26/go.owl"
P = "http://purl.obolibrary.org/obo/"

T_CELL = {"id": P + "CL_0000084", "lbl": "T cell", "type": "CLASS", "meta": {
    "definition": {"val": "A type of lymphocyte whose defining characteristic is the "
                          "expression of a T cell receptor complex.", "xrefs": ["PMID:1"]},
    "synonyms": [{"pred": "hasExactSynonym", "val": "T-lymphocyte"},
                 {"pred": "hasRelatedSynonym", "val": "T-cell"}],
    "basicPropertyValues": [{"pred": "http://www.geneontology.org/formats/oboInOwl#"
                                     "hasOBONamespace", "val": "cell"}]}}
# deprecated, unlabeled, a newline and a '"' inside a value
OBSOLETE = {"id": P + "CL_0000001", "type": "CLASS", "meta": {
    "deprecated": True, "comments": ['line one\nline "two"'],
    "basicPropertyValues": [{"pred": P + "IAO_0100001", "val": P + "CL_0000084"}]}}
# no meta, no type (MmusDv-style); an IRI outside the OBO PURL convention
BARE = {"id": "http://identifiers.org/ensembl/ENSG00000010610", "lbl": "CD4"}
PROPERTY = {"id": P + "BFO_0000050", "lbl": "part of", "type": "PROPERTY"}
IS_A = {"sub": P + "CL_0000084", "pred": "is_a", "obj": P + "CL_0000000"}
PART_OF = {"sub": P + "CL_0000084", "pred": P + "BFO_0000050", "obj": P + "UBERON_0002405",
           "meta": {"basicPropertyValues": [
               {"pred": "http://www.geneontology.org/formats/oboInOwl#source", "val": "x"}]}}


def graph(version: str | None, nodes: list | None, edges: list | None, **graph_keys) -> dict:
    g = {"id": P + "cl.owl", "meta": {"basicPropertyValues": [
        {"pred": "http://www.w3.org/2002/07/owl#versionInfo", "val": "2026-06-08"}]}}
    if version is not None:
        g["meta"]["version"] = version
    if nodes is not None:
        g["nodes"] = nodes
    if edges is not None:
        g["edges"] = edges
    return {"graphs": [{**g, "equivalentNodesSets": [], "logicalDefinitionAxioms": [],
                        "domainRangeAxioms": [], "propertyChainAxioms": [], **graph_keys}]}


def write(path: Path, doc: dict | str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(doc if isinstance(doc, str)
                    else json.dumps(doc, indent=2, separators=(",", " : ")))
    return path


def cl(tmp_path: Path, version: str = CL_V1, nodes=None, edges=None, name="cl.json") -> Path:
    return write(tmp_path / version.rsplit("/", 2)[-2] / name,
                 graph(version, nodes or [T_CELL, OBSOLETE, BARE, PROPERTY],
                       edges or [IS_A, PART_OF]))


@pytest.fixture
def lake_settings(tmp_path: Path) -> Settings:
    # lake_backend pinned: CU_OPENALEX_LAKE_BACKEND=postgres in the environment or a
    # .env would otherwise point these writes at the shared lake.
    return Settings(storage_base_uri=f"file://{tmp_path}", lake_backend="local",
                    lake_catalog=str(tmp_path / "catalog.ducklake"))


def seed(con, tmp_path: Path) -> int:
    obo.curate(con, "cl", cl(tmp_path / "seed"), CL_V1)
    obo.curate(con, "go", write(tmp_path / "seed" / "go.json", graph(GO_V1, [T_CELL], [IS_A])),
               GO_V1)
    return con.execute(SNAP).fetchone()[0]


def test_obo_lands_whole_and_a_new_release_replaces_only_its_ontology(lake_settings, tmp_path):
    con = lake_connect(lake_settings)
    try:
        before = seed(con, tmp_path)
        # raw.obo__<o>'s columns, names and types, without landed_in, after the ontology
        assert [(c[0], c[1]) for c in con.execute("DESCRIBE lake.obo.element").fetchall()] == [
            (c, "VARCHAR") for c in ("ontology", "kind", "id", "lbl", "sub", "pred", "obj",
                                     "meta", "release_version")]
        rows = {(r[0], r[2] or f"{r[4]} {r[5]}"): r for r in con.execute(
            "SELECT * FROM lake.obo.element").fetchall()}
        assert len(rows) == 6 + 2
        # meta is the object verbatim, NULL where the file has none; no lbl is NULL
        node = rows[("cl", P + "CL_0000001")]
        assert node[1:4] == ("node", P + "CL_0000001", None)
        assert json.loads(node[7]) == OBSOLETE["meta"]
        assert rows[("cl", BARE["id"])] == (
            "cl", "node", BARE["id"], "CD4", None, None, None, None, CL_V1)
        assert rows[("cl", P + "CL_0000084")][3] == "T cell"
        assert rows[("cl", f"{P}CL_0000084 is_a")] == (
            "cl", "edge", None, None, P + "CL_0000084", "is_a", P + "CL_0000000", None, CL_V1)
        part_of = rows[("cl", f"{P}CL_0000084 {P}BFO_0000050")]
        assert part_of[6] == P + "UBERON_0002405"
        assert json.loads(part_of[7]) == PART_OF["meta"]

        # re-landing the same release writes nothing
        assert obo.curate(con, "cl", cl(tmp_path / "seed"), CL_V1) == 8
        assert con.execute(SNAP).fetchone()[0] == before

        # a limited load never prunes (and returns the table's row count); a full load
        # of the next release drops CL's withdrawn rows and restamps CL's, not GO's
        nxt = cl(tmp_path / "next", CL_V2, [T_CELL, BARE], [IS_A])
        assert obo.curate(con, "cl", nxt, CL_V2, limit=1) == 8
        assert obo.curate(con, "cl", nxt, CL_V2) == 5
        assert con.execute(ROWS).fetchall() == [
            ("cl", "edge", f"{P}CL_0000084 is_a {P}CL_0000000", CL_V2),
            ("cl", "node", BARE["id"], CL_V2),
            ("cl", "node", P + "CL_0000084", CL_V2),
            ("go", "edge", f"{P}CL_0000084 is_a {P}CL_0000000", GO_V1),
            ("go", "node", P + "CL_0000084", GO_V1),
        ]
    finally:
        con.close()


def test_a_malformed_graph_fails_before_landing_and_keeps_the_table(lake_settings, tmp_path):
    con = lake_connect(lake_settings)
    try:
        before = seed(con, tmp_path)
        rows = con.execute(ROWS).fetchall()
        # past a limited load's rows: only a whole-input check sees these
        padding = [{"id": f"{P}CL_{i:07d}", "lbl": f"cell {i}"} for i in range(2, 5000)]
        edge = [{"sub": f"{P}CL_{i:07d}", "pred": "is_a", "obj": P + "CL_0000000"}
                for i in range(2, 5000)]
        doc = json.dumps(graph(CL_V2, [T_CELL], [IS_A]))
        bad = {
            "a node without an id": graph(CL_V2, [T_CELL, *padding, {"lbl": "no id"}], [IS_A]),
            "an edge without sub, pred or obj": graph(
                CL_V2, [T_CELL], [IS_A, *edge, {"sub": P + "CL_0000084", "pred": "is_a"}]),
            "a repeated node or edge": graph(CL_V2, [T_CELL, *padding], [IS_A, *edge, IS_A]),
            "no graphs\\[0\\].nodes or graphs\\[0\\].edges": graph(CL_V2, [T_CELL], None),
            "carries no graphs\\[0\\].meta.version": graph(None, [T_CELL], [IS_A]),
            "2 JSON documents": doc + "\n" + doc,
            "is version": graph(CL_V1, [T_CELL], [IS_A]),
        }
        for i, (match, body) in enumerate(bad.items()):
            path = write(tmp_path / "bad" / f"{i}.json", body)
            for limit in (None, 1):
                with pytest.raises(ValueError, match=match):
                    obo.curate(con, "cl", path, CL_V2, limit=limit)
        assert con.execute(SNAP).fetchone()[0] == before
        assert con.execute(ROWS).fetchall() == rows
    finally:
        con.close()


def test_an_empty_graph_is_refused_and_the_table_kept(lake_settings, tmp_path):
    con = lake_connect(lake_settings)
    try:
        before = seed(con, tmp_path)
        rows = con.execute(ROWS).fetchall()
        for i, body in enumerate((graph(CL_V2, [], []), graph(CL_V2, [], [IS_A]))):
            path = write(tmp_path / "empty" / f"{i}.json", body)
            for limit in (None, 1):
                with pytest.raises(ValueError, match="zero nodes"):
                    obo.curate(con, "cl", path, CL_V2, limit=limit)
        for limit in (0, -1):
            with pytest.raises(ValueError, match="limit must be positive"):
                obo.curate(con, "cl", cl(tmp_path / "one"), CL_V1, limit=limit)
        with pytest.raises(ValueError, match="not an ontology name"):
            obo.curate(con, "cl' OR 1=1 --", cl(tmp_path / "one"), CL_V1)
        assert con.execute(SNAP).fetchone()[0] == before
        assert con.execute(ROWS).fetchall() == rows
    finally:
        con.close()


def test_a_local_file_lands_one_ontology_under_its_in_band_version(lake_settings, tmp_path):
    path = cl(tmp_path / "in")
    with pytest.raises(ValueError, match="pass ontology"):
        obo.ingest(file=str(path), settings=lake_settings)
    with pytest.raises(ValueError, match="carries its own"):
        obo.ingest(ontology="cl", file=str(path), version="v2026-06-08", settings=lake_settings)
    with pytest.raises(ValueError, match="not one of"):
        obo.ingest(ontology="hp", file=str(path), settings=lake_settings)
    with pytest.raises(ValueError, match="not a release name"):
        obo.ingest(ontology="cl", version="../x", settings=lake_settings)
    [summary] = obo.ingest(ontology="cl", file=str(path), settings=lake_settings)
    assert (summary["ontology"], summary["version"], summary["rows"]) == ("cl", CL_V1, 6)
    [again] = obo.ingest(ontology="cl", file=str(path), settings=lake_settings)
    assert (again["status"], again["changed"]) == ("idempotent", False)


def test_the_default_run_lands_every_ontology_and_a_version_names_a_release(
        lake_settings, tmp_path, monkeypatch):
    s = lake_settings.model_copy(update={
        "obo_urls": {"cl": "https://up/cl.json", "go": "https://up/go-basic.json"},
        "obo_release_urls": {"cl": "https://up/{version}/cl.json",
                             "go": "https://up/{version}/go-basic.json"}})
    upstream = {"cl.json": cl(tmp_path / "up"),
                "go-basic.json": write(tmp_path / "up" / "go.json",
                                       graph(GO_V1, [T_CELL], [IS_A]))}
    fetched = []

    def download(url, dest):
        fetched.append((url, dest))
        dest.parent.mkdir(parents=True, exist_ok=True)
        return Path(shutil.copy(upstream[url.rsplit("/", 1)[-1]], dest))

    monkeypatch.setattr(INGEST, "download", download)
    result = obo.ingest(settings=s)
    assert [(r["ontology"], r["version"], r["rows"]) for r in result] == [
        ("cl", CL_V1, 6), ("go", GO_V1, 8)]
    assert [u for u, _ in fetched] == ["https://up/cl.json", "https://up/go-basic.json"]
    # keyed by version under the source's raw dir
    fetched.clear()
    obo.ingest(ontology="cl", version="v2026-06-08", settings=s)
    [(url, dest)] = fetched
    assert url == "https://up/v2026-06-08/cl.json"
    assert dest.parts[-4:] == ("obo", "cl", "v2026-06-08", "cl.json")

    # the CLI prints one line per ontology's run
    monkeypatch.setattr(INGEST, "get_settings", lambda: s)
    from cdsci.lake.sources.obo.__main__ import app
    out = CliRunner().invoke(app, ["run"])
    assert out.exit_code == 0, out.output
    assert out.output.count("lake.obo.element <-") == 2
