"""Offline tests for the PubTator3 source (bioc-on-ice#183).

The fixture is written at test time in the real files' dialect (probed on
prefixes of the 2026-10-06 gene and mutation dumps, 2026-11-03): gzip,
headerless, five tab-separated columns, LF, no quoting ('"' is data), '-' a real
concept id, an empty cell the only missing value.
"""

from __future__ import annotations

import gzip
import importlib
from pathlib import Path

import pytest

from cdsci.lake import Settings, lake_connect
from cdsci.lake.sources import pubtator3

# the module itself: the package's `ingest` attribute is the function
INGEST = importlib.import_module("cdsci.lake.sources.pubtator3.ingest")
SNAP = "SELECT max(snapshot_id) FROM lake.snapshots()"

DUMP = {
    "gene": ["100\tGene\t7157\tp53|TP53\tPubTator3",
             "100\tGene\t6900;1491938\tCNTN2\tPubTator3|gene2pubmed",
             '101\tGene\t-\tToll"-like receptor\tPubTator3',
             "102\tGene\t672\t\tgene2pubmed"],
    "disease": ["100\tDisease\tMESH:D009369\ttumor|Tumor\tPubTator3"],
    "chemical": ["100\tChemical\t-\taspirin-like\tPubTator3"],
    "species": ["100\tSpecies\t9606\thuman\tPubTator3"],
    "mutation": ["103\tMutation\ttmVar:p|SUB|S|80|I;HGVS:p.S80I;VariantGroup:2\tS80I\tPubTator3"],
}


def dump(root: Path, files: dict[str, list[str]]) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    for kind, lines in files.items():
        with gzip.open(root / f"{kind}2pubtator3.gz", "wt") as f:
            f.write("".join(line + "\n" for line in lines))
    return root


def rows(con, kind: str) -> list[tuple]:
    return con.execute(f"SELECT * FROM lake.pubtator3.{kind}_annotation "
                       "ORDER BY line_number").fetchall()


@pytest.fixture
def lake_settings(tmp_path: Path) -> Settings:
    # lake_backend pinned: CU_OPENALEX_LAKE_BACKEND=postgres in the environment or a
    # .env would otherwise point these writes at the shared lake.
    return Settings(storage_base_uri=f"file://{tmp_path}", lake_backend="local",
                    lake_catalog=str(tmp_path / "catalog.ducklake"))


def test_pubtator3_lands_each_file_verbatim_and_idempotently(lake_settings, tmp_path):
    d = dump(tmp_path / "d1", DUMP)
    result = pubtator3.ingest(directory=str(d), version="2026-09-19", batches=3,
                              settings=lake_settings)
    assert result["counts"] == {"gene_annotation": 4, "disease_annotation": 1,
                                "chemical_annotation": 1, "species_annotation": 1,
                                "mutation_annotation": 1}
    con = lake_connect(lake_settings)
    try:
        assert [(c[0], c[1]) for c in con.execute(
            "DESCRIBE lake.pubtator3.gene_annotation").fetchall()] == [
            ("line_number", "INTEGER"), *[(c, "VARCHAR") for c in pubtator3.COLUMNS],
            ("pubtator3_version", "VARCHAR")]
        # line_number is the line; '-' and '"' are data; only the empty cell is NULL
        assert rows(con, "gene") == [
            (1, "100", "Gene", "7157", "p53|TP53", "PubTator3", "2026-09-19"),
            (2, "100", "Gene", "6900;1491938", "CNTN2", "PubTator3|gene2pubmed", "2026-09-19"),
            (3, "101", "Gene", "-", 'Toll"-like receptor', "PubTator3", "2026-09-19"),
            (4, "102", "Gene", "672", None, "gene2pubmed", "2026-09-19")]
        before = con.execute(SNAP).fetchone()[0]
    finally:
        con.close()

    # the same dump again, in a different batching or retrieved on a later date, writes
    # nothing: pubtator3_version is out of change detection (ADR-0003)
    pubtator3.ingest(directory=str(d), version="2026-09-19", batches=2, settings=lake_settings)
    pubtator3.ingest(directory=str(d), version="2026-09-20", settings=lake_settings)
    con = lake_connect(lake_settings)
    try:
        assert con.execute(SNAP).fetchone()[0] == before
        assert {r[-1] for r in rows(con, "gene")} == {"2026-09-19"}
    finally:
        con.close()

    # a limited load never prunes; a full load of the next dump drops lines and
    # restamps only the line that changed
    changed = DUMP["gene"][1].replace("CNTN2", "CNTN2|TAG-1")
    nxt = dump(tmp_path / "d2", {**DUMP, "gene": [DUMP["gene"][0], changed]})
    pubtator3.ingest(directory=str(nxt), kind="gene", version="2026-10-06", limit=1,
                     settings=lake_settings)
    con = lake_connect(lake_settings)
    try:
        assert len(rows(con, "gene")) == 4
    finally:
        con.close()
    # batches=3: the dump no longer reaches line_number % 3 = 0, an empty load that
    # upsert alone refuses to prune; and a retry of the same dump writes nothing
    for _ in range(2):
        pubtator3.ingest(directory=str(nxt), kind="gene", version="2026-10-06", batches=3,
                         settings=lake_settings)
        con = lake_connect(lake_settings)
        try:
            assert [(r[0], r[4], r[-1]) for r in rows(con, "gene")] == [
                (1, "p53|TP53", "2026-09-19"), (2, "CNTN2|TAG-1", "2026-10-06")]
            # the other kinds were not part of this run
            assert rows(con, "disease")[0][-1] == "2026-09-19"
            after = con.execute(SNAP).fetchone()[0]
        finally:
            con.close()
    assert after == before + 2  # the shrinking run's line delete and merge; the retry none


def test_append_lands_an_empty_table_and_refuses_a_second_time(lake_settings, tmp_path):
    d = dump(tmp_path / "d1", DUMP)
    pubtator3.ingest(directory=str(d), kind="gene", version="2026-09-19", batches=3,
                     mode="append", settings=lake_settings)
    con = lake_connect(lake_settings)
    try:
        assert [r[0] for r in rows(con, "gene")] == [1, 2, 3, 4]
    finally:
        con.close()
    with pytest.raises(ValueError, match="already holds rows"):
        pubtator3.ingest(directory=str(d), kind="gene", version="2026-09-19", batches=3,
                         mode="append", settings=lake_settings)


@pytest.mark.parametrize("bad, error, match", [
    ({"species": []}, ValueError, "zero rows"),
    ({"species": ["100\tGene\t9606\thuman\tPubTator3"]}, ValueError, "Type other than 'Species'"),
    ({"species": ["PMID\tSpecies\t9606\thuman\tPubTator3"]}, ValueError, "non-numeric PMID"),
    # past a limited load's rows, so only the whole-file check sees it
    ({"species": [*DUMP["species"] * 20000, "100\tSpecies\t9606\thuman"]},
     ValueError, "line 20001 has 4 fields"),
    ({"species": [*DUMP["species"] * 20000, "100\tSpecies\t9606\thuman\tPubTator3\tx"]},
     ValueError, "line 20001 has 6 fields"),
    # DuckDB's read skips a blank line and would renumber every line after it
    ({"species": [*DUMP["species"] * 3, "", *DUMP["species"]]}, ValueError, "line 4 has 1 fields"),
])
def test_a_bad_file_fails_before_any_file_lands(lake_settings, tmp_path, bad, error, match):
    pubtator3.ingest(directory=str(dump(tmp_path / "d1", DUMP)), version="2026-09-19",
                     settings=lake_settings)
    con = lake_connect(lake_settings)
    try:
        before = con.execute(SNAP).fetchone()[0]
    finally:
        con.close()
    # species is checked after gene, disease and chemical, which would land first
    nxt = dump(tmp_path / "d2", {**DUMP, "gene": DUMP["gene"][:1], **bad})
    with pytest.raises(error, match=match):
        pubtator3.ingest(directory=str(nxt), version="2026-10-06", limit=1,
                         settings=lake_settings)
    con = lake_connect(lake_settings)
    try:
        assert con.execute(SNAP).fetchone()[0] == before
        assert len(rows(con, "gene")) == 4
    finally:
        con.close()


def test_a_record_replaced_by_a_blank_line_fails_a_full_merge(lake_settings, tmp_path):
    d = dump(tmp_path / "d1", DUMP)
    pubtator3.ingest(directory=str(d), kind="gene", version="2026-09-19", settings=lake_settings)
    con = lake_connect(lake_settings)
    try:
        before, landed = con.execute(SNAP).fetchone()[0], rows(con, "gene")
    finally:
        con.close()
    blank = dump(tmp_path / "d2", {"gene": [*DUMP["gene"][:2], "", DUMP["gene"][3]]})
    with pytest.raises(ValueError, match="line 3 has 1 fields"):
        pubtator3.ingest(directory=str(blank), kind="gene", version="2026-10-06",
                         settings=lake_settings)
    con = lake_connect(lake_settings)
    try:
        assert con.execute(SNAP).fetchone()[0] == before
        assert rows(con, "gene") == landed
    finally:
        con.close()


def test_arguments_are_checked(lake_settings, tmp_path):
    d = str(dump(tmp_path / "d1", DUMP))
    for kwargs, match in (({"directory": d}, "pass version"),
                          ({"directory": d, "version": "latest"}, "not a date"),
                          ({"directory": d, "version": "2026-09-19", "kind": "cellline"},
                           "kind must be one of"),
                          ({"directory": d, "version": "2026-09-19", "limit": 0},
                           "limit must be positive")):
        with pytest.raises(ValueError, match=match):
            pubtator3.ingest(settings=lake_settings, **kwargs)
    # land() is exported: called directly, limit=0 must not read as a full pruning load
    con = lake_connect(lake_settings)
    try:
        with pytest.raises(ValueError, match="limit must be positive"):
            pubtator3.land(con, "gene", Path(d) / "gene2pubtator3.gz", "2026-09-19", limit=0)
    finally:
        con.close()


def test_the_default_run_downloads_the_five_dumps(lake_settings, tmp_path, monkeypatch):
    up = dump(tmp_path / "up", DUMP)
    fetched = []

    def download(url, dest):
        fetched.append((url, dest))
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes((up / dest.name).read_bytes())
        return dest

    monkeypatch.setattr(INGEST, "download", download)
    result = pubtator3.ingest(version="2026-10-06", settings=lake_settings)
    assert result["rows"] == 8
    assert [u for u, _ in fetched] == [
        f"https://ftp.ncbi.nlm.nih.gov/pub/lu/PubTator3/{k}2pubtator3.gz" for k in pubtator3.KINDS]
    assert {(p.parent.name, p.parent.parent.name) for _, p in fetched} == {
        ("2026-10-06", "pubtator3")}
