"""Offline tests for the WikiPathways source (bioc-on-ice#183 step 9).

The fixture is written at test time in the real files' dialect: ragged
tab-separated lines, no header, no quoting, the four-field packed set name, one
file per species dated by name.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from cdsci.lake import Settings, lake_connect
from cdsci.lake.sources import wikipathways

SNAP = "SELECT max(snapshot_id) FROM lake.snapshots()"
ROWS = "SELECT file_name, line_number, genes, wikipathways_version FROM lake.wikipathways.gene_set"


def gmt(date: str, wp: str, title: str, species: str, *genes: str) -> str:
    return "\t".join([f"{title}%WikiPathways_{date}%{wp}%{species}",
                      f"https://www.wikipathways.org/instance/{wp}", *genes])


def release(root: Path, date: str, files: dict[str, list[str]]) -> Path:
    d = root / date
    d.mkdir()
    for species, lines in files.items():
        (d / f"wikipathways-{date}-gmt-{species}.gmt").write_text(
            "".join(line + "\n" for line in lines))
    return d


def september(date: str = "20260910") -> dict[str, list[str]]:
    return {
        "Homo_sapiens": [
            # 2678 is drawn twice in the real WP100, and listed twice
            gmt(date, "WP100", "Glutathione metabolism", "Homo sapiens", "2687", "2678", "2678"),
            gmt(date, "WP106", "Alanine and aspartate metabolism", "Homo sapiens", "2806", "435"),
            # a set with no genes keeps genes NULL
            gmt(date, "WP999", "Empty set", "Homo sapiens"),
        ],
        "Zea_mays": [gmt(date, "WP5421", "Anthocyanins in purple corn", "Zea mays", "542166")],
    }


@pytest.fixture
def lake_settings(tmp_path: Path) -> Settings:
    # lake_backend pinned: CU_OPENALEX_LAKE_BACKEND=postgres in the environment or a
    # .env would otherwise point these writes at the shared lake.
    return Settings(storage_base_uri=f"file://{tmp_path}", lake_backend="local",
                    lake_catalog=str(tmp_path / "catalog.ducklake"))


def test_wikipathways_lands_every_line_verbatim_and_a_new_release_replaces_the_last(
        lake_settings, tmp_path):
    d = release(tmp_path, "20260910", september())
    summary = wikipathways.ingest(directory=str(d), settings=lake_settings)
    assert (summary["version"], summary["rows"]) == ("20260910", 4)

    con = lake_connect(lake_settings)
    try:
        row = con.execute("""
            SELECT name, description, genes, species, line_number
            FROM lake.wikipathways.gene_set WHERE name LIKE '%WP100%'""").fetchone()
        # the packed name is unparsed; the genes are the rest of the line, repeats and all
        assert row == ("Glutathione metabolism%WikiPathways_20260910%WP100%Homo sapiens",
                       "https://www.wikipathways.org/instance/WP100", "2687\t2678\t2678",
                       "Homo_sapiens", 1)
        assert con.execute("SELECT genes FROM lake.wikipathways.gene_set WHERE name LIKE "
                           "'%WP999%'").fetchone() == (None,)
        cols = con.execute("DESCRIBE lake.wikipathways.gene_set").fetchall()
        assert [(c[0], c[1]) for c in cols] == [
            ("file_name", "VARCHAR"), ("line_number", "INTEGER"), ("name", "VARCHAR"),
            ("description", "VARCHAR"), ("genes", "VARCHAR"), ("species", "VARCHAR"),
            ("wikipathways_version", "VARCHAR")]

        # re-landing the same release writes nothing
        before = con.execute(SNAP).fetchone()[0]
        wikipathways.curate(con, sorted(d.glob("*.gmt")), "20260910")
        assert con.execute(SNAP).fetchone()[0] == before

        # a limited load never prunes; a full load of the next release replaces every row
        later = release(tmp_path, "20261010",
                        {"Homo_sapiens": september("20261010")["Homo_sapiens"][:2]})
        paths = sorted(later.glob("*.gmt"))
        # curate returns the table's row count: 4 kept + 1 added
        assert wikipathways.curate(con, paths, "20261010", limit=1) == 5
        assert wikipathways.curate(con, paths, "20261010") == 2
        assert sorted(con.execute(ROWS).fetchall()) == [
            ("wikipathways-20261010-gmt-Homo_sapiens.gmt", 1, "2687\t2678\t2678", "20261010"),
            ("wikipathways-20261010-gmt-Homo_sapiens.gmt", 2, "2806\t435", "20261010")]
    finally:
        con.close()


def test_files_that_are_not_one_release_fail_before_landing(lake_settings, tmp_path):
    d = release(tmp_path, "20260910", september())
    (d / "wikipathways-20261010-gmt-Mus_musculus.gmt").write_text(
        gmt("20261010", "WP1", "x", "Mus musculus", "1") + "\n")
    with pytest.raises(ValueError, match="exactly one release"):
        wikipathways.ingest(directory=str(d), settings=lake_settings)
    con = lake_connect(lake_settings)
    try:
        with pytest.raises(ValueError, match="not release 20261010"):
            wikipathways.curate(con, sorted(d.glob("*Homo*.gmt")), "20261010")
        with pytest.raises(ValueError, match="exactly one release"):
            wikipathways.curate(con, [d / "notes.gmt"], "20260910")
    finally:
        con.close()


def test_an_empty_release_is_refused_and_the_table_kept(lake_settings, tmp_path):
    con = lake_connect(lake_settings)
    try:
        good = release(tmp_path, "20260910", september())
        wikipathways.curate(con, sorted(good.glob("*.gmt")), "20260910")
        before = con.execute(SNAP).fetchone()[0]
        empty = release(tmp_path, "20261010", {"Homo_sapiens": []})
        with pytest.raises(ValueError, match="zero rows"):
            wikipathways.curate(con, sorted(empty.glob("*.gmt")), "20261010")
        assert con.execute("SELECT count(*) FROM lake.wikipathways.gene_set").fetchone()[0] == 4
        assert con.execute(SNAP).fetchone()[0] == before
    finally:
        con.close()


def test_a_line_that_is_not_a_gmt_record_fails_and_keeps_the_table(lake_settings, tmp_path):
    con = lake_connect(lake_settings)
    try:
        good = release(tmp_path, "20260910", september())
        wikipathways.curate(con, sorted(good.glob("*.gmt")), "20260910")
        before = con.execute(SNAP).fetchone()[0]
        # a truncated or non-GMT body: lines with no tab at all
        bad = release(tmp_path, "20261010", {"Homo_sapiens": [
            gmt("20261010", "WP100", "Glutathione metabolism", "Homo sapiens", "2687"),
            "<html>not found</html>"]})
        with pytest.raises(ValueError, match="line 2 is not a GMT record"):
            wikipathways.curate(con, sorted(bad.glob("*.gmt")), "20261010", limit=1)
        assert con.execute("SELECT count(*) FROM lake.wikipathways.gene_set").fetchone()[0] == 4
        assert con.execute(SNAP).fetchone()[0] == before
    finally:
        con.close()
