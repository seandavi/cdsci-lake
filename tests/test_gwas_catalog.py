"""Offline tests for the GWAS Catalog source (bioc-on-ice#183).

The fixture is written at test time in the real files' dialect: the headers
verbatim, tab-separated, no quoting ('"' is data, unbalanced too), an empty cell
for missing, 'NA'/'NR' as data, whole-row duplicates kept, the associations
zipped, both files under a dated ``YYYY/MM/DD`` release directory.
"""

from __future__ import annotations

import importlib
import zipfile
from pathlib import Path

import pytest

from cdsci.lake import Settings, lake_connect
from cdsci.lake.sources import gwas_catalog
from cdsci.lake.sources.gwas_catalog.ingest import ASSOCIATIONS_ZIP, STUDIES_FILE

SNAP = "SELECT max(snapshot_id) FROM lake.snapshots()"
ASSOC_ROWS = ("SELECT line_number, snps, gwas_catalog_release FROM lake.gwas_catalog.association "
              "ORDER BY line_number")


def line(columns: dict[str, str], **cells: str) -> str:
    return "\t".join(cells.get(c, "") for c in columns.values())


def assoc(**cells: str) -> str:
    return line(gwas_catalog.ASSOCIATIONS, **cells)


def study(**cells: str) -> str:
    return line(gwas_catalog.STUDIES, **cells)


# '"' opens a phrase that never closes; '#' opens a cell; 'NR' is data, not missing
QUOTED = assoc(study_accession="GCST000001", snps="rs1", study='The "lost" gene study',
               strongest_snp_risk_allele='rs1-"A', p_value="1E-396", risk_allele_frequency="NR",
               reported_genes="#N/A", or_beta="1.2")
# one study reports a variant twice, byte for byte: both lines land
TWICE = assoc(study_accession="GCST000002", snps="rs2 x rs3", p_value="2E-8")
OTHER = assoc(study_accession="GCST000002", snps="rs4", p_value="5E-9", context="NA")
STUDY1 = study(study_accession="GCST000001", study='The "lost" gene study',
               association_count="1", summary_stats_location="NA", gxe="no")
STUDY2 = study(study_accession="GCST000002", association_count="3")


def tsv(path: Path, columns: dict[str, str], rows: list[str], header=None) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(r + "\n" for r in ["\t".join(header or columns), *rows]))
    return path


def release(root: Path, version: str, assocs: list[str], studies: list[str]) -> Path:
    d = root.joinpath(*version.split("-"))
    inner = tsv(root / "gwas-catalog-download-associations-alt-full.tsv",
                gwas_catalog.ASSOCIATIONS, assocs)
    d.mkdir(parents=True)
    with zipfile.ZipFile(d / ASSOCIATIONS_ZIP, "w") as z:
        z.write(inner, inner.name)
    inner.unlink()
    tsv(d / STUDIES_FILE, gwas_catalog.STUDIES, studies)
    return d


@pytest.fixture
def lake_settings(tmp_path: Path) -> Settings:
    # lake_backend pinned: CU_OPENALEX_LAKE_BACKEND=postgres in the environment or a
    # .env would otherwise point these writes at the shared lake.
    return Settings(storage_base_uri=f"file://{tmp_path}", lake_backend="local",
                    lake_catalog=str(tmp_path / "catalog.ducklake"))


def test_gwas_catalog_lands_both_files_verbatim_and_a_new_release_replaces_the_last(
        lake_settings, tmp_path):
    d = release(tmp_path / "releases", "2026-09-15", [QUOTED, TWICE, TWICE, OTHER],
                [STUDY1, STUDY2])
    summary = gwas_catalog.ingest(directory=str(d), settings=lake_settings)
    assert (summary["version"], summary["association"], summary["study"]) == ("2026-09-15", 4, 2)

    con = lake_connect(lake_settings)
    try:
        assert con.execute("""
            SELECT study, strongest_snp_risk_allele, p_value, risk_allele_frequency,
                   reported_genes, or_beta, mapped_gene
            FROM lake.gwas_catalog.association WHERE line_number = 1""").fetchone() == (
            'The "lost" gene study', 'rs1-"A', "1E-396", "NR", "#N/A", "1.2", None)
        assert con.execute(ASSOC_ROWS).fetchall() == [
            (1, "rs1", "2026-09-15"), (2, "rs2 x rs3", "2026-09-15"),
            (3, "rs2 x rs3", "2026-09-15"), (4, "rs4", "2026-09-15")]
        assert con.execute("SELECT study, summary_stats_location, gxe, cohort FROM "
                           "lake.gwas_catalog.study WHERE study_accession = 'GCST000001'"
                           ).fetchone() == ('The "lost" gene study', "NA", "no", None)
        # raw's columns, names and types, plus the line number that keys an association
        assert [(c[0], c[1]) for c in con.execute(
            "DESCRIBE lake.gwas_catalog.association").fetchall()] == [
            ("line_number", "INTEGER"),
            *[(c, "VARCHAR") for c in gwas_catalog.ASSOCIATIONS.values()],
            ("gwas_catalog_release", "VARCHAR")]
        assert [(c[0], c[1]) for c in con.execute(
            "DESCRIBE lake.gwas_catalog.study").fetchall()] == [
            *[(c, "VARCHAR") for c in gwas_catalog.STUDIES.values()],
            ("gwas_catalog_release", "VARCHAR")]

        # re-landing the same release writes nothing
        before = con.execute(SNAP).fetchone()[0]
        tsv_path = next((tmp_path / "lake" / "raw" / "gwas_catalog" / "2026-09-15").glob("*.tsv"))
        gwas_catalog.curate(con, tsv_path, d / STUDIES_FILE, "2026-09-15")
        assert con.execute(SNAP).fetchone()[0] == before

        # a limited load never prunes; a full load of the next release drops what it
        # no longer has and restamps every row, so each table is one release
        nxt = tmp_path / "next"
        a, s = (tsv(nxt / "a.tsv", gwas_catalog.ASSOCIATIONS, [QUOTED, OTHER]),
                tsv(nxt / "s.txt", gwas_catalog.STUDIES, [STUDY2]))
        # curate returns each table's row count, not the rows written
        assert gwas_catalog.curate(con, a, s, "2026-10-05", limit=1) == {
            "study": 2, "association": 4}
        assert gwas_catalog.curate(con, a, s, "2026-10-05") == {"study": 1, "association": 2}
        assert con.execute(ASSOC_ROWS).fetchall() == [
            (1, "rs1", "2026-10-05"), (2, "rs4", "2026-10-05")]
        assert con.execute("SELECT study_accession, gwas_catalog_release FROM "
                           "lake.gwas_catalog.study").fetchall() == [("GCST000002", "2026-10-05")]
    finally:
        con.close()


def test_a_malformed_file_fails_before_landing_and_keeps_the_tables(lake_settings, tmp_path):
    con = lake_connect(lake_settings)
    try:
        good = (tsv(tmp_path / "a.tsv", gwas_catalog.ASSOCIATIONS, [QUOTED, TWICE]),
                tsv(tmp_path / "s.txt", gwas_catalog.STUDIES, [STUDY1, STUDY2]))
        gwas_catalog.curate(con, *good, "2026-09-15")
        before = con.execute(SNAP).fetchone()[0]

        # a line with a field too many, past a limited load's rows, in either file
        ragged = tsv(tmp_path / "ragged.tsv", gwas_catalog.ASSOCIATIONS,
                     [QUOTED, TWICE, OTHER + "\textra"])
        with pytest.raises(ValueError, match="data line 3 has 39 fields, not 38"):
            gwas_catalog.curate(con, ragged, good[1], "2026-10-05", limit=1)
        short = tsv(tmp_path / "short.txt", gwas_catalog.STUDIES, [STUDY1, "GCST000002"])
        with pytest.raises(ValueError, match="data line 2 has 1 fields"):
            gwas_catalog.curate(con, good[0], short, "2026-10-05")

        # a renamed column (the Catalog versions its file names because it does this)
        renamed = tsv(tmp_path / "renamed.txt", gwas_catalog.STUDIES, [STUDY1],
                      [*gwas_catalog.STUDIES][:-1] + ["GXE INTERACTION"])
        with pytest.raises(ValueError, match="GXE INTERACTION"):
            gwas_catalog.curate(con, good[0], renamed, "2026-10-05")

        assert con.execute(SNAP).fetchone()[0] == before
        assert con.execute(ASSOC_ROWS).fetchall() == [
            (1, "rs1", "2026-09-15"), (2, "rs2 x rs3", "2026-09-15")]
    finally:
        con.close()


def test_a_header_only_file_is_refused_and_the_tables_kept(lake_settings, tmp_path):
    con = lake_connect(lake_settings)
    try:
        a = tsv(tmp_path / "a.tsv", gwas_catalog.ASSOCIATIONS, [QUOTED])
        s = tsv(tmp_path / "s.txt", gwas_catalog.STUDIES, [STUDY1])
        gwas_catalog.curate(con, a, s, "2026-09-15")
        before = con.execute(SNAP).fetchone()[0]
        empty = tsv(tmp_path / "empty.tsv", gwas_catalog.ASSOCIATIONS, [])
        for limit in (None, 1):
            with pytest.raises(ValueError, match="zero rows"):
                gwas_catalog.curate(con, empty, s, "2026-10-05", limit=limit)
        assert con.execute(SNAP).fetchone()[0] == before
        assert con.execute("SELECT count(*) FROM lake.gwas_catalog.association").fetchone() == (1,)
    finally:
        con.close()


def test_an_undated_directory_needs_a_version(lake_settings, tmp_path):
    d = release(tmp_path / "latest", "2026-09-15", [QUOTED], [STUDY1])
    undated = d.rename(tmp_path / "copy")
    with pytest.raises(ValueError, match="pass version"):
        gwas_catalog.ingest(directory=str(undated), settings=lake_settings)
    summary = gwas_catalog.ingest(directory=str(undated), version="2026-09-15",
                                  settings=lake_settings)
    assert (summary["version"], summary["rows"]) == ("2026-09-15", 2)


def test_latest_walks_to_the_newest_dated_directory(monkeypatch):
    index = {"": ["2025", "2026"], "2026/": ["09", "10"], "2026/10/": ["05", "22"]}
    base = Settings().gwas_catalog_releases_url
    # the package's `ingest` attribute is the function, so patch the module itself
    monkeypatch.setattr(importlib.import_module("cdsci.lake.sources.gwas_catalog.ingest"),
                        "_dated", lambda url: index[url.removeprefix(base)])
    assert gwas_catalog.latest() == "2026-10-22"
