"""Offline tests for the BioGRID source (bioc-on-ice#183).

The fixture is written at test time in the real file's dialect (probed on
5.0.261, 2026-11-03): a zip holding one ``BIOGRID-ALL-<v>.tab3.txt``, the
header verbatim (starting with '#'), tab-separated, LF, no quoting ('"' is
data), '-' for missing, lists '|'-joined.
"""

from __future__ import annotations

import importlib
import shutil
import zipfile
from pathlib import Path

import duckdb
import pytest

from cdsci.lake import Settings, lake_connect
from cdsci.lake.sources import biogrid

# the module itself: the package's `ingest` attribute is the function
INGEST = importlib.import_module("cdsci.lake.sources.biogrid.ingest")

SNAP = "SELECT max(snapshot_id) FROM lake.snapshots()"
ROWS = ("SELECT biogrid_interaction_id, biogrid_version FROM lake.biogrid.interaction "
        "ORDER BY biogrid_interaction_id")
NAMES = list(biogrid.COLUMNS.values())


def line(**cells: str) -> str:
    return "\t".join(cells.get(c, "-") for c in NAMES)


# the first row of 5.0.261, verbatim
R103 = ("103\t6416\t2318\t112315\t108607\t-\t-\tMAP2K4\tFLNC\tJNKK|JNKK1|MAPKK4|MEK4|MKK4|"
        "PRKMK4|SAPKK-1|SAPKK1|SEK1|SERK1|SKK1\tABP-280|ABP280A|ABPA|ABPL|FLN2|MFM5|MPD4\t"
        "Two-hybrid\tphysical\tMarti A (1997)\tPUBMED:9006895\t9606\t9606\tLow Throughput\t-\t"
        "-\t-\t-\tBIOGRID\tP45985\t-\tNP_003001|NP_001268364\tQ14315\tQ59H94\t"
        "NP_001120959|NP_001449\t-\t-\t-\t-\t-\t-\tHomo sapiens\tHomo sapiens")
# no Entrez gene for A, a preprint DOI, '"' and "'" as data, a hyphenated value
R200 = line(biogrid_interaction_id="200", biogrid_id_a="4383868", entrez_gene_b="855",
            biogrid_id_b="31000", official_symbol_b="YAL-1", experimental_system="Two-hybrid",
            publication_source="DOI:10.1101/2024.01.01.000001",
            qualifications='a "bait" swap; Smith\'s note', organism_id_a="559292",
            organism_id_b="559292", score="0.85")
R300 = line(biogrid_interaction_id="300", entrez_gene_a="7157", entrez_gene_b="4193",
            biogrid_id_a="113010", biogrid_id_b="110358", experimental_system_type="physical")


def tab3(root: Path, version: str, rows: list[str], header=None) -> Path:
    """A ``BIOGRID-ALL-<version>.tab3.zip`` holding the one text file."""
    root.mkdir(parents=True, exist_ok=True)
    path = root / f"BIOGRID-ALL-{version}.tab3.zip"
    text = "".join(r + "\n" for r in ["\t".join(header or biogrid.COLUMNS), *rows])
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr(f"BIOGRID-ALL-{version}.tab3.txt", text)
    return path


def text(tmp_path: Path, name: str, rows: list[str], header=None) -> Path:
    path = tmp_path / name
    path.write_text("".join(r + "\n" for r in ["\t".join(header or biogrid.COLUMNS), *rows]))
    return path


@pytest.fixture
def lake_settings(tmp_path: Path) -> Settings:
    # lake_backend pinned: CU_OPENALEX_LAKE_BACKEND=postgres in the environment or a
    # .env would otherwise point these writes at the shared lake.
    return Settings(storage_base_uri=f"file://{tmp_path}", lake_backend="local",
                    lake_catalog=str(tmp_path / "catalog.ducklake"))


def seed(con, tmp_path: Path) -> int:
    biogrid.curate(con, text(tmp_path, "seed.txt", [R103, R200]), "5.0.261")
    return con.execute(SNAP).fetchone()[0]


def test_biogrid_lands_whole_and_a_new_release_replaces_the_last(lake_settings, tmp_path):
    z = tab3(tmp_path / "in", "5.0.261", [R103, R200, R300])
    result = biogrid.ingest(file=str(z), settings=lake_settings)
    assert (result["version"], result["rows"]) == ("5.0.261", 3)

    con = lake_connect(lake_settings)
    try:
        # raw's columns, names and types, without landed_in
        assert [(c[0], c[1]) for c in con.execute(
            "DESCRIBE lake.biogrid.interaction").fetchall()] == [
            *[(c, "VARCHAR") for c in NAMES], ("biogrid_version", "VARCHAR")]
        # '-' is NULL, '|'-lists stay joined, quotes are data
        assert con.execute("""
            SELECT systematic_name_a, synonyms_b, score, refseq_b, organism_name_b
            FROM lake.biogrid.interaction WHERE biogrid_interaction_id = '103'""").fetchone() == (
            None, "ABP-280|ABP280A|ABPA|ABPL|FLN2|MFM5|MPD4", None, "NP_001120959|NP_001449",
            "Homo sapiens")
        assert con.execute("""
            SELECT entrez_gene_a, biogrid_id_a, official_symbol_b, publication_source,
                   qualifications, score
            FROM lake.biogrid.interaction WHERE biogrid_interaction_id = '200'""").fetchone() == (
            None, "4383868", "YAL-1", "DOI:10.1101/2024.01.01.000001",
            'a "bait" swap; Smith\'s note', "0.85")
        assert con.execute(ROWS).fetchall() == [(i, "5.0.261") for i in ("103", "200", "300")]

        # re-landing the same release writes nothing
        before = con.execute(SNAP).fetchone()[0]
        path = INGEST.extract(z, "5.0.261", lake_settings)
        assert biogrid.curate(con, path, "5.0.261") == 3
        assert con.execute(SNAP).fetchone()[0] == before

        # a limited load never prunes (and returns the table's row count, not the rows
        # written); a full load of the next release drops withdrawn ids and restamps
        nxt = text(tmp_path, "next.txt", [R103, R300])
        assert biogrid.curate(con, nxt, "5.0.262", limit=1) == 3
        assert biogrid.curate(con, nxt, "5.0.262") == 2
        assert con.execute(ROWS).fetchall() == [("103", "5.0.262"), ("300", "5.0.262")]
    finally:
        con.close()


def test_a_malformed_file_fails_before_landing_and_keeps_the_table(lake_settings, tmp_path):
    con = lake_connect(lake_settings)
    try:
        before = seed(con, tmp_path)
        cols = list(biogrid.COLUMNS)
        swapped = [cols[1], cols[0], *cols[2:]]
        for name, header, match in (
            ("added", [*cols, "New Column"], "New Column"),
            ("dropped", cols[:-1], "Organism Name Interactor B"),
            ("swapped", swapped, "column order"),
        ):
            bad = text(tmp_path, f"{name}.txt", [R103], header)
            with pytest.raises(ValueError, match=match):
                biogrid.curate(con, bad, "5.0.262", limit=1)
        # a row with an extra cell, and one short a cell, past a limited load's rows: 20,000
        # rows in, beyond what DuckDB's reader parses for LIMIT 1, so only the whole-file
        # check sees it
        padding = [line(biogrid_interaction_id=str(i)) for i in range(1000, 21000)]
        for name, row in (("extra", R300 + "\tstray"), ("short", R300.rsplit("\t", 1)[0])):
            bad = text(tmp_path, f"{name}.txt", [R103, *padding, row])
            for limit in (None, 1):
                with pytest.raises(duckdb.InvalidInputException,
                                   match="Expected Number of Columns"):
                    biogrid.curate(con, bad, "5.0.262", limit=limit)
        assert con.execute(SNAP).fetchone()[0] == before
        assert con.execute(ROWS).fetchall() == [("103", "5.0.261"), ("200", "5.0.261")]
    finally:
        con.close()


def test_a_header_only_file_is_refused_and_the_table_kept(lake_settings, tmp_path):
    con = lake_connect(lake_settings)
    try:
        before = seed(con, tmp_path)
        empty = text(tmp_path, "empty.txt", [])
        for limit in (None, 1):
            with pytest.raises(ValueError, match="zero rows"):
                biogrid.curate(con, empty, "5.0.262", limit=limit)
        for limit in (0, -1):
            with pytest.raises(ValueError, match="limit must be positive"):
                biogrid.curate(con, text(tmp_path, "one.txt", [R300]), "5.0.262", limit=limit)
        assert con.execute(SNAP).fetchone()[0] == before
        assert con.execute(ROWS).fetchall() == [("103", "5.0.261"), ("200", "5.0.261")]
    finally:
        con.close()


def test_a_zip_must_name_its_release_and_hold_one_file(lake_settings, tmp_path):
    z = tab3(tmp_path / "in", "5.0.261", [R103])
    undated = z.rename(tmp_path / "biogrid.zip")
    with pytest.raises(ValueError, match="pass version"):
        biogrid.ingest(file=str(undated), settings=lake_settings)
    with pytest.raises(ValueError, match="not a release number"):
        biogrid.ingest(file=str(undated), version="latest", settings=lake_settings)
    two = tmp_path / "BIOGRID-ALL-5.0.262.tab3.zip"
    with zipfile.ZipFile(two, "w") as zf:
        zf.writestr("a.txt", "x\n")
        zf.writestr("b.txt", "y\n")
    with pytest.raises(ValueError, match="should hold one text file"):
        biogrid.ingest(file=str(two), settings=lake_settings)


def test_the_default_run_downloads_the_newest_release(lake_settings, tmp_path, monkeypatch):
    upstream = tab3(tmp_path / "up", "5.0.263", [R103, R300])
    fetched = []

    def download(url, dest, *, headers):
        fetched.append((url, dest, headers))
        dest.parent.mkdir(parents=True, exist_ok=True)
        return Path(shutil.copy(upstream, dest))

    monkeypatch.setattr(INGEST, "download", download)
    monkeypatch.setattr(INGEST, "latest", lambda s: "5.0.263")
    result = biogrid.ingest(settings=lake_settings)
    assert (result["version"], result["rows"]) == ("5.0.263", 2)
    [(url, dest, headers)] = fetched
    assert url == ("https://downloads.thebiogrid.org/Download/BioGRID/Release-Archive/"
                   "BIOGRID-5.0.263/BIOGRID-ALL-5.0.263.tab3.zip")
    assert headers == INGEST.UA
    # keyed by version under the source's raw dir, unzipped beside the download
    assert dest.parent.name == "5.0.263" and dest.parent.parent.name == "biogrid"
    assert (dest.parent / "BIOGRID-ALL-5.0.263.tab3.txt").exists()
