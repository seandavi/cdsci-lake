"""Offline tests for the HGNC source (bioc-on-ice#183 step 7).

The fixture is written at test time in the real file's dialect: the header
verbatim, '|'-lists wrapped in double quotes, empty cells for missing.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from cdsci.lake import Settings, lake_connect
from cdsci.lake.sources import hgnc
from cdsci.lake.sources.hgnc.ingest import file_version


def line(**cells: str) -> str:
    return "\t".join(cells.get(c, "") for c in hgnc.COLUMNS)


TP53 = line(hgnc_id="HGNC:11998", symbol="TP53", name="tumor protein p53",
            status="Approved", alias_symbol='"p53|LFS1"', entrez_id="7157",
            **{"pseudogene.org": "PGOHUM1", "mamit-trnadb": "7"})
GSTT1 = line(hgnc_id="HGNC:4641", symbol="GSTT1", status="Approved", omim_id='"600436|613143"')
A1BG_AS1 = line(hgnc_id="HGNC:37133", symbol="A1BG-AS1", status="Approved")


def write(path: Path, rows: list[str], header: tuple[str, ...] = hgnc.COLUMNS) -> Path:
    path.write_text("\n".join(["\t".join(header), *rows]) + "\n")
    return path


@pytest.fixture
def lake_settings(tmp_path: Path) -> Settings:
    return Settings(storage_base_uri=f"file://{tmp_path}")


def test_hgnc_lands_whole_and_a_new_version_replaces_the_last(lake_settings, tmp_path):
    tsv = write(tmp_path / "hgnc_complete_set.txt", [TP53, GSTT1, A1BG_AS1])
    con = lake_connect(lake_settings)
    try:
        assert hgnc.curate(con, tsv, "2026-09-18") == 3
        row = con.execute("""
            SELECT symbol, alias_symbol, omim_id, pseudogene_org, mamit_trnadb, name, hgnc_version
            FROM lake.hgnc.gene WHERE hgnc_id = 'HGNC:11998'
        """).fetchone()
        # quotes are the dialect, the pipes are the value; '.'/'-' names land with '_'
        assert row == ("TP53", "p53|LFS1", None, "PGOHUM1", "7", "tumor protein p53", "2026-09-18")
        assert con.execute("SELECT omim_id FROM lake.hgnc.gene WHERE symbol = 'GSTT1'"
                           ).fetchone()[0] == "600436|613143"
        cols = [c[0] for c in con.execute("DESCRIBE lake.hgnc.gene").fetchall()]
        assert len(cols) == len(hgnc.COLUMNS) + 1

        # re-landing the same version writes nothing
        before = con.execute("SELECT max(snapshot_id) FROM lake.snapshots()").fetchone()[0]
        hgnc.curate(con, tsv, "2026-09-18")
        assert con.execute("SELECT max(snapshot_id) FROM lake.snapshots()").fetchone()[0] == before

        # a limited load never prunes; a full load of the next version drops a withdrawn
        # id and restamps every row, so the table is one version
        later = write(tmp_path / "later.txt", [TP53, GSTT1])
        assert hgnc.curate(con, later, "2026-10-30", limit=1) == 3
        assert hgnc.curate(con, later, "2026-10-30") == 2
        assert con.execute("SELECT DISTINCT hgnc_version FROM lake.hgnc.gene").fetchall() == [
            ("2026-10-30",)]
    finally:
        con.close()


def test_a_changed_header_fails_before_landing(lake_settings, tmp_path):
    bad = write(tmp_path / "bad.txt", [TP53 + "\tx"], (*hgnc.COLUMNS, "location_sortable"))
    con = lake_connect(lake_settings)
    try:
        with pytest.raises(ValueError, match="location_sortable"):
            hgnc.curate(con, bad, "2026-08-07")
    finally:
        con.close()


def test_a_header_only_file_is_refused_and_the_table_kept(lake_settings, tmp_path):
    con = lake_connect(lake_settings)
    snap = "SELECT max(snapshot_id) FROM lake.snapshots()"
    try:
        hgnc.curate(con, write(tmp_path / "seed.txt", [TP53, GSTT1]), "2026-09-18")
        before = con.execute(snap).fetchone()[0]
        with pytest.raises(ValueError, match="zero rows"):
            hgnc.curate(con, write(tmp_path / "empty.txt", []), "2026-10-30")
        assert con.execute("SELECT count(*) FROM lake.hgnc.gene").fetchone()[0] == 2
        assert con.execute(snap).fetchone()[0] == before
    finally:
        con.close()


def test_a_dated_archive_names_its_version(lake_settings, tmp_path):
    assert file_version(Path("hgnc_complete_set_2026-09-04.txt")) == "2026-09-04"
    assert file_version(Path("hgnc_complete_set.txt")) is None
    tsv = write(tmp_path / "hgnc_complete_set_2026-09-04.txt", [TP53])
    summary = hgnc.ingest(file=str(tsv), settings=lake_settings)
    assert (summary["version"], summary["rows"]) == ("2026-09-04", 1)
