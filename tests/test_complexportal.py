"""Offline tests for the Complex Portal source (bioc-on-ice#183 step 7).

The fixture is written at test time in the real files' dialect: the header
verbatim, '"' as cell content, '-' for missing, one file per species plus the
predicted one, under a dated directory, which is where upstream puts the version.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from cdsci.lake import Settings, lake_connect
from cdsci.lake.sources import complexportal


def line(**cells: str) -> str:
    return "\t".join(cells.get(c, "-") for c in complexportal.COLUMNS.values())


TP53 = line(complex_ac="CPX-663", recommended_name="TP53-MDM4 complex", taxonomy_identifier="9606",
            participants="O15151(0)|P04637(0)", source='psi-mi:"MI:2228"(ceitec)')
PREDICTED = line(complex_ac="CPX-9000", recommended_name="HuMAP complex 1",
                 taxonomy_identifier="9606", participants="A0AVF1(0)|Q9Y366(0)")
YEAST = line(complex_ac="CPX-25", recommended_name="Yeast complex", taxonomy_identifier="559292",
             participants="P38011(1)")


def release(root: Path, version: str, files: dict[str, list[str]], header=None) -> Path:
    d = root / version / "complextab"
    d.mkdir(parents=True)
    for name, rows in files.items():
        (d / name).write_text("\n".join(["\t".join(header or complexportal.COLUMNS), *rows]) + "\n")
    return d


@pytest.fixture
def lake_settings(tmp_path: Path) -> Settings:
    return Settings(storage_base_uri=f"file://{tmp_path}")


def test_complexportal_lands_every_file_and_a_new_release_replaces_the_last(
        lake_settings, tmp_path):
    d = release(tmp_path, "2026-01-09",
                {"9606.tsv": [TP53], "9606_predicted.tsv": [PREDICTED], "559292.tsv": [YEAST]})
    summary = complexportal.ingest(directory=str(d), settings=lake_settings)
    assert (summary["version"], summary["rows"]) == ("2026-01-09", 3)

    con = lake_connect(lake_settings, read_only=True)
    try:
        rows = {r[0]: r[1:] for r in con.execute("""
            SELECT complex_ac, file, source, ligand, taxonomy_identifier, complexportal_version
            FROM lake.complexportal.complex""").fetchall()}
    finally:
        con.close()
    # '"' is content, '-' is NULL, the file name is kept (it alone says 'predicted')
    assert rows["CPX-663"] == ("9606.tsv", 'psi-mi:"MI:2228"(ceitec)', None, "9606", "2026-01-09")
    assert rows["CPX-9000"][0] == "9606_predicted.tsv"

    # the next release withdraws the predicted complex: pruned, and every row restamped
    later = release(tmp_path, "2026-10-17", {"9606.tsv": [TP53], "559292.tsv": [YEAST]})
    assert complexportal.ingest(directory=str(later), settings=lake_settings)["rows"] == 2
    con = lake_connect(lake_settings, read_only=True)
    try:
        assert con.execute("SELECT DISTINCT complexportal_version FROM lake.complexportal.complex"
                           ).fetchall() == [("2026-10-17",)]
    finally:
        con.close()


def test_a_changed_header_or_an_undated_directory_fails(lake_settings, tmp_path):
    bad = release(tmp_path, "2026-05-01", {"9606.tsv": [TP53 + "\tx"]},
                  [*complexportal.COLUMNS, "New column"])
    con = lake_connect(lake_settings)
    try:
        with pytest.raises(ValueError, match="New column"):
            complexportal.curate(con, sorted(bad.glob("*.tsv")), "2026-05-01")
    finally:
        con.close()
    undated = release(tmp_path, "current", {"9606.tsv": [TP53]})
    with pytest.raises(ValueError, match="pass version"):
        complexportal.ingest(directory=str(undated), settings=lake_settings)
