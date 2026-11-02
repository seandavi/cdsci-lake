"""Offline tests for the MANE source (bioc-on-ice#183).

The fixture is written at test time in the real file's dialect: gzipped, the
header verbatim (starting with '#'), tab-separated, no quoting ("'" and '"' are
data), an empty cell for missing, '-' as the minus strand.
"""

from __future__ import annotations

import gzip
import importlib
import shutil
from pathlib import Path

import pytest

from cdsci.lake import Settings, lake_connect
from cdsci.lake.sources import mane

# the module itself: the package's `ingest` attribute is the function
INGEST = importlib.import_module("cdsci.lake.sources.mane.ingest")

SNAP = "SELECT max(snapshot_id) FROM lake.snapshots()"
ROWS = ("SELECT refseq_nuc, ensembl_nuc, mane_version FROM lake.mane.transcript "
        "ORDER BY refseq_nuc")
NAMES = [c.lstrip("#").lower() for c in mane.COLUMNS]


def line(**cells: str) -> str:
    return "\t".join(cells.get(c, "") for c in NAMES)


TP53 = line(ncbi_geneid="GeneID:7157", ensembl_gene="ENSG00000141510.21", hgnc_id="HGNC:11998",
            symbol="TP53", name="tumor protein p53", refseq_nuc="NM_000546.6",
            refseq_prot="NP_000537.3", ensembl_nuc="ENST00000269305.9",
            ensembl_prot="ENSP00000269305.4", mane_status="MANE Select",
            grch38_chr="NC_000017.11", chr_start="7661779", chr_end="7687546", chr_strand="-")
# a non-coding pair (no proteins) of a gene HGNC has not named; "'" and '"' are data
NR = line(ncbi_geneid="GeneID:100", ensembl_gene="ENSG00000000100.1", symbol="LOC100",
          name="5'-nucleotidase \"like\" #1", refseq_nuc="NR_000100.1",
          ensembl_nuc="ENST00000000100.2", mane_status="MANE Plus Clinical",
          grch38_chr="NC_000001.11", chr_start="10", chr_end="20", chr_strand="+")
BRCA1 = line(ncbi_geneid="GeneID:672", ensembl_gene="ENSG00000012048.27", hgnc_id="HGNC:1100",
             symbol="BRCA1", refseq_nuc="NM_007294.4", refseq_prot="NP_009225.1",
             ensembl_nuc="ENST00000357654.9", ensembl_prot="ENSP00000350283.3",
             mane_status="MANE Select", chr_strand="-")


def summary(path: Path, rows: list[str], header: tuple[str, ...] = mane.COLUMNS) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wt") as f:
        f.write("".join(r + "\n" for r in ["\t".join(header), *rows]))
    return path


@pytest.fixture
def lake_settings(tmp_path: Path) -> Settings:
    # lake_backend pinned: CU_OPENALEX_LAKE_BACKEND=postgres in the environment or a
    # .env would otherwise point these writes at the shared lake.
    return Settings(storage_base_uri=f"file://{tmp_path}", lake_backend="local",
                    lake_catalog=str(tmp_path / "catalog.ducklake"))


def test_mane_lands_whole_and_a_new_release_replaces_the_last(lake_settings, tmp_path):
    gz = summary(tmp_path / "MANE.GRCh38.v1.4.summary.txt.gz", [TP53, NR, BRCA1])
    result = mane.ingest(file=str(gz), settings=lake_settings)
    assert (result["version"], result["rows"]) == ("1.4", 3)

    con = lake_connect(lake_settings)
    try:
        # '-' is the strand, the empty cell is NULL, quotes and '#' are data
        assert con.execute("""
            SELECT ncbi_geneid, hgnc_id, name, refseq_prot, ensembl_prot, chr_strand
            FROM lake.mane.transcript WHERE refseq_nuc = 'NR_000100.1'""").fetchone() == (
            "GeneID:100", None, "5'-nucleotidase \"like\" #1", None, None, "+")
        assert con.execute("SELECT chr_strand, chr_start FROM lake.mane.transcript "
                           "WHERE symbol = 'TP53'").fetchone() == ("-", "7661779")
        # raw's columns, names and types, without landed_in
        assert [(c[0], c[1]) for c in con.execute("DESCRIBE lake.mane.transcript").fetchall()] == [
            *[(c, "VARCHAR") for c in NAMES], ("mane_version", "VARCHAR")]

        # re-landing the same release writes nothing
        before = con.execute(SNAP).fetchone()[0]
        mane.curate(con, gz, "1.4")
        assert con.execute(SNAP).fetchone()[0] == before

        # a limited load never prunes (and returns the table's rows, not those written);
        # a full load of the next release drops a pair and restamps every row
        nxt = summary(tmp_path / "MANE.GRCh38.v1.5.summary.txt.gz", [TP53, BRCA1])
        assert mane.curate(con, nxt, "1.5", limit=1) == 3
        assert mane.curate(con, nxt, "1.5") == 2
        assert con.execute(ROWS).fetchall() == [
            ("NM_000546.6", "ENST00000269305.9", "1.5"),
            ("NM_007294.4", "ENST00000357654.9", "1.5")]
    finally:
        con.close()


def test_a_malformed_file_fails_before_landing_and_keeps_the_table(lake_settings, tmp_path):
    con = lake_connect(lake_settings)
    try:
        mane.curate(con, summary(tmp_path / "seed.txt.gz", [TP53, NR]), "1.4")
        before = con.execute(SNAP).fetchone()[0]

        # a line with a field too many or too few, past a limited load's rows
        ragged = summary(tmp_path / "ragged.txt.gz", [TP53, NR, BRCA1 + "\textra"])
        with pytest.raises(ValueError, match="data line 3 has 15 fields, not 14"):
            mane.curate(con, ragged, "1.5", limit=1)
        short = summary(tmp_path / "short.txt.gz", [TP53, "GeneID:672\tENSG00000012048.27"])
        with pytest.raises(ValueError, match="data line 2 has 2 fields"):
            mane.curate(con, short, "1.5")

        # a renamed column
        renamed = summary(tmp_path / "renamed.txt.gz", [TP53],
                          (*mane.COLUMNS[:-1], "strand"))
        with pytest.raises(ValueError, match="strand"):
            mane.curate(con, renamed, "1.5", limit=1)

        assert con.execute(SNAP).fetchone()[0] == before
        assert con.execute(ROWS).fetchall() == [
            ("NM_000546.6", "ENST00000269305.9", "1.4"),
            ("NR_000100.1", "ENST00000000100.2", "1.4")]
    finally:
        con.close()


def test_a_header_only_file_is_refused_and_the_table_kept(lake_settings, tmp_path):
    con = lake_connect(lake_settings)
    try:
        mane.curate(con, summary(tmp_path / "seed.txt.gz", [TP53]), "1.4")
        before = con.execute(SNAP).fetchone()[0]
        empty = summary(tmp_path / "empty.txt.gz", [])
        for limit in (None, 1):
            with pytest.raises(ValueError, match="zero rows"):
                mane.curate(con, empty, "1.5", limit=limit)
        assert con.execute(SNAP).fetchone()[0] == before
        assert con.execute("SELECT count(*) FROM lake.mane.transcript").fetchone() == (1,)
    finally:
        con.close()


def test_a_file_that_names_no_release_needs_a_version(lake_settings, tmp_path):
    gz = summary(tmp_path / "summary.txt.gz", [TP53])
    with pytest.raises(ValueError, match="pass version"):
        mane.ingest(file=str(gz), settings=lake_settings)
    result = mane.ingest(file=str(gz), version="1.5", settings=lake_settings)
    assert (result["version"], result["rows"]) == ("1.5", 1)


def test_the_default_run_downloads_current_from_its_release_directory(
        lake_settings, tmp_path, monkeypatch):
    """No file, no version: read the release off current/, fetch it from release_X.Y/."""
    upstream = summary(tmp_path / "upstream" / "MANE.GRCh38.v1.5.summary.txt.gz", [TP53, NR])
    base = lake_settings.mane_base_url
    monkeypatch.setattr(INGEST, "_index", lambda url: {
        f"{base}current/": '<a href="MANE.GRCh38.v1.5.ensembl_genomic.gff.gz">x</a>'
                           '<a href="MANE.GRCh38.v1.5.summary.txt.gz">x</a>'}[url])
    fetched = []

    def download(url, dest):
        fetched.append(url.removeprefix(base))
        dest.parent.mkdir(parents=True, exist_ok=True)
        return Path(shutil.copy(upstream, dest))

    monkeypatch.setattr(INGEST, "download", download)
    result = mane.ingest(settings=lake_settings)
    assert (result["version"], result["rows"]) == ("1.5", 2)
    # an explicit version is fetched from its own directory, current/ unread
    mane.ingest(version="1.4", settings=lake_settings)
    assert fetched == ["release_1.5/MANE.GRCh38.v1.5.summary.txt.gz",
                       "release_1.4/MANE.GRCh38.v1.4.summary.txt.gz"]
