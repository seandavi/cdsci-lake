"""Offline tests for the eQTL Catalogue source (bioc-on-ice#183).

The fixture is written at test time in the real files' dialect: the headers
verbatim, tab-separated, no quoting ('"' is data), 'NA' and the empty cell for
missing, LF, the metadata file with no final newline (as r7's has none), both
under the resources repo's ``data_tables/`` and ``tabix/``.
"""

from __future__ import annotations

import importlib
import shutil
from pathlib import Path

import pytest

from cdsci.lake import Settings, lake_connect
from cdsci.lake.sources import eqtl_catalogue
from cdsci.lake.sources.eqtl_catalogue import DATASET_COLUMNS, PATH_COLUMNS

# the module itself: the package's `ingest` attribute is the function
INGEST = importlib.import_module("cdsci.lake.sources.eqtl_catalogue.ingest")

SNAP = "SELECT max(snapshot_id) FROM lake.snapshots()"
DATASETS = ("SELECT dataset_id, eqtlcatalogue_release FROM lake.eqtl_catalogue.dataset "
            "ORDER BY dataset_id")


def line(columns: tuple[str, ...], **cells: str) -> str:
    return "\t".join(cells.get(c, "") for c in columns)


def ds(dataset_id: str, **cells: str) -> str:
    return line(DATASET_COLUMNS, study_id="QTS000001", dataset_id=dataset_id, **cells)


def path(dataset_id: str, **cells: str) -> str:
    ftp = "ftp://ftp.ebi.ac.uk/pub/databases/spot/eQTL"
    return line(PATH_COLUMNS, study_id="QTS000001", dataset_id=dataset_id,
                ftp_path=f"{ftp}/sumstats/QTS000001/{dataset_id}/{dataset_id}.all.tsv.gz",
                ftp_cs_path=f"{ftp}/susie/QTS000001/{dataset_id}/{dataset_id}.credible_sets.tsv.gz",
                ftp_lbf_path=f"{ftp}/susie/QTS000001/{dataset_id}/{dataset_id}.lbf_variable.txt.gz",
                **cells)


# 'NA' (R's missing marker) and the empty cell are NULL; '"' is an ordinary character
D1 = ds("QTD000001", study_label="Alasoo_2018", tissue_id="CL_0000235",
        tissue_label='macrophage "naive"', sample_size="84", quant_method="ge", pmid="NA",
        study_type="bulk")
D2 = ds("QTD000002", tissue_id="NA", sample_size="85", quant_method="exon")
D3 = ds("QTD000003", quant_method="tx")
P1, P2, P3 = path("QTD000001", sample_size="84"), path("QTD000002"), path("QTD000003")


def tsv(file: Path, columns: tuple[str, ...], rows: list[str], header=None, *,
        final_newline: bool = True) -> Path:
    file.parent.mkdir(parents=True, exist_ok=True)
    file.write_text("\n".join(["\t".join(header or columns), *rows])
                    + ("\n" if final_newline else ""))
    return file


def repo(root: Path, version: str, datasets: list[str], paths: list[str]) -> Path:
    """A copy of the resources repo holding one release's two files."""
    tsv(root / "data_tables" / f"dataset_metadata_r{version}.tsv", DATASET_COLUMNS, datasets,
        final_newline=False)
    tsv(root / "tabix" / "tabix_ftp_paths.tsv", PATH_COLUMNS, paths)
    return root


@pytest.fixture
def lake_settings(tmp_path: Path) -> Settings:
    # lake_backend pinned: CU_OPENALEX_LAKE_BACKEND=postgres in the environment or a
    # .env would otherwise point these writes at the shared lake.
    return Settings(storage_base_uri=f"file://{tmp_path}", lake_backend="local",
                    lake_catalog=str(tmp_path / "catalog.ducklake"))


def test_eqtl_catalogue_lands_both_files_verbatim_and_a_new_release_replaces_the_last(
        lake_settings, tmp_path):
    root = repo(tmp_path / "r7", "7", [D1, D2, D3], [P1, P2, P3])
    summary = eqtl_catalogue.ingest(directory=str(root), settings=lake_settings)
    assert (summary["version"], summary["dataset"], summary["tabix_ftp_path"]) == ("7", 3, 3)

    con = lake_connect(lake_settings)
    try:
        assert con.execute("""
            SELECT study_label, tissue_id, tissue_label, sample_size, pmid, study_type
            FROM lake.eqtl_catalogue.dataset WHERE dataset_id = 'QTD000001'""").fetchone() == (
            "Alasoo_2018", "CL_0000235", 'macrophage "naive"', "84", None, "bulk")
        # the last line of a file with no final newline lands, 'NA' reads as NULL
        assert con.execute("SELECT tissue_id, quant_method FROM lake.eqtl_catalogue.dataset "
                           "WHERE dataset_id = 'QTD000003'").fetchone() == (None, "tx")
        assert con.execute("SELECT tissue_id FROM lake.eqtl_catalogue.dataset "
                           "WHERE dataset_id = 'QTD000002'").fetchone() == (None,)
        assert con.execute("SELECT ftp_cs_path FROM lake.eqtl_catalogue.tabix_ftp_path "
                           "WHERE dataset_id = 'QTD000002'").fetchone()[0].endswith(
            "/QTD000002.credible_sets.tsv.gz")
        # raw's columns, names and types, without landed_in
        for table, columns in (("dataset", DATASET_COLUMNS), ("tabix_ftp_path", PATH_COLUMNS)):
            assert [(c[0], c[1]) for c in con.execute(
                f"DESCRIBE lake.eqtl_catalogue.{table}").fetchall()] == [
                *[(c, "VARCHAR") for c in columns], ("eqtlcatalogue_release", "VARCHAR")]

        # re-landing the same release writes nothing
        before = con.execute(SNAP).fetchone()[0]
        eqtl_catalogue.curate(con, root / "data_tables" / "dataset_metadata_r7.tsv",
                              root / "tabix" / "tabix_ftp_paths.tsv", "7")
        assert con.execute(SNAP).fetchone()[0] == before

        # a limited load never prunes; a full load of the next release drops what it
        # no longer has and restamps every row, so each table is one release
        r8 = repo(tmp_path / "r8", "8", [D1, D3], [P1, P3])
        files = (r8 / "data_tables" / "dataset_metadata_r8.tsv",
                 r8 / "tabix" / "tabix_ftp_paths.tsv")
        # curate returns each table's row count, not the rows written
        assert eqtl_catalogue.curate(con, *files, "8", limit=1) == {
            "dataset": 3, "tabix_ftp_path": 3}
        assert eqtl_catalogue.curate(con, *files, "8") == {"dataset": 2, "tabix_ftp_path": 2}
        assert con.execute(DATASETS).fetchall() == [("QTD000001", "8"), ("QTD000003", "8")]
        assert con.execute("SELECT DISTINCT eqtlcatalogue_release FROM "
                           "lake.eqtl_catalogue.tabix_ftp_path").fetchall() == [("8",)]
    finally:
        con.close()


def test_a_malformed_or_unpaired_file_fails_before_landing_and_keeps_the_tables(
        lake_settings, tmp_path):
    con = lake_connect(lake_settings)
    try:
        good = (tsv(tmp_path / "d.tsv", DATASET_COLUMNS, [D1, D2]),
                tsv(tmp_path / "p.tsv", PATH_COLUMNS, [P1, P2]))
        eqtl_catalogue.curate(con, *good, "7")
        before = con.execute(SNAP).fetchone()[0]

        # a line with a field too many, past a limited load's rows, in either file
        ragged = tsv(tmp_path / "ragged.tsv", DATASET_COLUMNS, [D1, D2 + "\textra"])
        with pytest.raises(ValueError, match="data line 2 has 12 fields, not 11"):
            eqtl_catalogue.curate(con, ragged, good[1], "7", limit=1)
        short = tsv(tmp_path / "short.tsv", PATH_COLUMNS, [P1, "QTS000001\tQTD000002"])
        with pytest.raises(ValueError, match="data line 2 has 2 fields"):
            eqtl_catalogue.curate(con, good[0], short, "7", limit=1)

        # a renamed column
        renamed = tsv(tmp_path / "renamed.tsv", DATASET_COLUMNS, [D1, D2],
                      [*DATASET_COLUMNS[:-1], "study_design"])
        with pytest.raises(ValueError, match="study_design"):
            eqtl_catalogue.curate(con, renamed, good[1], "7")

        # the path table is for another release: it names a dataset the metadata does
        # not, past a limited load's rows (release 8's metadata against r7's paths)
        extra = tsv(tmp_path / "extra.tsv", PATH_COLUMNS, [P1, P2, P3])
        with pytest.raises(ValueError, match="1 datasets are in only one .* QTD000003"):
            eqtl_catalogue.curate(con, good[0], extra, "7", limit=1)

        assert con.execute(SNAP).fetchone()[0] == before
        assert con.execute(DATASETS).fetchall() == [("QTD000001", "7"), ("QTD000002", "7")]
    finally:
        con.close()


def test_a_header_only_file_is_refused_and_the_tables_kept(lake_settings, tmp_path):
    con = lake_connect(lake_settings)
    try:
        d = tsv(tmp_path / "d.tsv", DATASET_COLUMNS, [D1])
        p = tsv(tmp_path / "p.tsv", PATH_COLUMNS, [P1])
        eqtl_catalogue.curate(con, d, p, "7")
        before = con.execute(SNAP).fetchone()[0]
        empty = tsv(tmp_path / "empty.tsv", DATASET_COLUMNS, [])
        for limit in (None, 1):
            with pytest.raises(ValueError, match="zero rows"):
                eqtl_catalogue.curate(con, empty, p, "8", limit=limit)
        assert con.execute(SNAP).fetchone()[0] == before
        assert con.execute(DATASETS).fetchall() == [("QTD000001", "7")]
    finally:
        con.close()


def test_the_default_run_downloads_the_pinned_release_into_the_raw_layer(
        lake_settings, tmp_path, monkeypatch):
    upstream = repo(tmp_path / "upstream", eqtl_catalogue.RELEASE, [D1, D2], [P1, P2])
    base = lake_settings.eqtl_catalogue_url
    fetched = []

    def download(url, dest):
        fetched.append((url.removeprefix(base), dest))
        dest.parent.mkdir(parents=True, exist_ok=True)
        return Path(shutil.copy(upstream / url.removeprefix(base + "/"), dest))

    monkeypatch.setattr(INGEST, "download", download)
    summary = eqtl_catalogue.ingest(settings=lake_settings)
    assert (summary["version"], summary["dataset"], summary["tabix_ftp_path"]) == ("7", 2, 2)
    assert [u for u, _ in fetched] == ["/data_tables/dataset_metadata_r7.tsv",
                                       "/tabix/tabix_ftp_paths.tsv"]
    # keyed by version under the source's raw dir
    assert all(d.parent.name == "7" and d.parent.parent.name == "eqtl_catalogue"
               for _, d in fetched)
