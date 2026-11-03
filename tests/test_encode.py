"""Offline tests for the ENCODE source (bioc-on-ice#183).

The fixture is written at test time in the real report.tsv dialect (probed
2026-11-03): a first line '<UTC timestamp>\\t<request URL>' echoing the
``field=`` list as ``/report/?...&limit=all``, a display-title header, then
tab-separated rows, CRLF throughout and after the last row, no quoting ('"' is
data), the empty cell for missing, lists ','-joined.
"""

from __future__ import annotations

import importlib
import shutil
from pathlib import Path

import duckdb
import pytest

from cdsci.lake import Settings, lake_connect
from cdsci.lake.sources import encode
from cdsci.lake.sources.encode import EXPERIMENT_FIELDS, FILE_FIELDS

# the module itself: the package's `ingest` attribute is the function
INGEST = importlib.import_module("cdsci.lake.sources.encode.ingest")

SNAP = "SELECT max(snapshot_id) FROM lake.snapshots()"
EXPERIMENTS = "SELECT accession, retrieval_date FROM lake.encode.experiment ORDER BY accession"
FILES = "SELECT title, retrieval_date FROM lake.encode.file ORDER BY title"


def row(fields: tuple[str, ...], **cells: str) -> str:
    return "\t".join(cells.get(f.replace(".", "_"), "") for f in fields)


def exp(accession: str, **cells: str) -> str:
    return row(EXPERIMENT_FIELDS, accession=accession, status="released", **cells)


def fil(title: str, **cells: str) -> str:
    return row(FILE_FIELDS, title=title, status="released", **cells)


E1 = exp("ENCSR000AKS", description='CTCF ChIP-seq on "K562" \u2013 replicate 1',
         target_genes_geneid="10664,10665", biosample_ontology_term_id="EFO:0002067",
         replicates_library_biosample_organism_taxon_id="9606,10090")
E2 = exp("ENCSR810EEJ", date_submitted="2018-02-26")
E3 = exp("ENCSR536ZNI")
F1 = fil("ENCFF002FAS", accession="ENCFF002FAS", dataset="/experiments/ENCSR000AKS/",
         file_size="713034762", derived_from="/files/ENCFF001AAA/,/files/GRCh38_EBV.chrom.sizes/")
F2 = fil("SRR1270455", external_accession="SRR1270455", dataset="/annotations/ENCSR999ZZZ/")
F3 = fil("ENCFF003XYZ", accession="ENCFF003XYZ")


def report(path: Path, type_: str, fields: tuple[str, ...], rows: list[str], *,
           stamp: str = "2026-09-18 21:23:01.337794",
           echoed: tuple[str, ...] | None = None) -> Path:
    url = (f"https://www.encodeproject.org/report/?type={type_}&"
           + "&".join(f"field={f}" for f in (fields if echoed is None else echoed)) + "&limit=all")
    titles = "\t".join(f.replace(".", " ").capitalize() for f in fields)
    path.write_bytes("".join(f"{line}\r\n" for line in (f"{stamp}\t{url}", titles, *rows))
                     .encode("utf-8"))
    return path


def experiments(path: Path, rows: list[str], **kw) -> Path:
    return report(path, "Experiment", EXPERIMENT_FIELDS, rows, **kw)


def files(path: Path, rows: list[str], **kw) -> Path:
    return report(path, "File", FILE_FIELDS, rows, **kw)


@pytest.fixture
def lake_settings(tmp_path: Path) -> Settings:
    # lake_backend pinned: CU_OPENALEX_LAKE_BACKEND=postgres in the environment or a
    # .env would otherwise point these writes at the shared lake.
    return Settings(storage_base_uri=f"file://{tmp_path}", lake_backend="local",
                    lake_catalog=str(tmp_path / "catalog.ducklake"))


def seed(con, tmp_path: Path) -> int:
    encode.curate(con, experiments(tmp_path / "seed-e.tsv", [E1, E2]),
                  files(tmp_path / "seed-f.tsv", [F1, F2]))
    return con.execute(SNAP).fetchone()[0]


def test_encode_lands_both_reports_verbatim_and_a_new_crawl_replaces_the_last(
        lake_settings, tmp_path):
    e = experiments(tmp_path / "e.tsv", [E1, E2, E3])
    # a download straddling UTC midnight: each table carries its own report's date
    f = files(tmp_path / "f.tsv", [F1, F2, F3], stamp="2026-09-19 00:04:11.002")
    summary = encode.ingest(experiments=str(e), files=str(f), settings=lake_settings)
    assert (summary["version"], summary["experiment"], summary["file"]) == ("2026-09-18", 3, 3)

    con = lake_connect(lake_settings)
    try:
        # raw's columns, all VARCHAR, without landed_in
        for table, fields in (("experiment", EXPERIMENT_FIELDS), ("file", FILE_FIELDS)):
            described = con.execute(f"DESCRIBE lake.encode.{table}").fetchall()
            assert [(c[0], c[1]) for c in described] == [
                *[(f.replace(".", "_"), "VARCHAR") for f in fields], ("retrieval_date", "VARCHAR")]
        # quotes are data, lists stay joined, the empty cell is NULL, no trailing '\r'
        assert con.execute("""
            SELECT description, target_genes_geneid, replicates_library_biosample_organism_taxon_id,
                   date_submitted, possible_controls
            FROM lake.encode.experiment WHERE accession = 'ENCSR000AKS'""").fetchone() == (
            'CTCF ChIP-seq on "K562" \u2013 replicate 1', "10664,10665", "9606,10090", None, None)
        assert con.execute("SELECT accession, external_accession, dataset, superseded_by "
                           "FROM lake.encode.file WHERE title = 'SRR1270455'").fetchone() == (
            None, "SRR1270455", "/annotations/ENCSR999ZZZ/", None)
        assert con.execute(EXPERIMENTS).fetchall() == [
            (a, "2026-09-18") for a in ("ENCSR000AKS", "ENCSR536ZNI", "ENCSR810EEJ")]
        assert {d for _, d in con.execute(FILES).fetchall()} == {"2026-09-19"}

        # re-landing the same crawl writes nothing
        before = con.execute(SNAP).fetchone()[0]
        assert encode.curate(con, e, f) == {"experiment": 3, "file": 3}
        assert con.execute(SNAP).fetchone()[0] == before

        # a limited load never prunes; a full load of the next crawl drops gone objects
        # and restamps every row, so each table is one crawl
        e2 = experiments(tmp_path / "e2.tsv", [E1, E3], stamp="2026-11-03 14:00:00")
        f2 = files(tmp_path / "f2.tsv", [F1, F3], stamp="2026-11-03 14:01:00")
        # curate returns the table's row count, not the rows written
        assert encode.curate(con, e2, f2, limit=1) == {"experiment": 3, "file": 3}
        assert encode.curate(con, e2, f2) == {"experiment": 2, "file": 2}
        assert con.execute(EXPERIMENTS).fetchall() == [
            ("ENCSR000AKS", "2026-11-03"), ("ENCSR536ZNI", "2026-11-03")]
        assert con.execute(FILES).fetchall() == [
            ("ENCFF002FAS", "2026-11-03"), ("ENCFF003XYZ", "2026-11-03")]
    finally:
        con.close()


def test_a_malformed_report_fails_before_landing_and_keeps_the_tables(lake_settings, tmp_path):
    con = lake_connect(lake_settings)
    try:
        before = seed(con, tmp_path)
        good_e = experiments(tmp_path / "good-e.tsv", [E1, E2, E3])
        good_f = files(tmp_path / "good-f.tsv", [F1, F2, F3])
        bad = {
            # a row with an extra cell, and one short a cell, past a limited load's rows
            "extra": files(tmp_path / "extra.tsv", [F1, F2, F3 + "\tstray"]),
            "short": files(tmp_path / "short.tsv", [F1, F2, F3.rsplit("\t", 1)[0]]),
            # the request the report echoes is not ours: a field dropped, or reordered
            "dropped": files(tmp_path / "dropped.tsv", [F1], echoed=FILE_FIELDS[:-1]),
            "reordered": files(tmp_path / "reordered.tsv", [F1],
                               echoed=(FILE_FIELDS[1], FILE_FIELDS[0], *FILE_FIELDS[2:])),
        }
        for name, f in bad.items():
            ragged = name in ("extra", "short")
            with pytest.raises(duckdb.InvalidInputException if ragged else ValueError,
                               match="Expected Number of Columns" if ragged
                               else "does not echo the requested fields"):
                encode.curate(con, good_e, f, limit=1)
        # the experiment report is checked too, before the file report lands
        with pytest.raises(duckdb.InvalidInputException, match="Expected Number of Columns"):
            encode.curate(con, experiments(tmp_path / "ragged-e.tsv", [E1, E2, E3 + "\tx"]),
                          good_f)
        assert con.execute(SNAP).fetchone()[0] == before
        assert con.execute(EXPERIMENTS).fetchall() == [
            ("ENCSR000AKS", "2026-09-18"), ("ENCSR810EEJ", "2026-09-18")]
    finally:
        con.close()


def test_a_header_only_report_is_refused_and_the_tables_kept(lake_settings, tmp_path):
    con = lake_connect(lake_settings)
    try:
        before = seed(con, tmp_path)
        empty = files(tmp_path / "empty.tsv", [])
        e = experiments(tmp_path / "e.tsv", [E1])
        for limit in (None, 1):
            with pytest.raises(ValueError, match="zero rows"):
                encode.curate(con, e, empty, limit=limit)
        assert con.execute(SNAP).fetchone()[0] == before
        assert con.execute(FILES).fetchall() == [
            ("ENCFF002FAS", "2026-09-18"), ("SRR1270455", "2026-09-18")]
    finally:
        con.close()


def test_a_nonpositive_limit_is_refused(lake_settings, tmp_path):
    con = lake_connect(lake_settings)
    try:
        before = seed(con, tmp_path)
        e, f = experiments(tmp_path / "e.tsv", [E1]), files(tmp_path / "f.tsv", [F1])
        for limit in (0, -1):
            with pytest.raises(ValueError, match="limit must be positive"):
                encode.curate(con, e, f, limit=limit)
        assert con.execute(SNAP).fetchone()[0] == before
    finally:
        con.close()


def test_the_default_run_downloads_both_reports_and_a_short_one_is_removed(
        lake_settings, tmp_path, monkeypatch):
    upstream = {"experiment": experiments(tmp_path / "up-e.tsv", [E1, E2]),
                "file": files(tmp_path / "up-f.tsv", [F1, F2])}
    totals = {"Experiment": 2, "File": 2}
    fetched = []

    def download(url, dest, *, headers):
        fetched.append((url, dest, headers))
        dest.parent.mkdir(parents=True, exist_ok=True)
        return Path(shutil.copy(upstream[dest.stem.split("-")[-1]], dest))

    monkeypatch.setattr(INGEST, "download", download)
    monkeypatch.setattr(INGEST, "total", lambda type_, s: totals[type_])
    summary = encode.ingest(settings=lake_settings)
    assert (summary["version"], summary["experiment"], summary["file"]) == ("2026-09-18", 2, 2)
    assert [u for u, _, _ in fetched] == [
        INGEST.report_url("Experiment", EXPERIMENT_FIELDS, lake_settings),
        INGEST.report_url("File", FILE_FIELDS, lake_settings)]
    assert all(h == INGEST.UA for _, _, h in fetched)
    # keyed by the download's UTC date under the source's raw dir
    assert all(d.parent.name == "encode" and d.name.endswith(("-experiment.tsv", "-file.tsv"))
               for _, d, _ in fetched)

    # fewer rows than the portal's total: a short download is removed, nothing lands
    totals["File"] = 3
    fetched.clear()
    with pytest.raises(ValueError, match="short download"):
        encode.ingest(settings=lake_settings)
    assert not fetched[1][1].exists() and fetched[0][1].exists()
    con = lake_connect(lake_settings)
    try:
        assert con.execute(FILES).fetchall() == [
            ("ENCFF002FAS", "2026-09-18"), ("SRR1270455", "2026-09-18")]
    finally:
        con.close()
