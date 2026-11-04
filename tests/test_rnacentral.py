"""Offline tests for the RNAcentral source (bioc-on-ice#183).

The fixture is written at test time in the real file's dialect (probed on the
first lines of release 27's id_mapping.tsv.gz, 2026-11-03): gzipped, no header,
six tab-separated columns, CRLF, no quoting, an empty cell for a missing gene name.
"""

from __future__ import annotations

import gzip
import importlib
import shutil
from pathlib import Path

import pytest

from cdsci.lake import Settings, lake_connect
from cdsci.lake.sources import rnacentral

# the module itself: the package's `ingest` attribute is the function
INGEST = importlib.import_module("cdsci.lake.sources.rnacentral.ingest")

SNAP = "SELECT max(snapshot_id) FROM lake.snapshots()"
ROWS = ("SELECT line_number, urs, external_id, gene_name, rnacentral_release "
        "FROM lake.rnacentral.id_mapping ORDER BY line_number")

R1 = "URS0000000001\tENA\tAL009126.3:2357010..2357213:rRNA\t224308\trRNA\t"
R2 = "URS0000626831\tHGNC\tHGNC:35391\t9606\tpre_miRNA\tMIR1302-2"
# '"' and "'" are data: quoting is off
R3 = 'URS0000626831\tENSEMBL\tENST00000408549\t9606\tpre_miRNA\tENSG"0\'1'


def gz(path: Path, rows: list[str]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wt", newline="") as f:
        f.write("".join(r + "\r\n" for r in rows))
    return path


@pytest.fixture
def lake_settings(tmp_path: Path) -> Settings:
    # lake_backend pinned: CU_OPENALEX_LAKE_BACKEND=postgres in the environment or a
    # .env would otherwise point these writes at the shared lake.
    return Settings(storage_base_uri=f"file://{tmp_path}", lake_backend="local",
                    lake_catalog=str(tmp_path / "catalog.ducklake"))


def test_rnacentral_lands_whole_and_a_new_release_replaces_the_last(lake_settings, tmp_path):
    f = gz(tmp_path / "releases" / "27.0" / "id_mapping" / "id_mapping.tsv.gz", [R1, R2, R3])
    result = rnacentral.ingest(file=str(f), batches=2, settings=lake_settings)
    assert (result["version"], result["rows"]) == ("27", 3)

    con = lake_connect(lake_settings)
    try:
        # the key, then raw's columns, names and types, without landed_in
        assert [(c[0], c[1]) for c in con.execute(
            "DESCRIBE lake.rnacentral.id_mapping").fetchall()] == [
            ("line_number", "INTEGER"), *rnacentral.COLUMNS.items(),
            ("rnacentral_release", "VARCHAR")]
        assert con.execute(ROWS).fetchall() == [
            (1, "URS0000000001", "AL009126.3:2357010..2357213:rRNA", None, "27"),
            (2, "URS0000626831", "HGNC:35391", "MIR1302-2", "27"),
            (3, "URS0000626831", "ENST00000408549", "ENSG\"0'1", "27"),
        ]

        # re-landing the same release writes nothing
        before = con.execute(SNAP).fetchone()[0]
        assert rnacentral.curate(con, f, "27", batches=2) == 3
        assert con.execute(SNAP).fetchone()[0] == before

        # a limited load never prunes (and returns the table's row count); a full load
        # of the next release drops withdrawn lines, in every shard, and restamps
        nxt = gz(tmp_path / "next.tsv.gz", [R2])
        assert rnacentral.curate(con, nxt, "28", limit=1) == 3
        assert rnacentral.curate(con, nxt, "28", batches=2) == 1
        assert con.execute(ROWS).fetchall() == [
            (1, "URS0000626831", "HGNC:35391", "MIR1302-2", "28")]
    finally:
        con.close()


def test_line_number_is_the_physical_line_under_parallel_batched_scans(tmp_path):
    # the key relies on row_number() OVER () following file order with
    # preserve_insertion_order=false, several threads and a shard filter after it
    s = Settings(storage_base_uri=f"file://{tmp_path}", lake_backend="local",
                 lake_catalog=str(tmp_path / "catalog.ducklake"), duckdb_threads=8)
    f = gz(tmp_path / "big.tsv.gz",
           [f"URS{i:010d}\tENA\tX{i}\t{i % 50}\trRNA\t{'g' * (i % 97)}" for i in range(1, 300_001)])
    con = lake_connect(s)
    try:
        assert rnacentral.curate(con, f, "27", batches=3) == 300_000
        assert con.execute("""
            SELECT count(*) FROM lake.rnacentral.id_mapping
            WHERE external_id <> 'X' || line_number""").fetchone()[0] == 0
        before = con.execute(SNAP).fetchone()[0]
        assert rnacentral.curate(con, f, "27", batches=3) == 300_000
        assert con.execute(SNAP).fetchone()[0] == before
    finally:
        con.close()


def test_a_malformed_or_empty_file_fails_before_landing_and_keeps_the_table(
        lake_settings, tmp_path):
    con = lake_connect(lake_settings)
    try:
        rnacentral.curate(con, gz(tmp_path / "seed.tsv.gz", [R1, R2]), "27", batches=1)
        before = con.execute(SNAP).fetchone()[0]
        # an extra cell, a missing one, a blank line: past a limited load's rows,
        # so only the whole-file check sees it
        padding = [f"URS{i:010d}\tENA\tX{i}\t9606\trRNA\t" for i in range(20_000)]
        for name, row in (("extra", R2 + "\tstray"), ("short", R2.rsplit("\t", 1)[0]),
                          ("blank", "")):
            bad = gz(tmp_path / f"{name}.tsv.gz", [R1, *padding, row])
            for limit in (None, 1):
                with pytest.raises(ValueError, match="fields"):
                    rnacentral.curate(con, bad, "28", limit=limit)
        for limit in (None, 1):
            with pytest.raises(ValueError, match="zero rows"):
                rnacentral.curate(con, gz(tmp_path / "empty.tsv.gz", []), "28", limit=limit)
        for limit in (0, -1):
            with pytest.raises(ValueError, match="limit must be positive"):
                rnacentral.curate(con, gz(tmp_path / "one.tsv.gz", [R3]), "28", limit=limit)
        assert con.execute(SNAP).fetchone()[0] == before
        assert [r[:2] for r in con.execute(ROWS).fetchall()] == [
            (1, "URS0000000001"), (2, "URS0000626831")]
    finally:
        con.close()


def test_a_local_file_must_name_its_release(lake_settings, tmp_path):
    f = gz(tmp_path / "id_mapping.tsv.gz", [R1])
    with pytest.raises(ValueError, match="pass version"):
        rnacentral.ingest(file=str(f), settings=lake_settings)
    with pytest.raises(ValueError, match="not a release number"):
        rnacentral.ingest(file=str(f), version="current", settings=lake_settings)
    result = rnacentral.ingest(file=str(f), version="27", settings=lake_settings)
    assert (result["version"], result["rows"]) == ("27", 1)


def test_a_release_before_20_is_refused_for_its_licence(lake_settings, tmp_path, monkeypatch):
    # the registry declares cc0, which holds from release 20 on
    monkeypatch.setattr(INGEST, "download", lambda url, dest: pytest.fail("downloaded"))
    old = gz(tmp_path / "releases" / "19.0" / "id_mapping" / "id_mapping.tsv.gz", [R1])
    with pytest.raises(ValueError, match="predates CC0"):
        rnacentral.ingest(file=str(old), settings=lake_settings)
    for version in ("19", "019", "0"):
        with pytest.raises(ValueError, match="EMBL-EBI's Terms of use"):
            rnacentral.ingest(version=version, settings=lake_settings)
    monkeypatch.setattr(INGEST, "latest", lambda s: "19")
    with pytest.raises(ValueError, match="predates CC0"):
        rnacentral.ingest(settings=lake_settings)
    con = lake_connect(lake_settings)
    try:
        with pytest.raises(ValueError, match="predates CC0"):
            rnacentral.curate(con, old, "19", batches=1)
    finally:
        con.close()


def test_the_default_run_downloads_the_current_release(lake_settings, tmp_path, monkeypatch):
    upstream = gz(tmp_path / "up.tsv.gz", [R1, R2])
    fetched = []

    def download(url, dest):
        fetched.append((url, dest))
        dest.parent.mkdir(parents=True, exist_ok=True)
        return Path(shutil.copy(upstream, dest))

    monkeypatch.setattr(INGEST, "download", download)
    monkeypatch.setattr(INGEST, "latest", lambda s: "28")
    result = rnacentral.ingest(settings=lake_settings)
    assert (result["version"], result["rows"]) == ("28", 2)
    [(url, dest)] = fetched
    assert url == ("https://ftp.ebi.ac.uk/pub/databases/RNAcentral/releases/28.0/"
                   "id_mapping/id_mapping.tsv.gz")
    assert dest.parent.name == "28" and dest.parent.parent.name == "rnacentral"
