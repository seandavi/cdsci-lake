"""Offline tests for the BEDbase source (bioc-on-ice#183).

The fixture is written at test time in the real snapshot's form (schema_version
1, probed on the 2026-09-01 files): Parquet with the timestamps TIMESTAMPTZ,
the counts BIGINT, the two id lists VARCHAR[], named as upstream names its
files, and an index entry per file with its sha256 and record count.
"""

from __future__ import annotations

import hashlib
import importlib
import shutil
from pathlib import Path

import duckdb
import pytest

from cdsci.lake import Settings, lake_connect
from cdsci.lake.sources import bedbase
from cdsci.lake.sources.bedbase.ingest import (
    BEDSETS_SCHEMA,
    MEMBERSHIP_SCHEMA,
    METADATA_SCHEMA,
    file_name,
)

# the module itself: the package's `ingest` attribute is the function
INGEST = importlib.import_module("cdsci.lake.sources.bedbase.ingest")

SNAP = "SELECT max(snapshot_id) FROM lake.snapshots()"
BEDS = "SELECT id, bedbase_version FROM lake.bedbase.bed ORDER BY id"
MEMBERS = ("SELECT bedset_id, bedfile_id, bedbase_version FROM lake.bedbase.bedset_membership "
           "ORDER BY ALL")

# raw.bedbase__bed's columns minus landed_in, in raw's order, then the version
BED_COLUMNS = [
    ("id", "VARCHAR"), ("name", "VARCHAR"), ("description", "VARCHAR"),
    ("genome_alias", "VARCHAR"), ("genome_digest", "VARCHAR"), ("bed_compliance", "VARCHAR"),
    ("data_format", "VARCHAR"), ("compliant_columns", "INTEGER"),
    ("non_compliant_columns", "INTEGER"), ("is_universe", "BOOLEAN"), ("license_id", "VARCHAR"),
    ("processed", "BOOLEAN"), ("submission_date", "VARCHAR"), ("last_update_date", "VARCHAR"),
    ("annotation_organism", "VARCHAR"), ("annotation_species_id", "VARCHAR"),
    ("annotation_genotype", "VARCHAR"), ("annotation_phenotype", "VARCHAR"),
    ("annotation_description", "VARCHAR"), ("annotation_cell_type", "VARCHAR"),
    ("annotation_cell_line", "VARCHAR"), ("annotation_tissue", "VARCHAR"),
    ("annotation_library_source", "VARCHAR"), ("annotation_assay", "VARCHAR"),
    ("annotation_antibody", "VARCHAR"), ("annotation_target", "VARCHAR"),
    ("annotation_treatment", "VARCHAR"), ("annotation_global_sample_id", "VARCHAR"),
    ("annotation_global_experiment_id", "VARCHAR"), ("annotation_original_file_name", "VARCHAR"),
    ("header", "VARCHAR"), ("indexed", "BOOLEAN"), ("file_indexed", "BOOLEAN"),
    ("bedbase_version", "VARCHAR"),
]
BEDSET_COLUMNS = [
    ("id", "VARCHAR"), ("name", "VARCHAR"), ("md5sum", "VARCHAR"), ("submission_date", "VARCHAR"),
    ("last_update_date", "VARCHAR"), ("description", "VARCHAR"), ("bedfile_count", "INTEGER"),
    ("author", "VARCHAR"), ("bedset_source", "VARCHAR"), ("summary", "VARCHAR"),
    ("bedset_means", "VARCHAR"), ("bedset_standard_deviation", "VARCHAR"),
    ("bedset_stats", "VARCHAR"), ("processed", "BOOLEAN"), ("bedbase_version", "VARCHAR"),
]

B1 = {"id": "0000120fe8c5334bb0ce759dfcf06c3b", "name": "GSM4837486 peaks",
      "genome_alias": "hg38", "compliant_columns": 6, "non_compliant_columns": 4,
      "is_universe": False, "license_id": "DUO:0000042", "processed": True,
      "submission_date": "2025-05-22 12:02:50.145308+00",
      # an offset, and zero microseconds: printed as UTC with the fraction dropped
      "last_update_date": "2025-05-22 14:02:50+02", "species_id": "9606, 11676",
      "global_sample_id": ["geo:gsm4837486", ""], "global_experiment_id": ["geo:gse159673"],
      "indexed": True}
B2 = {"id": "1111", "name": "universe", "is_universe": True, "global_sample_id": [],
      "submission_date": "2024-01-01 00:00:00.000001+00"}
B3 = {"id": "2222", "name": "no annotation"}
S1 = {"id": "gse33600", "name": "GSE33600", "source": "gse33600", "bedfile_count": 2,
      "bedset_means": '{"number_of_regions": 4363.0}',
      "submission_date": "2025-06-05 12:03:43.962219+00"}
S2 = {"id": "excluderanges", "bedfile_count": 1}
M = [{"bedset_id": "gse33600", "bedfile_id": "0000120fe8c5334bb0ce759dfcf06c3b"},
     {"bedset_id": "gse33600", "bedfile_id": "1111"},
     {"bedset_id": "excluderanges", "bedfile_id": "1111"}]


def parquet(path: Path, schema: dict[str, str], rows: list[dict]) -> Path:
    """``rows`` as Parquet with exactly ``schema``'s columns and types."""
    path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute("CREATE TABLE t (" + ", ".join(f'"{c}" {t}' for c, t in schema.items()) + ")")
    for r in rows:
        con.execute(f"INSERT INTO t VALUES ({', '.join('?' * len(schema))})",
                    [r.get(c) for c in schema])
    con.execute(f"COPY t TO '{path}' (FORMAT parquet)")
    con.close()
    return path


def snapshot(d: Path, version: str, beds: list[dict], bedsets: list[dict],
             members: list[dict], schemas: dict | None = None) -> dict[str, Path]:
    """One snapshot's three files under upstream's names; ``schemas`` overrides some."""
    files = {"metadata": (METADATA_SCHEMA, beds), "bedsets": (BEDSETS_SCHEMA, bedsets),
             "bedset_membership": (MEMBERSHIP_SCHEMA, members)}
    return {f: parquet(d / file_name(f, version), (schemas or {}).get(f, schema), rows)
            for f, (schema, rows) in files.items()}


@pytest.fixture
def lake_settings(tmp_path: Path) -> Settings:
    # lake_backend pinned: CU_OPENALEX_LAKE_BACKEND=postgres in the environment or a
    # .env would otherwise point these writes at the shared lake.
    return Settings(storage_base_uri=f"file://{tmp_path}", lake_backend="local",
                    lake_catalog=str(tmp_path / "catalog.ducklake"))


def seed(con, tmp_path: Path) -> int:
    bedbase.curate(con, snapshot(tmp_path / "seed", "2026-09-01", [B1, B2], [S1], M[:2]),
                   "2026-09-01")
    return con.execute(SNAP).fetchone()[0]


def test_bedbase_lands_the_snapshot_as_raw_has_it_and_a_new_one_replaces_the_last(
        lake_settings, tmp_path):
    d = tmp_path / "snap"
    snapshot(d, "2026-09-01", [B1, B2, B3], [S1, S2], M)
    summary = bedbase.ingest(directory=str(d), settings=lake_settings)
    assert (summary["version"], summary["bed"], summary["bedset"],
            summary["bedset_membership"]) == ("2026-09-01", 3, 2, 3)
    assert summary["snapshot_files"]["metadata"] == "bedbase_metadata_2026_09_01.parquet"

    con = lake_connect(lake_settings)
    try:
        # raw's names, types and order, minus landed_in, plus bedbase_version
        assert [c[:2] for c in con.execute("DESCRIBE lake.bedbase.bed").fetchall()] == BED_COLUMNS
        assert [c[:2] for c in con.execute("DESCRIBE lake.bedbase.bedset").fetchall()] == \
            BEDSET_COLUMNS
        assert [c[:2] for c in con.execute("DESCRIBE lake.bedbase.bedset_membership"
                                           ).fetchall()] == [
            ("bedset_id", "VARCHAR"), ("bedfile_id", "VARCHAR"), ("bedbase_version", "VARCHAR")]
        # the session's TimeZone does not move the printed UTC text
        con.execute("SET TimeZone = 'America/New_York'")
        q = """SELECT submission_date, last_update_date, annotation_species_id,
                      annotation_global_sample_id, annotation_global_experiment_id,
                      annotation_description, compliant_columns, is_universe
               FROM lake.bedbase.bed WHERE id = ?"""
        # the trailing '' upstream lists carry survives the join, as raw has it
        assert con.execute(q, [B1["id"]]).fetchone() == (
            "2025-05-22T12:02:50.145308Z", "2025-05-22T12:02:50Z", "9606, 11676",
            "geo:gsm4837486|", "geo:gse159673", None, 6, False)
        assert con.execute(q, ["1111"]).fetchone()[:4] == (
            "2024-01-01T00:00:00.000001Z", None, None, "")
        assert con.execute(q, ["2222"]).fetchone()[3:5] == (None, None)
        assert con.execute("SELECT bedset_source, bedfile_count, bedset_means FROM "
                           "lake.bedbase.bedset WHERE id = 'gse33600'").fetchone() == (
            "gse33600", 2, '{"number_of_regions": 4363.0}')

        # re-landing the same snapshot writes nothing
        paths = snapshot(tmp_path / "again", "2026-09-01", [B1, B2, B3], [S1, S2], M)
        before = con.execute(SNAP).fetchone()[0]
        assert bedbase.curate(con, paths, "2026-09-01") == {
            "bed": 3, "bedset": 2, "bedset_membership": 3}
        assert con.execute(SNAP).fetchone()[0] == before

        # a limited load never prunes; a full load of the next snapshot drops gone
        # records and restamps every row, so each table is one snapshot
        later = snapshot(tmp_path / "later", "2026-10-16", [B1, B3], [S1], M[1:2])
        # curate returns the table's row count, not the rows written
        assert bedbase.curate(con, later, "2026-10-16", limit=1) == {
            "bed": 3, "bedset": 2, "bedset_membership": 3}
        assert bedbase.curate(con, later, "2026-10-16") == {
            "bed": 2, "bedset": 1, "bedset_membership": 1}
        assert con.execute(BEDS).fetchall() == [(B1["id"], "2026-10-16"), ("2222", "2026-10-16")]
        assert con.execute(MEMBERS).fetchall() == [("gse33600", "1111", "2026-10-16")]
    finally:
        con.close()


def test_a_malformed_snapshot_fails_before_landing_and_keeps_the_tables(lake_settings, tmp_path):
    con = lake_connect(lake_settings)
    try:
        before = seed(con, tmp_path)
        renamed = {("organism" if c == "species_name" else c): t
                   for c, t in METADATA_SCHEMA.items()}
        bad = {
            # each in one file only; the others are good, and the bad one is not the first
            "added": {"bedset_membership": {**MEMBERSHIP_SCHEMA, "role": "VARCHAR"}},
            "dropped": {"bedsets": {c: t for c, t in BEDSETS_SCHEMA.items() if c != "summary"}},
            "renamed": {"metadata": renamed},
            "retyped": {"bedsets": {**BEDSETS_SCHEMA, "bedfile_count": "VARCHAR"}},
            "list flattened": {"metadata": {**METADATA_SCHEMA, "global_sample_id": "VARCHAR"}},
        }
        for name, override in bad.items():
            paths = snapshot(tmp_path / name, "2026-10-16", [B1, B2, B3], [S1, S2], M, override)
            for limit in (None, 1):
                with pytest.raises(ValueError, match="schema is not the declared one"):
                    bedbase.curate(con, paths, "2026-10-16", limit=limit)
        assert con.execute(SNAP).fetchone()[0] == before
        assert con.execute(BEDS).fetchall() == [(B1["id"], "2026-09-01"), ("1111", "2026-09-01")]
    finally:
        con.close()


def test_an_empty_file_is_refused_and_the_tables_kept(lake_settings, tmp_path):
    con = lake_connect(lake_settings)
    try:
        before = seed(con, tmp_path)
        paths = snapshot(tmp_path / "empty", "2026-10-16", [B1], [S1], [])
        for limit in (None, 1):
            with pytest.raises(ValueError, match="zero rows"):
                bedbase.curate(con, paths, "2026-10-16", limit=limit)
        assert con.execute(SNAP).fetchone()[0] == before
        assert con.execute(MEMBERS).fetchall() == [
            (m["bedset_id"], m["bedfile_id"], "2026-09-01") for m in M[:2]]
    finally:
        con.close()


def test_a_nonpositive_limit_is_refused(lake_settings, tmp_path):
    con = lake_connect(lake_settings)
    try:
        before = seed(con, tmp_path)
        paths = snapshot(tmp_path / "next", "2026-10-16", [B1], [S1], M[:1])
        for limit in (0, -1):
            with pytest.raises(ValueError, match="limit must be positive"):
                bedbase.curate(con, paths, "2026-10-16", limit=limit)
        assert con.execute(SNAP).fetchone()[0] == before
    finally:
        con.close()


def test_the_default_run_downloads_the_asked_snapshot_verified_against_the_index(
        lake_settings, tmp_path, monkeypatch):
    upstream = {}
    for v, beds in (("2026-09-01", [B1, B2]), ("2026-10-16", [B1, B2, B3])):
        upstream.update({p.name: p for p in snapshot(tmp_path / "up" / v, v, beds, [S1], M[:2])
                         .values()})

    def entry(name: str) -> dict:
        p = upstream[name]
        # bedbase_<file_type>_<YYYY_MM_DD>.parquet
        file_type = name.removeprefix("bedbase_").removesuffix(".parquet")[:-11]
        stamp = name.removesuffix(".parquet")[-10:].replace("_", "-")
        return {"file_path": f"https://data2.bedbase.org/snapshot/{name}", "file_type": file_type,
                "creation_date": f"{stamp}T00:25:10.695219Z",
                "record_count": duckdb.sql(f"SELECT count(*) FROM '{p}'").fetchone()[0],
                "checksum": hashlib.sha256(p.read_bytes()).hexdigest()}

    index = {"results": [entry(n) for n in upstream]
             + [{"file_type": "manifest", "creation_date": "2026-10-16T00:25:10.695219Z"}]}
    fetched = []

    def download(url, dest, *, headers):
        fetched.append((url, dest, headers))
        dest.parent.mkdir(parents=True, exist_ok=True)
        return Path(shutil.copy(upstream[url.rsplit("/", 1)[1]], dest))

    monkeypatch.setattr(INGEST, "get_json", lambda url, headers: index)
    monkeypatch.setattr(INGEST, "download", download)

    # an older snapshot by version, keyed by it under the source's raw dir
    summary = bedbase.ingest(version="2026-09-01", settings=lake_settings)
    assert (summary["version"], summary["bed"]) == ("2026-09-01", 2)
    assert all(h == INGEST.UA for _, _, h in fetched)
    assert {(d.parent.name, d.parent.parent.name) for _, d, _ in fetched} == {
        ("2026-09-01", "bedbase")}
    # the newest by default
    assert bedbase.ingest(settings=lake_settings)["bed"] == 3

    # a digest that is not the index's: the file is removed and nothing lands
    index["results"][0]["checksum"] = "0" * 64
    fetched.clear()
    with pytest.raises(ValueError, match="sha256"):
        bedbase.ingest(version="2026-09-01", settings=lake_settings)
    assert not fetched[0][1].exists()

    # a row count that is not the index's fails before landing
    index["results"][0]["checksum"] = entry(index["results"][0]["file_path"].rsplit("/", 1)[1]
                                            )["checksum"]
    index["results"][0]["record_count"] += 1
    with pytest.raises(ValueError, match="the index says"):
        bedbase.ingest(version="2026-09-01", settings=lake_settings)
    with pytest.raises(ValueError, match="snapshot 2026-01-01 lists"):
        bedbase.ingest(version="2026-01-01", settings=lake_settings)
    con = lake_connect(lake_settings)
    try:
        assert {v for _, v in con.execute(BEDS).fetchall()} == {"2026-10-16"}
    finally:
        con.close()
