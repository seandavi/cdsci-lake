"""Offline tests for the CELLxGENE source (bioc-on-ice#183).

The fixture is written at test time in the real listing's dialect: one JSON
array, keys sorted, no whitespace, non-ASCII as ``\\u`` escapes, a final newline,
JSON null for absent values, and ``batch_condition`` omitted where uncurated.
"""

from __future__ import annotations

import importlib
import json
import shutil
from pathlib import Path

import pytest

from cdsci.lake import Settings, lake_connect
from cdsci.lake.sources import cellxgene

# the module itself: the package's `ingest` attribute is the function
INGEST = importlib.import_module("cdsci.lake.sources.cellxgene.ingest")

SNAP = "SELECT max(snapshot_id) FROM lake.snapshots()"
ROWS = ("SELECT dataset_version_id, retrieval_date, census_release FROM lake.cellxgene.dataset "
        "ORDER BY dataset_version_id")
LTS = "2025-11-08"


def record(version_id: str, **fields) -> dict:
    r = {k: None for k in cellxgene.COLUMNS}
    r.update(dataset_id=f"d-{version_id}", dataset_version_id=version_id, collection_id="c1",
             cell_count=100, tombstone=False, is_pre_analysis=False,
             organism=[{"label": "Homo sapiens", "ontology_term_id": "NCBITaxon:9606"}],
             assets=[{"filesize": 713034762, "filetype": "H5AD",
                      "url": f"https://datasets.cellxgene.cziscience.com/{version_id}.h5ad"}])
    r.update(fields)
    return r


V1 = record("v1", collection_name="Zebrahub \u2013 Multimodal", title='A "quoted" title',
            mean_genes_per_cell=1512.1838053340666, primary_cell_count=90,
            spatial={"has_fullres": False, "is_single": True}, is_primary_data=[True],
            batch_condition=["donor_id"])
V2 = record("v2", is_pre_analysis=True)
V3 = record("v3")
for r in (V2, V3):
    del r["batch_condition"]  # upstream omits the key rather than writing null


def listing(path: Path, records: list) -> Path:
    path.write_text(json.dumps(records, separators=(",", ":"), sort_keys=True) + "\n")
    return path


@pytest.fixture
def lake_settings(tmp_path: Path) -> Settings:
    # lake_backend pinned: CU_OPENALEX_LAKE_BACKEND=postgres in the environment or a
    # .env would otherwise point these writes at the shared lake.
    return Settings(storage_base_uri=f"file://{tmp_path}", lake_backend="local",
                    lake_catalog=str(tmp_path / "catalog.ducklake"))


def test_cellxgene_lands_the_listing_typed_and_a_new_crawl_replaces_the_last(
        lake_settings, tmp_path):
    f = listing(tmp_path / "2026-09-18-datasets.json", [V1, V2, V3])
    summary = cellxgene.ingest(file=str(f), version="2026-09-18", census_release=LTS,
                               settings=lake_settings)
    assert (summary["version"], summary["rows"], summary["census_release"]) == (
        "2026-09-18", 3, LTS)

    con = lake_connect(lake_settings)
    try:
        # raw's columns, names and types, without landed_in
        typed = {"BIGINT", "DOUBLE", "BOOLEAN"}
        described = con.execute("DESCRIBE lake.cellxgene.dataset").fetchall()
        assert [(c[0], c[1]) for c in described] == [
            *[(k, t if t in typed else "VARCHAR") for k, t in cellxgene.COLUMNS.items()],
            ("retrieval_date", "VARCHAR"), ("census_release", "VARCHAR")]
        assert con.execute("""
            SELECT collection_name, title, mean_genes_per_cell, primary_cell_count, tombstone,
                   spatial, is_primary_data, batch_condition, organism, revised_at
            FROM lake.cellxgene.dataset WHERE dataset_version_id = 'v1'""").fetchone() == (
            "Zebrahub \u2013 Multimodal", 'A "quoted" title', 1512.1838053340666, 90, False,
            '{"has_fullres":false,"is_single":true}', "[true]", '["donor_id"]',
            '[{"label":"Homo sapiens","ontology_term_id":"NCBITaxon:9606"}]', None)
        # JSON null and an omitted key both read as NULL
        assert con.execute("SELECT spatial, batch_condition, is_pre_analysis FROM "
                           "lake.cellxgene.dataset WHERE dataset_version_id = 'v2'"
                           ).fetchone() == (None, None, True)

        # re-landing the same crawl writes nothing
        before = con.execute(SNAP).fetchone()[0]
        cellxgene.curate(con, f, "2026-09-18", LTS)
        assert con.execute(SNAP).fetchone()[0] == before

        # a limited load never prunes; a full load of the next crawl drops a revised
        # dataset version and restamps every row, so the table is one crawl
        later = listing(tmp_path / "2026-11-03-datasets.json", [V1, V3])
        # curate returns the table's row count, not the rows written
        assert cellxgene.curate(con, later, "2026-11-03", LTS, limit=1) == 3
        assert cellxgene.curate(con, later, "2026-11-03", LTS) == 2
        assert con.execute(ROWS).fetchall() == [
            ("v1", "2026-11-03", LTS), ("v3", "2026-11-03", LTS)]
        # a new Census LTS restamps every row
        cellxgene.curate(con, later, "2026-11-03", "2026-05-01")
        assert {r[2] for r in con.execute(ROWS).fetchall()} == {"2026-05-01"}
    finally:
        con.close()


def test_a_malformed_listing_fails_before_landing_and_keeps_the_table(lake_settings, tmp_path):
    con = lake_connect(lake_settings)
    try:
        cellxgene.curate(con, listing(tmp_path / "seed.json", [V1, V2]), "2026-09-18", LTS)
        before = con.execute(SNAP).fetchone()[0]

        # a record without its key, past a limited load's rows
        keyless = {k: v for k, v in V3.items() if k != "dataset_version_id"}
        with pytest.raises(ValueError, match="record 2 is not an object"):
            cellxgene.curate(con, listing(tmp_path / "keyless.json", [V1, V2, keyless]),
                             "2026-11-03", LTS, limit=1)
        with pytest.raises(ValueError, match="record 1 is not an object"):
            cellxgene.curate(con, listing(tmp_path / "scalar.json", [V1, "v2"]), "2026-11-03",
                             LTS, limit=1)
        # a renamed key, in every record
        renamed = [{("n_cells" if k == "cell_count" else k): v for k, v in r.items()}
                   for r in (V1, V2)]
        with pytest.raises(ValueError, match=r"missing upstream: \['cell_count'\]"):
            cellxgene.curate(con, listing(tmp_path / "renamed.json", renamed), "2026-11-03", LTS)
        # not an array (an API error body), and a truncated download
        err = tmp_path / "error.json"
        err.write_text('{"detail":"Internal Server Error"}\n')
        with pytest.raises(ValueError, match="not a JSON array"):
            cellxgene.curate(con, err, "2026-11-03", LTS)
        cut = tmp_path / "cut.json"
        cut.write_text(listing(tmp_path / "whole.json", [V1, V2, V3]).read_text()[:-200])
        with pytest.raises(json.JSONDecodeError):
            cellxgene.curate(con, cut, "2026-11-03", LTS, limit=1)

        assert con.execute(SNAP).fetchone()[0] == before
        assert con.execute(ROWS).fetchall() == [
            ("v1", "2026-09-18", LTS), ("v2", "2026-09-18", LTS)]
    finally:
        con.close()


def test_a_nonpositive_limit_is_refused_and_never_prunes(lake_settings, tmp_path):
    con = lake_connect(lake_settings)
    try:
        cellxgene.curate(con, listing(tmp_path / "seed.json", [V1, V2]), "2026-09-18", LTS)
        before = con.execute(SNAP).fetchone()[0]
        later = listing(tmp_path / "later.json", [V1])
        for limit in (0, -1):
            with pytest.raises(ValueError, match="limit must be positive"):
                cellxgene.curate(con, later, "2026-11-03", LTS, limit=limit)
        assert con.execute(SNAP).fetchone()[0] == before
        assert con.execute(ROWS).fetchall() == [
            ("v1", "2026-09-18", LTS), ("v2", "2026-09-18", LTS)]
    finally:
        con.close()


def test_an_empty_listing_is_refused_and_the_table_kept(lake_settings, tmp_path):
    con = lake_connect(lake_settings)
    try:
        cellxgene.curate(con, listing(tmp_path / "seed.json", [V1]), "2026-09-18", LTS)
        before = con.execute(SNAP).fetchone()[0]
        empty = listing(tmp_path / "empty.json", [])
        for limit in (None, 1):
            with pytest.raises(ValueError, match="zero rows"):
                cellxgene.curate(con, empty, "2026-11-03", LTS, limit=limit)
        assert con.execute(SNAP).fetchone()[0] == before
        assert con.execute(ROWS).fetchall() == [("v1", "2026-09-18", LTS)]
    finally:
        con.close()


def test_a_local_file_needs_a_version(lake_settings, tmp_path):
    with pytest.raises(ValueError, match="pass version"):
        cellxgene.ingest(file=str(listing(tmp_path / "d.json", [V1])), settings=lake_settings)


def test_the_default_run_downloads_todays_listing_and_stamps_the_stable_census(
        lake_settings, tmp_path, monkeypatch):
    upstream = listing(tmp_path / "upstream.json", [V1, V2])
    fetched = []

    def download(url, dest):
        fetched.append((url, dest))
        dest.parent.mkdir(parents=True, exist_ok=True)
        return Path(shutil.copy(upstream, dest))

    class Manifest:
        def raise_for_status(self):
            pass

        def json(self):
            return {"stable": LTS, "latest": "2025-11-17"}

    manifests = []
    monkeypatch.setattr(INGEST, "download", download)
    monkeypatch.setattr(INGEST.httpx, "get", lambda url, **kw: manifests.append(url) or Manifest())
    summary = cellxgene.ingest(version="2026-11-03", settings=lake_settings)
    assert (summary["version"], summary["rows"], summary["census_release"]) == (
        "2026-11-03", 2, LTS)
    assert manifests == [lake_settings.cellxgene_census_release_url]
    [(url, dest)] = fetched
    assert url == lake_settings.cellxgene_datasets_url
    # keyed by version under the source's raw dir
    assert (dest.name, dest.parent.name) == ("2026-11-03-datasets.json", "cellxgene")
