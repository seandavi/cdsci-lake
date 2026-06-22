"""Offline tests for the ``cri`` lake substrate and the iCite / RePORTER curates.

These exercise the full **curate-into-DuckLake** mechanics against small CSV
fixtures (no network): attach a temp lake, build the table, assert typing,
casting, row-dropping, and that a snapshot was recorded. Live bulk fetches are
covered by an opt-in integration test (``RUN_INTEGRATION=1``).
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from cri.config import Settings
from cri.lake import csv_source, lake_connect, snapshots, table_exists
from cri.sources import icite, reporter
from cri.sources.icite.ingest import _pick_metadata_file

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def lake_settings(tmp_path: Path) -> Settings:
    """A Settings pointing the whole lake (catalog + data) into a temp dir."""
    return Settings(storage_base_uri=f"file://{tmp_path}")


def test_csv_source_rendering():
    assert csv_source("a/*.csv") == "'a/*.csv'"
    assert csv_source([Path("/x/a.csv"), Path("/x/b.csv")]) == "['/x/a.csv', '/x/b.csv']"


def test_icite_curate(lake_settings: Settings):
    con = lake_connect(lake_settings)
    try:
        rows = icite.curate(con, [FIXTURES / "icite_sample.csv"], "test-2026-06")
        # 4 rows in the fixture; the non-numeric pmid row is dropped.
        assert rows == 3
        assert table_exists(con, "icite")

        r = con.execute(
            "SELECT pmid, doi, rcr, citation_count, is_research_article, snapshot_version "
            "FROM lake.icite WHERE pmid = 23456789"
        ).fetchone()
        pmid, doi, rcr, citations, is_article, version = r
        assert pmid == 23456789
        assert doi == "10.1002/cncr.27976"  # lowercased
        assert rcr == pytest.approx(2.51)
        assert citations == 142
        assert is_article is True
        assert version == "test-2026-06"

        # Empty DOI becomes NULL, not "".
        null_doi = con.execute("SELECT doi FROM lake.icite WHERE pmid = 30000002").fetchone()[0]
        assert null_doi is None

        # A snapshot was committed to the catalog (time-travel / versioning).
        assert len(snapshots(con)) >= 1
    finally:
        con.close()


def test_icite_curate_limit(lake_settings: Settings):
    con = lake_connect(lake_settings)
    try:
        rows = icite.curate(con, [FIXTURES / "icite_sample.csv"], "test", limit=1)
        assert rows == 1
    finally:
        con.close()


def test_reporter_curate(lake_settings: Settings):
    con = lake_connect(lake_settings)
    try:
        rows = reporter.curate(con, [FIXTURES / "reporter_sample.csv"])
        assert rows == 2  # the 'badid' row drops on non-numeric appl_id

        r = con.execute(
            "SELECT core_project_num, fiscal_year, total_cost, org_name, pi_names "
            "FROM lake.reporter_projects WHERE appl_id = 10617263"
        ).fetchone()
        core, fy, total, org, pis = r
        assert core == "5R01CA123456"
        assert fy == 2023
        assert total == pytest.approx(750000.0)
        assert "COLORADO" in org.upper()
        assert "DAVIS, SEAN" in pis  # PI_NAMEs header read case-insensitively
    finally:
        con.close()


def test_pick_metadata_file_prefers_metadata_archive():
    files = [
        {"name": "readme.txt", "download_url": "u0"},
        {"name": "open_citation_collection.zip", "download_url": "u1"},
        {"name": "icite_metadata.zip", "download_url": "u2"},
    ]
    assert _pick_metadata_file(files)["download_url"] == "u2"


@pytest.mark.skipif(not os.getenv("RUN_INTEGRATION"), reason="set RUN_INTEGRATION=1")
def test_icite_resolve_latest_live():
    info = icite.resolve_latest()
    assert info["version"]
    assert info["files"]
