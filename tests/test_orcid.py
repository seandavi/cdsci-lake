"""Offline tests for the ORCID source (issue #56, EL only).

The fixture is five *real* ``expanded-search`` results (raw NDJSON, exactly what
:func:`cdsci.lake.sources.orcid.fetch` writes to the raw layer), including an iD
ending in ``X``, a record with alternate names, and one with several institution
affiliations. No network — except the ``RUN_INTEGRATION`` test, which calls the
real public API.

The load being demand-driven rather than bulk is the deliberate deviation this
source is built on; ``test_orcid_refuses_to_guess_an_id_set`` is the guard that
keeps it explicit instead of silently landing nothing.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from cdsci.lake import Settings, lake_connect, ops, table_exists
from cdsci.lake.sources import orcid

FIXTURES = Path(__file__).parent / "fixtures"
SAMPLE = FIXTURES / "orcid_expanded_search_sample.ndjson"
VERSION = "test-2026-08-11"
# Real, public, long-lived iDs (ORCID's own demo record + this lake's author).
REAL_IDS = "0000-0002-1825-0097,0000-0002-8991-6458"


@pytest.fixture
def lake_settings(tmp_path: Path) -> Settings:
    return Settings(storage_base_uri=f"file://{tmp_path}")


def test_orcid_land(lake_settings: Settings):
    con = lake_connect(lake_settings)
    try:
        ops.register_sources(con, writer="cdsci", sources=ops.SOURCES)

        assert orcid.land(con, SAMPLE, VERSION) == 5
        assert table_exists(con, "person")

        # An iD's check digit can be X — it is text, never numeric.
        assert con.execute("""
            SELECT given_names, family_name, other_names
            FROM lake.orcid.person WHERE orcid_id = '0000-0003-0307-290X'
        """).fetchone() == ("Erica", "Davis", ["Erica L Davis"])

        # Affiliations are a list, not a single institution.
        assert con.execute("""
            SELECT len(institution_names) FROM lake.orcid.person
            WHERE orcid_id = '0000-0003-1723-4551'
        """).fetchone()[0] == 4

        # Empty upstream lists stay empty lists, not NULL.
        assert con.execute("""
            SELECT other_names FROM lake.orcid.person
            WHERE orcid_id = '0000-0002-0503-3894'
        """).fetchone()[0] == []

        lic = con.execute(
            "SELECT license FROM ops.lake_ops.source WHERE name = 'orcid'"
        ).fetchone()[0]
        assert lic == "cc0"

        before = con.execute("SELECT max(snapshot_id) FROM lake.snapshots()").fetchone()[0]
        orcid.land(con, SAMPLE, VERSION)
        after = con.execute("SELECT max(snapshot_id) FROM lake.snapshots()").fetchone()[0]
        assert before == after
    finally:
        con.close()


def test_orcid_ingest_end_to_end(lake_settings: Settings):
    """``ingest(file=...)`` brackets in ops.run, no API call."""
    summary = orcid.ingest(file=str(SAMPLE), version=VERSION, settings=lake_settings)
    assert summary["rows"] == 5
    assert summary["status"] == "success"


def test_orcid_refuses_to_guess_an_id_set(lake_settings: Settings):
    """No iDs given → a loud error, never an empty silent load (#56)."""
    with pytest.raises(ValueError, match="no ORCID iDs to fetch"):
        orcid.ingest(settings=lake_settings)


def test_orcid_resolve_orcids(lake_settings: Settings):
    """Both id sources: an explicit list, and a query over the attached lake."""
    con = lake_connect(lake_settings)
    try:
        assert orcid.resolve_orcids(
            con, orcids=" 0000-0002-1825-0097 , 0000-0002-8991-6458 ,", orcids_sql=None
        ) == ["0000-0002-1825-0097", "0000-0002-8991-6458"]
        # The "iDs referenced by an existing source" path: any lake query works.
        con.execute("CREATE TEMP TABLE authors AS SELECT * FROM (VALUES "
                    "('0000-0002-1825-0097'), ('0000-0002-1825-0097'), (NULL)) t(orcid_id)")
        assert orcid.resolve_orcids(
            con, orcids=None, orcids_sql="SELECT orcid_id FROM authors"
        ) == ["0000-0002-1825-0097"]
    finally:
        con.close()


@pytest.mark.skipif(not os.getenv("RUN_INTEGRATION"), reason="calls the real ORCID API")
def test_orcid_real_api(lake_settings: Settings):
    """Real bytes: batch-fetch two public iDs and land them."""
    summary = orcid.ingest(orcids=REAL_IDS, settings=lake_settings)
    assert summary["status"] == "success"
    con = lake_connect(lake_settings, read_only=True)
    try:
        rows, keys = con.execute(
            "SELECT count(*), count(DISTINCT orcid_id) FROM lake.orcid.person"
        ).fetchone()
        assert rows == keys == 2
        assert con.execute("""
            SELECT family_name FROM lake.orcid.person
            WHERE orcid_id = '0000-0002-1825-0097'
        """).fetchone()[0] == "Carberry"
    finally:
        con.close()
