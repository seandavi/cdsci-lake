"""Offline tests for the ROR source (issue #57, EL only).

The fixture is six *real* records sliced out of the v2.11 dump, each chosen for
a property the load has to get right: a bare ``"`` in an organization name, an
alternate name with padding whitespace, three locations on one record, a
withdrawn status, and a NULL ``established``. No network — except the
``RUN_INTEGRATION`` test, which fetches the real 135k-record dump.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from cdsci.lake import Settings, lake_connect, ops, table_exists
from cdsci.lake.sources import ror
from cdsci.lake.sources.ror.ingest import _version_from_name

FIXTURES = Path(__file__).parent / "fixtures"
SAMPLE = FIXTURES / "ror_sample.json"
VERSION = "v2.11"


@pytest.fixture
def lake_settings(tmp_path: Path) -> Settings:
    return Settings(storage_base_uri=f"file://{tmp_path}")


def test_ror_land(lake_settings: Settings):
    con = lake_connect(lake_settings)
    try:
        # land() is called directly (not via ops.run), so register explicitly.
        ops.register_sources(con, writer="cdsci", sources=ops.SOURCES)

        assert ror.land(con, SAMPLE, VERSION) == 6
        assert table_exists(con, "organization")

        # ror_id is the bare LUI, not the URI (#46's id-column convention), and
        # `name` is the single ror_display name.
        assert con.execute("""
            SELECT ror_id, name, status, established
            FROM lake.ror.organization WHERE ror_id = '04ttjf776'
        """).fetchone() == ("04ttjf776", "RMIT University", "active", 1887)

        # 804 real names carry a bare double quote — JSON quotes its own strings,
        # so nothing mangles them (the reason the CSV subset isn't landed).
        assert con.execute("""
            SELECT name FROM lake.ror.organization WHERE ror_id = '03tf96d34'
        """).fetchone()[0] == 'Azienda Ospedaliera Universitaria Policlinico "G. Martino"'

        # The raw names list lands verbatim — padding included. (Trimming happens
        # on the derived `name` only; no ror_display name is padded upstream.)
        assert con.execute("""
            SELECT list_filter(names, n -> list_contains(n.types, 'acronym'))[1].value
            FROM lake.ror.organization WHERE ror_id = '05re2b915'
        """).fetchone()[0] == "ERTICO "

        # Country is deliberately not derived: this record has three locations.
        assert con.execute("""
            SELECT len(locations) FROM lake.ror.organization WHERE ror_id = '05qhvy459'
        """).fetchone()[0] == 3

        # Withdrawn records still land — a retired ROR id must resolve for
        # historical affiliation data.
        assert con.execute("""
            SELECT status FROM lake.ror.organization WHERE ror_id = '058mseb02'
        """).fetchone()[0] == "withdrawn"

        # established is genuinely absent for 26,434 records; NULL, not 0.
        assert con.execute("""
            SELECT established FROM lake.ror.organization WHERE ror_id = '050b31k83'
        """).fetchone()[0] is None

        # Nested LIST<STRUCT> round-trips through DuckLake.
        assert con.execute("""
            SELECT list_filter(external_ids, e -> e."type" = 'grid')[1].preferred
            FROM lake.ror.organization WHERE ror_id = '04ttjf776'
        """).fetchone()[0] == "grid.1017.7"
        assert con.execute("""
            SELECT count(*) FROM (
                SELECT unnest(relationships) r FROM lake.ror.organization
                WHERE ror_id = '04ttjf776'
            ) WHERE r."type" = 'child'
        """).fetchone()[0] == 3

        lic = con.execute(
            "SELECT license FROM ops.lake_ops.source WHERE name = 'ror'"
        ).fetchone()[0]
        assert lic == "cc0"

        # Idempotent re-run adds no snapshot (nested columns compare cleanly).
        before = con.execute("SELECT max(snapshot_id) FROM lake.snapshots()").fetchone()[0]
        ror.land(con, SAMPLE, VERSION)
        after = con.execute("SELECT max(snapshot_id) FROM lake.snapshots()").fetchone()[0]
        assert before == after
    finally:
        con.close()


def test_ror_ingest_end_to_end(lake_settings: Settings):
    """``ingest(file=...)`` brackets in ops.run, no network."""
    summary = ror.ingest(file=str(SAMPLE), version=VERSION, settings=lake_settings)
    assert summary["rows"] == 6
    assert summary["status"] == "success"
    assert summary["version"] == VERSION


def test_ror_version_comes_from_the_dump():
    """A ROR release names itself — the snapshot is not tagged by pull date."""
    assert _version_from_name(Path("v2.11-2026-08-03-ror-data.json")) == "v2.11"
    with pytest.raises(ValueError, match="release version"):
        _version_from_name(Path("ror_sample.json"))


def test_ror_key_drift_fails_loudly(lake_settings: Settings, tmp_path: Path):
    """A renamed upstream key would otherwise land as a silent all-NULL column."""
    records = json.loads(SAMPLE.read_text())
    for record in records:
        record["organisation_names"] = record.pop("names")
    drifted = tmp_path / "drifted.json"
    drifted.write_text(json.dumps(records))
    con = lake_connect(lake_settings)
    try:
        with pytest.raises(ValueError, match="keys drifted"):
            ror.land(con, drifted, VERSION)
    finally:
        con.close()


@pytest.mark.skipif(not os.getenv("RUN_INTEGRATION"), reason="downloads the real ROR dump")
def test_ror_real_dump(lake_settings: Settings):
    """Real bytes: the whole current dump, checking ror_id really is unique."""
    summary = ror.ingest(settings=lake_settings)
    assert summary["status"] == "success"
    con = lake_connect(lake_settings, read_only=True)
    try:
        rows, keys, named = con.execute(
            "SELECT count(*), count(DISTINCT ror_id), count(name) "
            "FROM lake.ror.organization"
        ).fetchone()
        assert rows == keys, "ror_id is not unique"
        assert named == rows, "every record has exactly one ror_display name"
        assert rows > 130_000
    finally:
        con.close()
