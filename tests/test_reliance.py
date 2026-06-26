"""Offline tests for the Reliance on Science source.

Curate both files into the ``reliance`` schema: oaid→W-form normalization, key
dedup, self/uspto booleans, patent lowercasing, the pairs table, MERGE
idempotency, and that the CC BY-NC license is carried forward in lake_ops. No
network.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from cdsci.lake import Settings, lake_connect, table_exists
from cdsci.lake.sources import reliance

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def lake_settings(tmp_path: Path) -> Settings:
    return Settings(storage_base_uri=f"file://{tmp_path}")


def test_reliance_citations_and_pairs(lake_settings: Settings):
    con = lake_connect(lake_settings)
    try:
        # citations: 5 fixture rows → 3 distinct keys (one dup collapses, blank drops).
        n = reliance.curate(con, "citations", FIXTURES / "reliance_pcs_oa.csv", "test")
        assert n == 3
        assert table_exists(con, "patent_citations")

        row = con.execute("""
            SELECT work_id, oaid, confscore, self_cite, uspto
            FROM lake.reliance.patent_citations
            WHERE patent = 'us-11426570-b2' AND reftype = 'app' AND wherefound = 'frontonly'
        """).fetchone()
        assert row == ("W1552", 1552, 10, False, True)   # oaid→W-form, max confscore, notself

        # self-citation + patent lowercased.
        self_row = con.execute("""
            SELECT patent, self_cite FROM lake.reliance.patent_citations WHERE work_id = 'W2000'
        """).fetchone()
        assert self_row == ("us-9999999-b1", True)

        # pairs: 3 rows → 2 distinct (work_id, patent).
        assert reliance.curate(con, "pairs", FIXTURES / "reliance_pairs.csv", "test") == 2
        pair = con.execute("""
            SELECT ppp_score, days_paper_to_patent, all_patents_for_paper
            FROM lake.reliance.patent_paper_pairs WHERE work_id = 'W4234301399'
        """).fetchone()
        assert pair == (2, -342, "US-10000103;US-10000104")

        # License carried forward in the ops registry.
        lic = con.execute(
            "SELECT license FROM ops.lake_ops.source WHERE name = 'reliance'"
        ).fetchone()[0]
        assert lic == "cc-by-nc-4.0"

        # Idempotent re-run of citations adds no snapshot.
        before = con.execute("SELECT max(snapshot_id) FROM lake.snapshots()").fetchone()[0]
        reliance.curate(con, "citations", FIXTURES / "reliance_pcs_oa.csv", "test")
        after = con.execute("SELECT max(snapshot_id) FROM lake.snapshots()").fetchone()[0]
        assert before == after
    finally:
        con.close()
