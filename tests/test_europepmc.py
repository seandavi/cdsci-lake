"""Offline tests for the Europe PMC text-mined annotations source.

Curate the same-shape per-database CSVs into one tidy ``europepmc.annotations``
table: key dedup, the ``database`` column, MED ``pmid`` extraction, multiple
databases coexisting, and MERGE idempotency. No network.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from cdsci.lake import Settings, lake_connect, ops, table_exists
from cdsci.lake.sources import europepmc

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def lake_settings(tmp_path: Path) -> Settings:
    return Settings(storage_base_uri=f"file://{tmp_path}")


def test_europepmc_curate_one_tidy_table(lake_settings: Settings):
    con = lake_connect(lake_settings)
    try:
        # curate() is called directly (not via ops.run), so register explicitly —
        # the substrate no longer seeds on connect (ADR-0011 §4).
        ops.register_sources(con, writer="cdsci", sources=ops.SOURCES)
        # 4 fixture rows, but the duplicate (MINT-1, PMC1) collapses → 3 distinct keys.
        n = europepmc.curate(con, "mint", FIXTURES / "europepmc_mint.csv", "test-2026-06")
        assert n == 3
        assert table_exists(con, "annotations")

        row = con.execute(
            "SELECT database, accession, pmcid, pmid, snapshot_version "
            "FROM lake.europepmc.annotations WHERE accession = 'MINT-1' AND pmcid = 'PMC1'"
        ).fetchone()
        assert row == ("mint", "MINT-1", "PMC1", 111, "test-2026-06")  # MED EXTID → pmid

        # A second database lands in the SAME table, namespaced by `database`.
        europepmc.curate(con, "chebi", FIXTURES / "europepmc_chebi.csv", "test-2026-06")
        dbs = {
            r[0] for r in con.execute(
                "SELECT DISTINCT database FROM lake.europepmc.annotations"
            ).fetchall()
        }
        assert dbs == {"mint", "chebi"}
        assert con.execute(
            "SELECT count(*) FROM lake.europepmc.annotations"
        ).fetchone()[0] == 5  # 3 mint + 2 chebi

        # The ops ledger has europepmc registered (explicitly, above).
        n_src = con.execute(
            "SELECT count(*) FROM ops.lake_ops.source WHERE name = 'europepmc'"
        ).fetchone()[0]
        assert n_src == 1

        # Idempotent re-run of mint adds no snapshot.
        before = con.execute("SELECT max(snapshot_id) FROM lake.snapshots()").fetchone()[0]
        europepmc.curate(con, "mint", FIXTURES / "europepmc_mint.csv", "test-2026-06")
        after = con.execute("SELECT max(snapshot_id) FROM lake.snapshots()").fetchone()[0]
        assert before == after
    finally:
        con.close()


def test_europepmc_ingest_file_path_records_run(lake_settings: Settings):
    """The end-to-end ingest(--file) path loads one database and records an ops run."""
    from cdsci.lake import ops

    summary = europepmc.ingest(
        database="mint", file=str(FIXTURES / "europepmc_mint.csv"), settings=lake_settings
    )
    assert summary["databases"] == 1
    assert summary["counts"] == {"mint": 3}
    assert summary["rows"] == 3
    assert summary["status"] == "success" and summary["run_id"]

    con = lake_connect(lake_settings)
    try:
        last = ops.last_run(con, "europepmc")
        assert last["status"] == "success"
        assert last["target"].endswith("europepmc.annotations")
    finally:
        con.close()
