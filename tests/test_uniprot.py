"""Offline tests for the UniProt ID-mapping source.

Curate the tab-delimited, headerless idmapping_selected fixture into the tidy
``uniprot.idmapping`` table: multi-valued GeneID explodes to one row per
(accession, gene_id) pair, an accession with no GeneID drops, unused columns
land raw, MERGE idempotency, and the CC BY 4.0 license carries forward in
lake_ops. No network.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from cdsci.lake import Settings, lake_connect, ops, table_exists
from cdsci.lake.sources import uniprot

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def lake_settings(tmp_path: Path) -> Settings:
    return Settings(storage_base_uri=f"file://{tmp_path}")


def test_uniprot_curate(lake_settings: Settings):
    con = lake_connect(lake_settings)
    try:
        # curate() is called directly (not via ops.run), so register the source
        # explicitly -- the substrate no longer seeds on connect (ADR-0011 §4).
        ops.register_sources(con, writer="cdsci", sources=ops.SOURCES)

        # 3 fixture rows: P04637 (1 GeneID), P01308 (2 GeneIDs, explodes to 2
        # rows), Q9UNQ0 (no GeneID, drops entirely) -> 3 pairs.
        n = uniprot.curate(
            con, FIXTURES / "uniprot_idmapping_sample.tab", "HUMAN_9606", "test-2026-08-11"
        )
        assert n == 3
        assert table_exists(con, "idmapping")

        row = con.execute("""
            SELECT gene_id, uniprotkb_id, refseq, ncbi_taxon, organism
            FROM lake.uniprot.idmapping WHERE accession = 'P04637'
        """).fetchone()
        assert row == (7157, "P53_HUMAN", "NM_000546.6", "9606", "HUMAN_9606")

        # Many-to-many: one accession, two exploded (accession, gene_id) rows.
        gene_ids = {
            r[0] for r in con.execute(
                "SELECT gene_id FROM lake.uniprot.idmapping WHERE accession = 'P01308'"
            ).fetchall()
        }
        assert gene_ids == {3630, 100000004}

        # No GeneID -> no row at all (nothing to key a mapping pair on).
        assert con.execute(
            "SELECT count(*) FROM lake.uniprot.idmapping WHERE accession = 'Q9UNQ0'"
        ).fetchone()[0] == 0

        lic = con.execute(
            "SELECT license FROM ops.lake_ops.source WHERE name = 'uniprot'"
        ).fetchone()[0]
        assert lic == "cc-by-4.0"

        # Idempotent re-run adds no snapshot.
        before = con.execute("SELECT max(snapshot_id) FROM lake.snapshots()").fetchone()[0]
        uniprot.curate(
            con, FIXTURES / "uniprot_idmapping_sample.tab", "HUMAN_9606", "test-2026-08-11"
        )
        after = con.execute("SELECT max(snapshot_id) FROM lake.snapshots()").fetchone()[0]
        assert before == after
    finally:
        con.close()


def test_uniprot_ingest_end_to_end(lake_settings: Settings):
    """``ingest(file=...)`` self-connects, brackets in ops.run, returns a summary."""
    summary = uniprot.ingest(
        organisms=["HUMAN_9606"],
        file=str(FIXTURES / "uniprot_idmapping_sample.tab"),
        version="test-2026-08-11",
        settings=lake_settings,
    )
    assert summary["rows"] == 3
    assert summary["status"] == "success"
    assert summary["counts"] == {"HUMAN_9606": 3}
