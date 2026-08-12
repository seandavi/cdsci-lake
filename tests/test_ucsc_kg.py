"""Offline tests for the UCSC Known Gene source (issue #55).

Curate the headerless kgXref + knownToLocusLink dumps into
``lake.ucsc.known_gene_xref``: schema-file column order, the LEFT JOIN to the
Entrez mapping (a kgID with no mapping keeps its row), empty MySQL strings →
NULL, quotes inside ``description`` surviving the tab dialect, short rows from
older assemblies, per-build keying, and MERGE idempotency. No network — except
the ``RUN_INTEGRATION`` test, which fetches the real hg38 dumps.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from cdsci.lake import Settings, lake_connect, ops, table_exists
from cdsci.lake.sources import ucsc_kg

FIXTURES = Path(__file__).parent / "fixtures"
KGXREF = FIXTURES / "ucsc_kgxref_sample.txt"
LOCUSLINK = FIXTURES / "ucsc_knowntolocuslink_sample.txt"


@pytest.fixture
def lake_settings(tmp_path: Path) -> Settings:
    return Settings(storage_base_uri=f"file://{tmp_path}")


def test_ucsc_kg_curate(lake_settings: Settings):
    con = lake_connect(lake_settings)
    try:
        # curate() is called directly (not via ops.run), so register the source
        # explicitly -- the substrate no longer seeds on connect (ADR-0011 §4).
        ops.register_sources(con, writer="cdsci", sources=ops.SOURCES)

        n = ucsc_kg.curate(con, "hg38", KGXREF, LOCUSLINK, "test-2026-08-11")
        assert n == 4  # every kgXref row lands, mapped or not
        assert table_exists(con, "known_gene_xref")

        row = con.execute("""
            SELECT gene_id, gene_symbol, sp_id, refseq, description, rfam_acc
            FROM lake.ucsc.known_gene_xref WHERE kg_id = 'ENST00000000233.10'
        """).fetchone()
        assert row == (
            381, "ARF5", "P84085", "NM_001662",
            "ARF GTPase 5 (from RefSeq NM_001662.4)", None,  # empty string -> NULL
        )

        # A double quote inside description must not eat the tab delimiter.
        assert con.execute("""
            SELECT description FROM lake.ucsc.known_gene_xref
            WHERE kg_id = 'ENST00000000412.8'
        """).fetchone()[0] == 'mannose-6-phosphate receptor, "cation dependent"'

        # No knownToLocusLink row -> kept with a NULL gene_id (land raw whole).
        assert con.execute("""
            SELECT gene_id FROM lake.ucsc.known_gene_xref
            WHERE kg_id = 'ENST00000999999.1'
        """).fetchone()[0] is None

        # Old-assembly 8-column row: trailing columns null-padded, not a parse error.
        assert con.execute("""
            SELECT gene_id, prot_acc, rfam_acc, trna_name
            FROM lake.ucsc.known_gene_xref WHERE kg_id = 'uc001aaa.3'
        """).fetchone() == (9429, "NP_004818", None, None)

        lic = con.execute(
            "SELECT license FROM ops.lake_ops.source WHERE name = 'ucsc_kg'"
        ).fetchone()[0]
        assert lic == "ucsc-free"

        # Idempotent re-run adds no snapshot.
        before = con.execute("SELECT max(snapshot_id) FROM lake.snapshots()").fetchone()[0]
        ucsc_kg.curate(con, "hg38", KGXREF, LOCUSLINK, "test-2026-08-11")
        after = con.execute("SELECT max(snapshot_id) FROM lake.snapshots()").fetchone()[0]
        assert before == after

        # Genome build is part of the key: the same kgIDs under another build
        # are new rows, not updates.
        ucsc_kg.curate(con, "hg19", KGXREF, LOCUSLINK, "test-2026-08-11")
        assert con.execute(
            "SELECT count(*) FROM lake.ucsc.known_gene_xref"
        ).fetchone()[0] == 8
    finally:
        con.close()


def test_ucsc_kg_ingest_end_to_end(lake_settings: Settings):
    """``ingest(kgxref_file=..., locuslink_file=...)`` brackets in ops.run, no network."""
    summary = ucsc_kg.ingest(
        builds="hg38",
        kgxref_file=str(KGXREF),
        locuslink_file=str(LOCUSLINK),
        version="test-2026-08-11",
        settings=lake_settings,
    )
    assert summary["rows"] == 4
    assert summary["status"] == "success"


def test_ucsc_kg_local_files_need_one_build(lake_settings: Settings):
    with pytest.raises(ValueError, match="single build"):
        ucsc_kg.ingest(
            builds="hg38,mm39",
            kgxref_file=str(KGXREF),
            locuslink_file=str(LOCUSLINK),
            settings=lake_settings,
        )


@pytest.mark.skipif(not os.getenv("RUN_INTEGRATION"), reason="downloads UCSC hg38 dumps")
def test_ucsc_kg_real_hg38(lake_settings: Settings):
    """Real bytes: the whole hg38 pair, checking the key really is unique."""
    summary = ucsc_kg.ingest(builds="hg38", settings=lake_settings)
    assert summary["status"] == "success"
    con = lake_connect(lake_settings, read_only=True)
    try:
        rows, keys = con.execute(
            "SELECT count(*), count(DISTINCT (genome_build, kg_id)) "
            "FROM lake.ucsc.known_gene_xref"
        ).fetchone()
        assert rows == keys  # (genome_build, kg_id) is unique
        assert con.execute(
            "SELECT count(*) FROM lake.ucsc.known_gene_xref WHERE gene_id IS NOT NULL"
        ).fetchone()[0] > 500_000
    finally:
        con.close()
