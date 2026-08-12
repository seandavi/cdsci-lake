"""Offline tests for the Reactome source (issue #34, EL only).

Land the three headerless TSVs into ``lake.reactome.*``: the stated column spec,
padding whitespace trimmed, a bare ``"`` inside a pathway name surviving the tab
dialect, a non-Entrez ``gene_id`` accession kept rather than NULLed, evidence
code as part of the gene_pathway key, and MERGE idempotency. No network — except
the ``RUN_INTEGRATION`` test, which fetches the real files.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from cdsci.lake import Settings, lake_connect, ops, table_exists
from cdsci.lake.sources import reactome

FIXTURES = Path(__file__).parent / "fixtures"
GENE_PATHWAY = FIXTURES / "reactome_ncbi2reactome_sample.txt"
PATHWAY = FIXTURES / "reactome_pathways_sample.txt"
RELATION = FIXTURES / "reactome_pathways_relation_sample.txt"
VERSION = "test-2026-08-11"


@pytest.fixture
def lake_settings(tmp_path: Path) -> Settings:
    return Settings(storage_base_uri=f"file://{tmp_path}")


def test_reactome_land(lake_settings: Settings):
    con = lake_connect(lake_settings)
    try:
        # land() is called directly (not via ops.run), so register the source
        # explicitly -- the substrate no longer seeds on connect (ADR-0011 §4).
        ops.register_sources(con, writer="cdsci", sources=ops.SOURCES)

        assert reactome.land(con, "gene_pathway", GENE_PATHWAY, VERSION) == 5
        assert reactome.land(con, "pathway", PATHWAY, VERSION) == 3
        assert reactome.land(con, "pathway_relation", RELATION, VERSION) == 3
        assert table_exists(con, "gene_pathway")

        # Padding whitespace trimmed, URL landed whole, species is the *name*.
        assert con.execute("""
            SELECT pathway_name, pathway_url, species FROM lake.reactome.gene_pathway
            WHERE gene_id = '1' AND pathway_id = 'R-HSA-114608'
        """).fetchone() == (
            "Platelet degranulation",
            "https://reactome.org/PathwayBrowser/#/R-HSA-114608",
            "Homo sapiens",
        )

        # A bare double quote in a pathway name must not eat the tab delimiter.
        assert con.execute("""
            SELECT pathway_name FROM lake.reactome.gene_pathway
            WHERE pathway_id = 'R-MMU-109582'
        """).fetchone()[0] == 'Signaling by "NOTCH"'

        # 232 real rows carry a GenBank/RefSeq accession, not an Entrez id --
        # kept verbatim, because a NULL key column would break every MERGE.
        assert con.execute("""
            SELECT count(*) FROM lake.reactome.gene_pathway
            WHERE gene_id = 'MN908947.3'
        """).fetchone()[0] == 1

        # evidence_code is part of the key: same gene+pathway under TAS and IEA
        # are two rows, not an update.
        assert con.execute("""
            SELECT count(*) FROM lake.reactome.gene_pathway
            WHERE gene_id = '1' AND pathway_id = 'R-HSA-109582'
        """).fetchone()[0] == 2

        assert con.execute("""
            SELECT pathway_name FROM lake.reactome.pathway
            WHERE pathway_id = 'R-MMU-109582'
        """).fetchone()[0] == "Hemostasis"  # leading *and* trailing padding gone

        assert con.execute("""
            SELECT count(*) FROM lake.reactome.pathway_relation
            WHERE parent_pathway_id = 'R-HSA-109582'
        """).fetchone()[0] == 2

        lic = con.execute(
            "SELECT license FROM ops.lake_ops.source WHERE name = 'reactome'"
        ).fetchone()[0]
        assert lic == "cc0"

        # Idempotent re-run adds no snapshot.
        before = con.execute("SELECT max(snapshot_id) FROM lake.snapshots()").fetchone()[0]
        reactome.land(con, "gene_pathway", GENE_PATHWAY, VERSION)
        after = con.execute("SELECT max(snapshot_id) FROM lake.snapshots()").fetchone()[0]
        assert before == after
    finally:
        con.close()


def test_reactome_ingest_end_to_end(lake_settings: Settings):
    """``ingest(dump=..., file=...)`` brackets in ops.run, no network."""
    summary = reactome.ingest(
        dump="pathway", file=str(PATHWAY), version=VERSION, settings=lake_settings
    )
    assert summary["rows"] == 3
    assert summary["status"] == "success"
    assert summary["counts"] == {"pathway": 3}


def test_reactome_file_needs_one_dump(lake_settings: Settings):
    with pytest.raises(ValueError, match="single dump"):
        reactome.ingest(file=str(PATHWAY), settings=lake_settings)


def test_reactome_unknown_dump(lake_settings: Settings):
    with pytest.raises(ValueError, match="unknown dump"):
        reactome.ingest(dump="pathways", settings=lake_settings)


@pytest.mark.skipif(not os.getenv("RUN_INTEGRATION"), reason="downloads the real Reactome TSVs")
def test_reactome_real_files(lake_settings: Settings):
    """Real bytes: all three files, checking the keys really are unique."""
    summary = reactome.ingest(settings=lake_settings)
    assert summary["status"] == "success"
    con = lake_connect(lake_settings, read_only=True)
    try:
        for table, key in (
            ("gene_pathway", "(gene_id, pathway_id, evidence_code)"),
            ("pathway", "(pathway_id)"),
            ("pathway_relation", "(parent_pathway_id, child_pathway_id)"),
        ):
            rows, keys = con.execute(
                f"SELECT count(*), count(DISTINCT {key}) FROM lake.reactome.{table}"
            ).fetchone()
            assert rows == keys, f"{table}: {key} is not unique"
        # A Reactome stable id is species-scoped, so the issue's suggested
        # taxon_id key component is redundant (0 prefixes span two species).
        assert con.execute("""
            SELECT count(*) FROM (
                SELECT split_part(pathway_id, '-', 2) AS prefix
                FROM lake.reactome.gene_pathway
                GROUP BY 1 HAVING count(DISTINCT species) > 1
            )
        """).fetchone()[0] == 0
    finally:
        con.close()
