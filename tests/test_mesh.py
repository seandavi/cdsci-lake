"""Offline tests for the MeSH ingestor (ADR-0010).

Parse the descriptor + qualifier XML fixtures → bronze Parquet → curate the five
silver tables, asserting the vocabulary shape and — the point of MeSH — the tree
hierarchy (parent derivation + prefix rollup). No network.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from cdsci.lake import Settings, lake_connect
from cdsci.lake.sources import mesh

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def lake_settings(tmp_path: Path) -> Settings:
    return Settings(storage_base_uri=f"file://{tmp_path}")


def test_mesh_curate_vocabulary_and_hierarchy(lake_settings: Settings, tmp_path: Path):
    desc_nd = tmp_path / "desc.ndjson.gz"
    qual_nd = tmp_path / "qual.ndjson.gz"
    assert mesh.descriptors_to_ndjson(FIXTURES / "mesh_desc_sample.xml", desc_nd) == 2
    assert mesh.qualifiers_to_ndjson(FIXTURES / "mesh_qual_sample.xml", qual_nd) == 2

    con = lake_connect(lake_settings)
    try:
        desc_raw = mesh.materialize_raw(con, desc_nd)
        qual_raw = mesh.materialize_raw(con, qual_nd)
        counts = mesh.curate(con, desc_raw, qual_raw, version="2026")
        assert counts == {
            "descriptor": 2,
            "tree": 3,  # D03.633.100.221.173 + C04 + C04.557
            "qualifier": 2,
            "descriptor_qualifier": 2,
            "entry_term": 4,  # two terms per descriptor
        }

        # The hierarchy: parent of C04.557 is C04; C04 is top-level (NULL parent).
        assert con.execute(
            "SELECT parent_tree_number FROM lake.mesh.tree WHERE tree_number = 'C04.557'"
        ).fetchone()[0] == "C04"
        assert con.execute(
            "SELECT parent_tree_number FROM lake.mesh.tree WHERE tree_number = 'C04'"
        ).fetchone()[0] is None
        # "Everything under Neoplasms (C04)" is a prefix query — both tree rows match.
        assert con.execute(
            "SELECT count(*) FROM lake.mesh.tree WHERE tree_number LIKE 'C04%'"
        ).fetchone()[0] == 2

        # Descriptor name + scope note carried through.
        name, scope = con.execute(
            "SELECT name, scope_note FROM lake.mesh.descriptor WHERE descriptor_ui = 'D009369'"
        ).fetchone()
        assert name == "Neoplasms" and scope.startswith("New abnormal growth")

        # Entry terms: the descriptor name is preferred, the synonym is not.
        assert con.execute(
            "SELECT is_preferred FROM lake.mesh.entry_term WHERE term = 'Neoplasms'"
        ).fetchone()[0] is True
        assert con.execute(
            "SELECT is_preferred FROM lake.mesh.entry_term WHERE term = 'Tumors'"
        ).fetchone()[0] is False

        # Allowable-qualifier bridge + the qualifier dimension.
        assert con.execute(
            "SELECT qualifier_ui FROM lake.mesh.descriptor_qualifier "
            "WHERE descriptor_ui = 'D009369'"
        ).fetchone()[0] == "Q000235"
        assert con.execute(
            "SELECT name FROM lake.mesh.qualifier WHERE qualifier_ui = 'Q000008'"
        ).fetchone()[0] == "administration & dosage"
    finally:
        con.close()


def test_mesh_article_headings_explosion(lake_settings: Settings):
    """Phase 1b: explode omicidx mesh_terms → (pmid, descriptor_ui, qualifier_ui)."""
    con = lake_connect(lake_settings)
    try:
        # stand up a tiny in-lake omicidx.pubmed_article (pmid VARCHAR, like production).
        con.execute("CREATE SCHEMA lake.omicidx;")
        con.execute(
            "CREATE TABLE lake.omicidx.pubmed_article AS SELECT * FROM (VALUES "
            "('100', 'D006801:Humans; D015820:Cadherins / Q000378:metabolism / Q000235:genetics'), "
            "('200', 'D000328:Adult; D000653:Amniotic Fluid / Q000737:chemistry'), "
            "('300', NULL)) t(pmid, mesh_terms);"
        )
        n = mesh.curate_article_headings(con, version="2026-06")
        # 100: Humans=1 + Cadherins×2 quals=2 → 3 ; 200: Adult=1 + Amniotic/chem=1 → 2 ; 300 skip
        assert n == 5
        got = {
            tuple(r) for r in con.execute(
                "SELECT pmid, descriptor_ui, qualifier_ui FROM lake.mesh.article_heading"
            ).fetchall()
        }
        assert got == {
            (100, "D006801", ""),
            (100, "D015820", "Q000378"),
            (100, "D015820", "Q000235"),
            (200, "D000328", ""),
            (200, "D000653", "Q000737"),
        }
        # pmid normalized to BIGINT (joins icite/publink); no-qualifier sentinel is '', not NULL.
        assert con.execute(
            "SELECT count(*) FROM lake.mesh.article_heading WHERE qualifier_ui IS NULL"
        ).fetchone()[0] == 0
        # idempotent re-run adds no snapshot.
        before = con.execute("SELECT max(snapshot_id) FROM lake.snapshots()").fetchone()[0]
        mesh.curate_article_headings(con, version="2026-06")
        after = con.execute("SELECT max(snapshot_id) FROM lake.snapshots()").fetchone()[0]
        assert after == before
    finally:
        con.close()


def test_mesh_article_chemicals_explosion(lake_settings: Settings):
    """Literature edge: explode omicidx chemical_list → (pmid, substance_ui)."""
    con = lake_connect(lake_settings)
    try:
        con.execute("CREATE SCHEMA lake.omicidx;")
        con.execute(
            "CREATE TABLE lake.omicidx.pubmed_article AS SELECT * FROM (VALUES "
            "('1', 'D011494:Protein Kinases; D000098984:Fluorescent Cmpd; C000005:rofecoxib'), "
            "('2', 'D017382:Reactive Oxygen Species'), "
            "('3', NULL)) t(pmid, chemical_list);"
        )
        n = mesh.curate_article_chemicals(con, version="2026-06")
        assert n == 4  # pmid1: 3 substances ; pmid2: 1 ; pmid3 skipped
        got = {
            tuple(r) for r in con.execute(
                "SELECT pmid, substance_ui FROM lake.mesh.article_chemical"
            ).fetchall()
        }
        assert got == {
            (1, "D011494"),
            (1, "D000098984"),  # long (9-digit) descriptor UI
            (1, "C000005"),  # an SCR substance
            (2, "D017382"),
        }
        # pmid is BIGINT; idempotent re-run adds no snapshot.
        before = con.execute("SELECT max(snapshot_id) FROM lake.snapshots()").fetchone()[0]
        mesh.curate_article_chemicals(con, version="2026-06")
        after = con.execute("SELECT max(snapshot_id) FROM lake.snapshots()").fetchone()[0]
        assert after == before
    finally:
        con.close()


def test_mesh_supplemental_records(lake_settings: Settings, tmp_path: Path):
    """Phase 2 SCRs: record + heading-mapped-to bridge (with primary flag) + terms."""
    nd = tmp_path / "supp.ndjson.gz"
    assert mesh.supplemental_to_ndjson(FIXTURES / "mesh_supp_sample.xml", nd) == 2
    con = lake_connect(lake_settings)
    try:
        raw = mesh.materialize_raw(con, nd)
        counts = mesh.curate_supplemental(con, raw, version="2026")
        assert counts == {
            "supplemental": 2,
            "supplemental_descriptor": 3,  # bevonium→2 descriptors, rofecoxib→1
            "supplemental_term": 4,  # two terms each
        }
        # SCR record: class + registry number ('0' normalized to NULL).
        cls, reg = con.execute(
            "SELECT scr_class, registry_number FROM lake.mesh.supplemental "
            "WHERE supplemental_ui = 'C000002'"
        ).fetchone()
        assert cls == "1" and reg == "34B0471E08"
        assert con.execute(
            "SELECT registry_number FROM lake.mesh.supplemental WHERE supplemental_ui = 'C000005'"
        ).fetchone()[0] is None
        # Heading-mapped-to: '*' marks the primary descriptor, UI stripped of the '*'.
        prim = con.execute(
            "SELECT descriptor_ui, is_primary FROM lake.mesh.supplemental_descriptor "
            "WHERE supplemental_ui = 'C000005'"
        ).fetchone()
        assert prim == ("D052246", True)
        assert con.execute(
            "SELECT is_primary FROM lake.mesh.supplemental_descriptor "
            "WHERE supplemental_ui = 'C000002' AND descriptor_ui = 'D010879'"
        ).fetchone()[0] is False
    finally:
        con.close()


def test_mesh_pharmacological_actions(lake_settings: Settings, tmp_path: Path):
    """Phase 2 PAs: substance ↔ action-descriptor edge (descriptors and SCRs as substances)."""
    nd = tmp_path / "pa.ndjson.gz"
    assert mesh.pharmacological_actions_to_ndjson(FIXTURES / "mesh_pa_sample.xml", nd) == 2
    con = lake_connect(lake_settings)
    try:
        raw = mesh.materialize_raw(con, nd)
        # Aspirin: 2 actions; rofecoxib: 1 action → 3 edges.
        assert mesh.curate_pharmacological_actions(con, raw, version="2026") == 3
        got = {
            tuple(r) for r in con.execute(
                "SELECT substance_ui, action_ui FROM lake.mesh.pharmacological_action"
            ).fetchall()
        }
        assert got == {
            ("D001241", "D000894"),  # Aspirin → NSAID
            ("D001241", "D058633"),  # Aspirin → Antipyretic
            ("C000005", "D000894"),  # rofecoxib (SCR) → NSAID
        }
    finally:
        con.close()


def test_mesh_curate_idempotent(lake_settings: Settings, tmp_path: Path):
    """A re-curate of identical data is a no-op (adds no snapshot)."""
    desc_nd = tmp_path / "desc.ndjson.gz"
    qual_nd = tmp_path / "qual.ndjson.gz"
    mesh.descriptors_to_ndjson(FIXTURES / "mesh_desc_sample.xml", desc_nd)
    mesh.qualifiers_to_ndjson(FIXTURES / "mesh_qual_sample.xml", qual_nd)
    con = lake_connect(lake_settings)
    try:
        desc_raw = mesh.materialize_raw(con, desc_nd)
        qual_raw = mesh.materialize_raw(con, qual_nd)
        mesh.curate(con, desc_raw, qual_raw, version="2026")
        before = con.execute("SELECT max(snapshot_id) FROM lake.snapshots()").fetchone()[0]
        mesh.curate(con, desc_raw, qual_raw, version="2026")  # identical re-run
        after = con.execute("SELECT max(snapshot_id) FROM lake.snapshots()").fetchone()[0]
        assert after == before
    finally:
        con.close()
