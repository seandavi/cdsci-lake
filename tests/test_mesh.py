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
