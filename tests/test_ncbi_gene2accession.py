"""Offline tests for the NCBI gene2accession source + its transform model (issue #37).

The fixture is **real bytes**: rows sliced verbatim out of the real
``gene/DATA/gene2accession.gz`` (2026-08-11) — the bacterial head via an HTTP
byte-range request (partial tail line dropped), the human and PDB rows from full
streaming passes. It is deliberately diverse where the discriminator lives:
RefSeq RNA (``NM_``/``XM_``), RefSeq protein (``NP_``/``XP_``/``WP_``), RefSeq
genomic (``NC_``/``NG_``/``NZ_``), GenBank genomic contigs, GenBank RNA/protein,
a bare Swiss-Prot accession (``P04217.4``) sitting in the protein column, and —
the case that breaks bioc-on-ice's ported rule — PDB *chain* accessions
(``3SID_A.1``, ``6Y3D_aA.1``), which contain an underscore and are not RefSeq.

No network except the ``RUN_INTEGRATION``-gated test at the bottom.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from cdsci.lake import Settings, lake_connect, ops
from cdsci.lake.sources import ncbi_gene, ncbi_gene2accession
from cdsci.lake.transform.models import load_models
from cdsci.lake.transform.runner import run_model

FIXTURES = Path(__file__).parent / "fixtures"
MODELS = Path(__file__).parent.parent / "transform" / "models"
SAMPLE = FIXTURES / "ncbi_gene2accession_sample.tsv"
VERSION = "test-2026-08-11"
SCHEMA = "ncbi_gene2accession"
ROWS = 36


@pytest.fixture
def lake_settings(tmp_path: Path) -> Settings:
    return Settings(storage_base_uri=f"file://{tmp_path}")


def _land(con, **kwargs) -> int:
    """The shared ncbi_gene helper, driven by this source's own spec."""
    return ncbi_gene.land(
        con, ncbi_gene2accession.DUMP, SAMPLE, VERSION, schema=SCHEMA,
        columns=ncbi_gene2accession.COLUMNS, key=ncbi_gene2accession.KEY, **kwargs,
    )


@pytest.fixture
def landed(lake_settings: Settings):
    con = lake_connect(lake_settings)
    ops.register_sources(con, writer="cdsci", sources=ops.SOURCES)
    _land(con)
    try:
        yield con
    finally:
        con.close()


def test_land(landed):
    assert landed.execute(
        "SELECT count(*) FROM lake.ncbi_gene2accession.gene2accession"
    ).fetchone()[0] == ROWS

    # The column spec's names and types, not the header's -- '-' is NULL, the
    # version suffix is kept, and coordinates/GIs arrive as integers.
    assert landed.execute("""
        SELECT rna_accession, protein_accession, genomic_accession, start_position,
               end_position, assembly, rna_gi, mature_peptide_accession
        FROM lake.ncbi_gene2accession.gene2accession
        WHERE gene_id = 1 AND status = 'REVIEWED' AND genomic_accession = 'NC_000019.10'
    """).fetchall() == [(
        "NM_130786.4", "NP_570602.2", "NC_000019.10", 58345182, 58353491,
        "Reference GRCh38.p14 Primary Assembly", 1653960645, None,
    )]

    # Why the key needs the genomic accession: one transcript, three placements.
    assert landed.execute("""
        SELECT count(*) FROM lake.ncbi_gene2accession.gene2accession
        WHERE rna_accession = 'NM_000014.6'
    """).fetchone()[0] == 3


def test_orientation_sentinel_collision(landed):
    """`nullstr='-'` eats minus-strand: NULL orientation means '-', never "absent".

    The family-wide ``nullstr='-'`` collides with this dump's own minus-strand
    marker. It is recoverable rather than lost because NCBI writes '?' (not '-')
    when orientation is unknown — verified on real bytes: across 2.4M bacterial
    rows and the human rows in this fixture the column is only ever '+', '-' or
    '?'. So ``coalesce(orientation, '-')`` reconstructs the file value exactly.
    This test pins that premise: if a genuinely-empty orientation ever appears,
    the reconstruction stops being sound and this fails.
    """
    assert landed.execute("""
        SELECT DISTINCT coalesce(orientation, '-')
        FROM lake.ncbi_gene2accession.gene2accession
        WHERE genomic_accession IS NOT NULL
        ORDER BY 1
    """).fetchall() == [("+",), ("-",), ("?",)]
    assert landed.execute("""
        SELECT count(*) FROM lake.ncbi_gene2accession.gene2accession
        WHERE orientation IS NULL AND genomic_accession IS NOT NULL
    """).fetchone()[0] > 0


def test_key_is_unique_on_real_rows(landed):
    """The declared KEY holds the grain -- if it didn't, MERGE would collapse rows."""
    key = ", ".join(ncbi_gene2accession.KEY)
    assert landed.execute(f"""
        SELECT count(*) FROM (
            SELECT {key} FROM lake.ncbi_gene2accession.gene2accession
            GROUP BY ALL HAVING count(*) > 1
        )
    """).fetchone()[0] == 0


def test_land_is_idempotent(landed):
    """A re-land of unchanged data MERGEs to nothing — no new snapshot."""
    before = landed.execute("SELECT max(snapshot_id) FROM lake.snapshots()").fetchone()[0]
    _land(landed)
    after = landed.execute("SELECT max(snapshot_id) FROM lake.snapshots()").fetchone()[0]
    assert before == after


def test_land_batched_covers_every_row(lake_settings: Settings):
    """``batch=(i, n)`` shards by ``hash(gene_id)`` — the shards together == unsharded.

    This is the lever the ~300M-row full load depends on, so it is asserted on
    real rows rather than assumed.
    """
    con = lake_connect(lake_settings)
    try:
        ops.register_sources(con, writer="cdsci", sources=ops.SOURCES)
        for i in range(4):
            _land(con, batch=(i, 4), mode="append")
        assert con.execute(
            "SELECT count(*) FROM lake.ncbi_gene2accession.gene2accession"
        ).fetchone()[0] == ROWS
    finally:
        con.close()


def test_ingest_end_to_end(lake_settings: Settings):
    summary = ncbi_gene2accession.ingest(
        file=str(SAMPLE), version=VERSION, batches=1, settings=lake_settings,
    )
    assert summary["status"] == "success"
    assert summary["rows"] == ROWS

    con = lake_connect(lake_settings)
    try:
        assert con.execute(
            "SELECT license FROM ops.lake_ops.source WHERE name = 'ncbi_gene2accession'"
        ).fetchone()[0] == "us-public-domain"
    finally:
        con.close()


def test_mapping_model(landed):
    """The ported 3-way UNION ALL, including its declared model tests."""
    model = load_models(MODELS)["ncbi_gene2accession.mapping"]
    assert run_model(landed, model) == 26  # run_model runs mapping.test.sql too

    counts = dict(landed.execute("""
        SELECT target_namespace, count(*) FROM lake.ncbi_gene2accession.mapping
        GROUP BY 1
    """).fetchall())
    assert counts == {"REFSEQ_RNA": 6, "REFSEQ_PROTEIN": 10, "GENBANK_GENOMIC": 10}

    # The version suffix survives, and taxon comes from the row.
    assert landed.execute("""
        SELECT target_id, taxon_id FROM lake.ncbi_gene2accession.mapping
        WHERE source_id = '1' AND target_namespace = 'REFSEQ_RNA'
    """).fetchall() == [("NM_130786.4", 9606)]


def test_pdb_chain_accessions_are_not_refseq(landed):
    """bioc-on-ice's `contains(protein, '_')` called PDB chains RefSeq. This is the fix.

    ``3SID_A.1`` / ``6Y3D_aA.1`` are PDB chain accessions that NCBI really does
    put in gene2accession's protein column — 4,818 rows over 931 PDB entries in
    the full 284.8M-row dump. They match "has an underscore" but are not RefSeq,
    so the model keys on the RefSeq *shape* (two letters, then '_') instead.
    """
    assert landed.execute("""
        SELECT count(*) FROM lake.ncbi_gene2accession.gene2accession
        WHERE protein_accession IN ('3SID_A.1', '6Y3D_aA.1')
    """).fetchone()[0] == 2

    run_model(landed, load_models(MODELS)["ncbi_gene2accession.mapping"])
    assert landed.execute("""
        SELECT count(*) FROM lake.ncbi_gene2accession.mapping
        WHERE contains(target_id, 'SID_') OR contains(target_id, 'Y3D_')
    """).fetchone()[0] == 0


def test_mapping_discriminator_drops_what_it_should(landed):
    """The asymmetry the model docstring calls out, pinned on real rows.

    GenBank RNA/protein and RefSeq genomic are present in the fixture and are
    deliberately NOT emitted — if a future edit starts emitting them, this fails
    and the decision gets made on purpose rather than by accident.
    """
    run_model(landed, load_models(MODELS)["ncbi_gene2accession.mapping"])
    emitted = {r[0] for r in landed.execute(
        "SELECT target_id FROM lake.ncbi_gene2accession.mapping"
    ).fetchall()}

    # present in raw...
    assert landed.execute("""
        SELECT count(*) FROM lake.ncbi_gene2accession.gene2accession
        WHERE rna_accession = 'AB073611.1' OR protein_accession = 'P04217.4'
           OR genomic_accession = 'NZ_MCBT01000001.1'
    """).fetchone()[0] > 0
    # ...and absent from the mapping
    assert emitted.isdisjoint({"AB073611.1", "P04217.4", "NZ_MCBT01000001.1", "NG_011717.2"})


@pytest.mark.skipif(not os.getenv("RUN_INTEGRATION"), reason="downloads 4.3 GiB from NCBI FTP")
def test_download_dump_real(lake_settings: Settings):
    path = ncbi_gene.download_dump(ncbi_gene2accession.DUMP, VERSION, lake_settings)
    assert path.exists() and path.stat().st_size > 0
