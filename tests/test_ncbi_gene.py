"""Offline tests for the NCBI Gene source + its transform models (issue #36).

The fixtures are **real bytes**: header + rows sliced verbatim out of
``gene/DATA/gene_info.gz`` (the NEWENTRY placeholders that lead the file),
``GENE_INFO/Mammalia/Homo_sapiens.gene_info.gz`` (human rows, the only ones with
populated Synonyms/dbXrefs) and ``gene/DATA/gene2ensembl.gz`` — so the column
spec, the ``-`` null marker and the ``|``/``:`` sub-delimiters are checked
against what NCBI actually ships, not against a hand-typed guess.

No network except the ``RUN_INTEGRATION``-gated test at the bottom.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from cdsci.lake import Settings, lake_connect, ops, table_exists
from cdsci.lake.sources import ncbi_gene
from cdsci.lake.transform.models import load_models
from cdsci.lake.transform.runner import run_model

FIXTURES = Path(__file__).parent / "fixtures"
MODELS = Path(__file__).parent.parent / "models"
GENE_INFO = FIXTURES / "ncbi_gene_info_sample.tsv"
GENE2ENSEMBL = FIXTURES / "ncbi_gene2ensembl_sample.tsv"
VERSION = "test-2026-08-11"


@pytest.fixture
def lake_settings(tmp_path: Path) -> Settings:
    return Settings(storage_base_uri=f"file://{tmp_path}")


@pytest.fixture
def landed(lake_settings: Settings):
    """A lake with both raw dumps landed from the real-bytes fixtures."""
    con = lake_connect(lake_settings)
    ops.register_sources(con, writer="cdsci", sources=ops.SOURCES)
    ncbi_gene.land(con, "gene_info", GENE_INFO, VERSION)
    ncbi_gene.land(con, "gene2ensembl", GENE2ENSEMBL, VERSION)
    try:
        yield con
    finally:
        con.close()


def test_land_gene_info(landed):
    # 7 rows: 2 NEWENTRY placeholders (tax 7, 9) + 5 human genes.
    assert landed.execute("SELECT count(*) FROM lake.ncbi_gene.gene_info").fetchone()[0] == 7
    assert table_exists(landed, "gene_info")

    row = landed.execute("""
        SELECT taxon_id, symbol, synonyms, dbxrefs, chromosome, type_of_gene, locus_tag
        FROM lake.ncbi_gene.gene_info WHERE gene_id = 1
    """).fetchone()
    assert row == (
        9606, "A1BG", "A1B|ABG|GAB|HYST2477",
        "MIM:138670|HGNC:HGNC:5|Ensembl:ENSG00000121410|AllianceGenome:HGNC:5",
        "19", "protein-coding",
        None,  # NCBI's '-' is the absent marker, not a literal value
    )


def test_land_gene2ensembl(landed):
    assert landed.execute("SELECT count(*) FROM lake.ncbi_gene.gene2ensembl").fetchone()[0] == 4
    row = landed.execute("""
        SELECT taxon_id, ensembl_gene_id, rna_accession, protein_accession
        FROM lake.ncbi_gene.gene2ensembl WHERE gene_id = 27215448
    """).fetchone()
    assert row == (3483, "gene-A5N79_gp34", None, "YP_009243652.1")


def test_land_is_idempotent(landed):
    """A re-land of unchanged data MERGEs to nothing — no new snapshot."""
    before = landed.execute("SELECT max(snapshot_id) FROM lake.snapshots()").fetchone()[0]
    ncbi_gene.land(landed, "gene_info", GENE_INFO, VERSION)
    after = landed.execute("SELECT max(snapshot_id) FROM lake.snapshots()").fetchone()[0]
    assert before == after


def test_land_batched_covers_every_row(lake_settings: Settings):
    """``batch=(i, n)`` shards by ``hash(gene_id)`` — every shard together == unsharded."""
    con = lake_connect(lake_settings)
    try:
        ops.register_sources(con, writer="cdsci", sources=ops.SOURCES)
        for i in range(4):
            ncbi_gene.land(con, "gene_info", GENE_INFO, VERSION, batch=(i, 4), mode="append")
        assert con.execute("SELECT count(*) FROM lake.ncbi_gene.gene_info").fetchone()[0] == 7
    finally:
        con.close()


def test_ingest_end_to_end(lake_settings: Settings):
    summary = ncbi_gene.ingest(
        dump="gene_info", file=str(GENE_INFO), version=VERSION,
        batches=1, settings=lake_settings,
    )
    assert summary["status"] == "success"
    assert summary["counts"] == {"gene_info": 7}

    con = lake_connect(lake_settings)
    try:
        lic = con.execute(
            "SELECT license FROM ops.lake_ops.source WHERE name = 'ncbi_gene'"
        ).fetchone()[0]
        assert lic == "us-public-domain"
    finally:
        con.close()


def test_ingest_rejects_file_without_dump(lake_settings: Settings):
    with pytest.raises(ValueError, match="single dump"):
        ncbi_gene.ingest(file=str(GENE_INFO), settings=lake_settings)


def _model(name: str):
    return load_models(MODELS)[f"ncbi_gene.{name}"]


def test_gene_model(landed):
    """The gene projection drops NEWENTRY and keeps each row's own taxon."""
    rows = run_model(landed, _model("gene"))
    assert rows == 5
    assert landed.execute(
        "SELECT gene_type, map_location FROM lake.ncbi_gene.gene WHERE gene_id = 11"
    ).fetchone() == ("pseudo", "8p22")


def test_mapping_model(landed):
    """The 4-way UNION ALL: Ensembl xref, symbol, pipe-split aliases, dbXrefs."""
    run_model(landed, _model("gene"))  # not a dependency, but keeps the pair coherent
    run_model(landed, _model("mapping"))

    def targets(gene_id: int, ns: str) -> set[str]:
        return {
            r[0] for r in landed.execute(
                "SELECT target_id FROM lake.ncbi_gene.mapping "
                "WHERE source_namespace = 'ENTREZ' AND source_id = ? AND target_namespace = ?",
                [str(gene_id), ns],
            ).fetchall()
        }

    assert targets(1, "SYMBOL") == {"A1BG"}
    assert targets(1, "ALIAS") == {"A1B", "ABG", "GAB", "HYST2477"}
    # dbXrefs split at the FIRST colon: HGNC's own id keeps its embedded prefix.
    assert targets(1, "HGNC") == {"HGNC:5"}
    assert targets(1, "ALLIANCEGENOME") == {"HGNC:5"}
    assert targets(1, "ENSEMBL") == {"ENSG00000121410"}
    # MIM -> OMIM, and MIM never survives as a namespace.
    assert targets(1, "OMIM") == {"138670"}
    assert targets(1, "MIM") == set()

    # gene2ensembl contributes the ENSEMBL -> ENTREZ direction, with its own taxon.
    assert landed.execute("""
        SELECT source_id, target_id, taxon_id FROM lake.ncbi_gene.mapping
        WHERE source_namespace = 'ENSEMBL' AND target_id = '27215448'
    """).fetchall() == [("gene-A5N79_gp34", "27215448", 3483)]

    # NEWENTRY placeholders contribute nothing.
    assert landed.execute(
        "SELECT count(*) FROM lake.ncbi_gene.mapping WHERE target_id = 'NEWENTRY'"
    ).fetchone()[0] == 0

    # Every row carries its own taxon, not a single scoped one.
    assert {r[0] for r in landed.execute(
        "SELECT DISTINCT taxon_id FROM lake.ncbi_gene.mapping"
    ).fetchall()} == {9606, 3483}


@pytest.mark.skipif(not os.getenv("RUN_INTEGRATION"), reason="downloads 1.5 GiB from NCBI FTP")
def test_download_dump_real(lake_settings: Settings):
    path = ncbi_gene.download_dump("gene2ensembl", VERSION, lake_settings)
    assert path.exists() and path.stat().st_size > 0
