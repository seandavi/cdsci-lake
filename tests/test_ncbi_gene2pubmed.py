"""Offline tests for the NCBI gene2pubmed source + its transform model (issue #38).

The fixture is **real bytes**: the header and rows sliced verbatim out of the
first 128 KiB of ``gene/DATA/gene2pubmed.gz`` (2026-08-11) — so the column spec
is checked against what NCBI actually ships. It keeps both directions of the
many-to-many on purpose: gene 310495633 with several PMIDs, and PMID 28065880
with several genes (2,525 of them in the real file).

No network except the ``RUN_INTEGRATION``-gated test at the bottom.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from cdsci.lake import Settings, lake_connect, ops
from cdsci.lake.sources import ncbi_gene, ncbi_gene2pubmed
from cdsci.lake.transform.models import load_models
from cdsci.lake.transform.runner import run_model

FIXTURES = Path(__file__).parent / "fixtures"
MODELS = Path(__file__).parent.parent / "models"
SAMPLE = FIXTURES / "ncbi_gene2pubmed_sample.tsv"
VERSION = "test-2026-08-11"
SCHEMA = "ncbi_gene2pubmed"


@pytest.fixture
def lake_settings(tmp_path: Path) -> Settings:
    return Settings(storage_base_uri=f"file://{tmp_path}")


def _land(con, **kwargs) -> int:
    """The shared ncbi_gene helper, driven by this source's own spec."""
    return ncbi_gene.land(
        con, ncbi_gene2pubmed.DUMP, SAMPLE, VERSION, schema=SCHEMA,
        columns=ncbi_gene2pubmed.COLUMNS, key=ncbi_gene2pubmed.KEY, **kwargs,
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
        "SELECT count(*) FROM lake.ncbi_gene2pubmed.gene2pubmed"
    ).fetchone()[0] == 20

    # One gene, many PMIDs — the column spec's names, not the header's.
    assert {r[0] for r in landed.execute(
        "SELECT pmid FROM lake.ncbi_gene2pubmed.gene2pubmed WHERE gene_id = 310495633"
    ).fetchall()} == {7751290, 9182530, 10799476, 11200221}

    # One PMID, many genes — the grain that keeps this out of ref.id_crosswalk.
    assert landed.execute(
        "SELECT count(DISTINCT gene_id), any_value(taxon_id) "
        "FROM lake.ncbi_gene2pubmed.gene2pubmed WHERE pmid = 28065880"
    ).fetchone() == (8, 69)


def test_land_is_idempotent(landed):
    """A re-land of unchanged data MERGEs to nothing — no new snapshot."""
    before = landed.execute("SELECT max(snapshot_id) FROM lake.snapshots()").fetchone()[0]
    _land(landed)
    after = landed.execute("SELECT max(snapshot_id) FROM lake.snapshots()").fetchone()[0]
    assert before == after


def test_land_batched_covers_every_row(lake_settings: Settings):
    """``batch=(i, n)`` shards by ``hash(gene_id)`` — the shards together == unsharded."""
    con = lake_connect(lake_settings)
    try:
        ops.register_sources(con, writer="cdsci", sources=ops.SOURCES)
        for i in range(4):
            _land(con, batch=(i, 4), mode="append")
        assert con.execute(
            "SELECT count(*) FROM lake.ncbi_gene2pubmed.gene2pubmed"
        ).fetchone()[0] == 20
    finally:
        con.close()


def test_ingest_end_to_end(lake_settings: Settings):
    summary = ncbi_gene2pubmed.ingest(
        file=str(SAMPLE), version=VERSION, batches=1, settings=lake_settings,
    )
    assert summary["status"] == "success"
    assert summary["rows"] == 20

    con = lake_connect(lake_settings)
    try:
        assert con.execute(
            "SELECT license FROM ops.lake_ops.source WHERE name = 'ncbi_gene2pubmed'"
        ).fetchone()[0] == "us-public-domain"
    finally:
        con.close()


def test_gene_publication_model(landed):
    model = load_models(MODELS)["ncbi_gene2pubmed.gene_publication"]
    assert run_model(landed, model) == 20
    assert landed.execute("""
        SELECT gene_id, taxon_id FROM lake.ncbi_gene2pubmed.gene_publication
        WHERE pmid = 15925900
    """).fetchall() == [(310495631, 23)]


@pytest.mark.skipif(not os.getenv("RUN_INTEGRATION"), reason="downloads 269 MiB from NCBI FTP")
def test_download_dump_real(lake_settings: Settings):
    path = ncbi_gene.download_dump(ncbi_gene2pubmed.DUMP, VERSION, lake_settings)
    assert path.exists() and path.stat().st_size > 0
