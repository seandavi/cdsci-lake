"""Offline tests for the NCBI gene2go source + its transform model (issue #39).

The gene2go fixture is **real bytes**: the header plus rows sliced verbatim out
of ``gene/DATA/gene2go.gz`` (two taxa, a multi-PMID row, all three GO aspects),
so the column spec and the ``|`` sub-delimiter are checked against what NCBI
actually ships.

The GO side is *not* real bytes: ``lake.ontology.terms`` is populated by the
``ontology`` source, which has no local snapshot to load in a scratch lake, so
the join target is hand-built here -- four rows, real GO curies, with the
``obsolete`` flag set on one purely to exercise the column (that term is not
actually obsolete upstream).

No network except the ``RUN_INTEGRATION``-gated test at the bottom.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from cdsci.lake import Settings, lake_connect, ops, table_exists
from cdsci.lake.sources import ncbi_gene, ncbi_gene2go
from cdsci.lake.transform.models import load_models
from cdsci.lake.transform.runner import run_model

FIXTURES = Path(__file__).parent / "fixtures"
MODELS = Path(__file__).parent.parent / "models"
GENE2GO = FIXTURES / "ncbi_gene2go_sample.tsv"
VERSION = "test-2026-08-11"

# Stand-in for what the `ontology` source lands for GO. GO:0030247 is
# deliberately absent (the "annotation to a term this GO release doesn't have"
# case) and GO:0016705 is flagged obsolete to exercise the column.
_GO_TERMS = """
    CREATE SCHEMA IF NOT EXISTS lake.ontology;
    CREATE OR REPLACE TABLE lake.ontology.terms AS
    SELECT * FROM (VALUES
        ('go', 'GO:0004449', 'isocitrate dehydrogenase (NAD+) activity', FALSE),
        ('go', 'GO:0005739', 'mitochondrion', FALSE),
        ('go', 'GO:0016705', 'oxidoreductase activity', TRUE),
        -- same curie, different ontology: must NOT join (the discriminator matters)
        ('chebi', 'GO:0030247', 'wrong ontology', FALSE)
    ) AS t(ontology, curie, label, obsolete);
"""


@pytest.fixture
def lake_settings(tmp_path: Path) -> Settings:
    return Settings(storage_base_uri=f"file://{tmp_path}")


@pytest.fixture
def landed(lake_settings: Settings):
    """A lake with raw gene2go landed from the real-bytes fixture."""
    con = lake_connect(lake_settings)
    ops.register_sources(con, writer="cdsci", sources=ops.SOURCES)
    ncbi_gene.land(
        con, "gene2go", GENE2GO, VERSION,
        schema="ncbi_gene2go", columns=ncbi_gene2go.COLUMNS, key=ncbi_gene2go.KEY,
    )
    try:
        yield con
    finally:
        con.close()


def test_land(landed):
    assert landed.execute("SELECT count(*) FROM lake.ncbi_gene2go.gene2go").fetchone()[0] == 12
    assert table_exists(landed, "gene2go")

    row = landed.execute("""
        SELECT taxon_id, gene_id, evidence, qualifier, go_term, pubmed, category
        FROM lake.ncbi_gene2go.gene2go WHERE go_id = 'GO:0006099'
    """).fetchone()
    assert row == (
        2711, 102577933, "IEA", "involved_in", "tricarboxylic acid cycle",
        "22301074|30032202",  # kept whole -- splitting PMIDs is interpretation
        "Process",
    )


def test_land_is_idempotent(landed):
    """A re-land of unchanged data MERGEs to nothing — no new snapshot.

    This is also the tripwire for NCBI starting to emit '-' in a key column:
    `nullstr='-'` would make it NULL, a NULL key never equals itself, and the
    row count would grow here.
    """
    before = landed.execute("SELECT max(snapshot_id) FROM lake.snapshots()").fetchone()[0]
    ncbi_gene.land(
        landed, "gene2go", GENE2GO, VERSION,
        schema="ncbi_gene2go", columns=ncbi_gene2go.COLUMNS, key=ncbi_gene2go.KEY,
    )
    after = landed.execute("SELECT max(snapshot_id) FROM lake.snapshots()").fetchone()[0]
    assert before == after
    assert landed.execute("SELECT count(*) FROM lake.ncbi_gene2go.gene2go").fetchone()[0] == 12


def test_ingest_end_to_end(lake_settings: Settings):
    summary = ncbi_gene2go.ingest(
        file=str(GENE2GO), version=VERSION, batches=1, settings=lake_settings
    )
    assert summary["status"] == "success"
    assert summary["rows"] == 12
    assert summary["schema"] == "ncbi_gene2go"

    con = lake_connect(lake_settings)
    try:
        # Its own ledger row, separate from ncbi_gene's (issue #39).
        assert con.execute(
            "SELECT license FROM ops.lake_ops.source WHERE name = 'ncbi_gene2go'"
        ).fetchone() == ("us-public-domain",)
        assert con.execute(
            "SELECT count(*) FROM ops.lake_ops.run WHERE source = 'ncbi_gene'"
        ).fetchone()[0] == 0
    finally:
        con.close()


def test_ingest_batched_covers_every_row(lake_settings: Settings):
    """``batches`` shards by ``hash(gene_id)`` — every shard together == unsharded."""
    summary = ncbi_gene2go.ingest(
        file=str(GENE2GO), version=VERSION, batches=4, mode="append",
        settings=lake_settings,
    )
    assert summary["rows"] == 12


def test_gene_go_model(landed):
    """The DISTINCT projection + the LEFT JOIN onto this lake's GO release."""
    landed.execute(_GO_TERMS)
    rows = run_model(landed, load_models(MODELS)["ncbi_gene2go.gene_go"])
    assert rows == 12  # nothing collapses here: no two fixture rows differ only by PMID

    def go(curie: str):
        return landed.execute(
            "SELECT DISTINCT go_term, go_label, go_obsolete FROM lake.ncbi_gene2go.gene_go "
            "WHERE go_id = ?", [curie],
        ).fetchall()

    assert go("GO:0004449") == [
        ("isocitrate dehydrogenase (NAD+) activity",
         "isocitrate dehydrogenase (NAD+) activity", False),
    ]
    # Obsolete upstream, still annotated by NCBI — the whole point of the join.
    assert go("GO:0016705")[0][2] is True
    # Absent from this lake's GO release: NULL, not FALSE, and the row survives.
    assert go("GO:0030247") == [("polysaccharide binding", None, None)]

    # pubmed is dropped by the model (still whole in raw).
    assert "pubmed" not in [
        c[0] for c in landed.execute("DESCRIBE lake.ncbi_gene2go.gene_go").fetchall()
    ]
    # Every row keeps its own taxon — the model is not species-scoped.
    assert {r[0] for r in landed.execute(
        "SELECT DISTINCT taxon_id FROM lake.ncbi_gene2go.gene_go"
    ).fetchall()} == {2711, 3483}


def test_gene_go_model_collapses_rows_differing_only_by_pubmed(landed):
    """DISTINCT is load-bearing: same annotation, two PMID lists, one row out."""
    landed.execute(_GO_TERMS)
    landed.execute("""
        INSERT INTO lake.ncbi_gene2go.gene2go
        SELECT * REPLACE ('99999999' AS pubmed)
        FROM lake.ncbi_gene2go.gene2go WHERE go_id = 'GO:0006099'
    """)
    assert run_model(landed, load_models(MODELS)["ncbi_gene2go.gene_go"]) == 12


@pytest.mark.skipif(not os.getenv("RUN_INTEGRATION"), reason="downloads 1.37 GiB from NCBI FTP")
def test_download_dump_real(lake_settings: Settings):
    path = ncbi_gene.download_dump("gene2go", VERSION, lake_settings)
    assert path.exists() and path.stat().st_size > 0
