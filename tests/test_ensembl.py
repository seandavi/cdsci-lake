"""Offline tests for the Ensembl GTF source + its transform models (issue #35).

The fixture is a **real slice of real bytes**: four genes lifted verbatim out of
``Saccharomyces_cerevisiae.R64-1-1.63.gtf.gz`` (Ensembl release 116) — the
single-exon protein-coding case (YDL246C, YKL013C), the multi-exon + multi-CDS
case (Q0045, the mitochondrial COX1 with 8 exons), and a non-coding gene with no
CDS at all (snR58), plus the ``#!``-pragma header lines a parser must skip. So
the attribute-blob shapes, the missing ``gene_version`` in an SGD genebuild and
the ``tag "Ensembl_canonical"`` marker are Ensembl's, not something a fixture
author imagined.

Network is only touched by the ``RUN_INTEGRATION`` tests at the bottom (the FTP
listing scrape and the species-metadata lookup, the two things a fixture can't
stand in for).
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from cdsci.lake import Settings, lake_connect, ops
from cdsci.lake.sources import ensembl
from cdsci.lake.transform.models import load_models
from cdsci.lake.transform.runner import run_all

FIXTURES = Path(__file__).parent / "fixtures"
MODELS = Path(__file__).parents[1] / "transform" / "models"
SAMPLE = FIXTURES / "ensembl_sample.gtf"

# What Ensembl release 116's species_EnsemblVertebrates.txt says for yeast —
# resolved over the network by `ingest()`, passed in directly by `curate()`.
INFO = {
    "species": "saccharomyces_cerevisiae",
    "ensembl_release": "116",
    "ncbitaxon_id": 559292,
    "assembly": "R64-1-1",
    "genome_accession": "GCA_000146045.2",
}

# 35 feature lines in the fixture (5 `#!` pragma lines are comments, not data).
SAMPLE_ROWS = 35


@pytest.fixture
def lake_settings(tmp_path: Path) -> Settings:
    return Settings(storage_base_uri=f"file://{tmp_path}")


@pytest.fixture
def con(lake_settings: Settings):
    c = lake_connect(lake_settings)
    ops.register_sources(c, writer="cdsci", sources=ops.SOURCES)
    try:
        yield c
    finally:
        c.close()


def test_curate_lands_gtf_verbatim(con):
    assert ensembl.curate(con, SAMPLE, INFO) == SAMPLE_ROWS

    # The 9 GTF columns land untouched; the pragma lines don't land at all.
    row = con.execute("""
        SELECT seqname, source, feature, "start", "end", strand, attribute,
               ncbitaxon_id, ensembl_release, species, assembly, genome_accession
        FROM lake.ensembl.gtf WHERE feature = 'gene' AND attribute LIKE '%YDL246C%'
    """).fetchone()
    assert row[:6] == ("IV", "sgd", "gene", 8683, 9756, "-")
    assert row[6].startswith('gene_id "YDL246C";')
    assert row[7:] == (559292, "116", "saccharomyces_cerevisiae", "R64-1-1", "GCA_000146045.2")
    assert con.execute(
        "SELECT count(*) FROM lake.ensembl.gtf WHERE seqname LIKE '#%'"
    ).fetchone()[0] == 0

    lic = con.execute(
        "SELECT license FROM ops.lake_ops.source WHERE name = 'ensembl'"
    ).fetchone()[0]
    assert lic == "ensembl-no-restrictions"


def test_curate_skips_an_already_landed_release(con):
    """An Ensembl release is immutable — re-ingesting a landed partition writes nothing."""
    ensembl.curate(con, SAMPLE, INFO)
    before = con.execute("SELECT max(snapshot_id) FROM lake.snapshots()").fetchone()[0]
    assert ensembl.curate(con, SAMPLE, INFO) == SAMPLE_ROWS
    after = con.execute("SELECT max(snapshot_id) FROM lake.snapshots()").fetchone()[0]
    assert before == after


def test_curate_replace_rewrites_only_its_own_partition(con):
    """``replace=True`` re-lands one (taxon, release); a different taxon is untouched."""
    ensembl.curate(con, SAMPLE, INFO)
    other = {**INFO, "ncbitaxon_id": 9606, "species": "homo_sapiens"}
    ensembl.curate(con, SAMPLE, other)
    assert con.execute("SELECT count(*) FROM lake.ensembl.gtf").fetchone()[0] == 2 * SAMPLE_ROWS

    ensembl.curate(con, SAMPLE, INFO, replace=True, limit=5)
    counts = dict(con.execute(
        "SELECT ncbitaxon_id, count(*) FROM lake.ensembl.gtf GROUP BY 1"
    ).fetchall())
    assert counts == {559292: 5, 9606: SAMPLE_ROWS}


def test_models_derive_genome_gene_transcript_exon(con):
    """The real models under ``transform/models/ensembl/`` against the real GTF slice."""
    ensembl.curate(con, SAMPLE, INFO)
    models = {t: m for t, m in load_models(MODELS).items() if t.startswith("ensembl.")}
    assert set(models) == {
        "ensembl.feature", "ensembl.genome", "ensembl.gene",
        "ensembl.transcript", "ensembl.exon",
    }
    # run_all runs every model's .test.sql assertions too — a failure raises.
    rows = run_all(con, models)
    assert rows == {
        "ensembl.feature": SAMPLE_ROWS, "ensembl.genome": 1, "ensembl.gene": 4,
        "ensembl.transcript": 4, "ensembl.exon": 11,
    }

    assert con.execute("SELECT * FROM lake.ensembl.genome").fetchone() == (
        "GCA_000146045.2", 559292, "116", "saccharomyces_cerevisiae", "R64-1-1", "ENSEMBL",
    )

    # An SGD genebuild carries no gene_version — the parse must yield NULL, not ''.
    gene = con.execute("""
        SELECT version, symbol, gene_type, curation_source, sequence_name, strand
        FROM lake.ensembl.gene WHERE gene_id = 'YDL246C'
    """).fetchone()
    assert gene == (None, "SOR2", "protein_coding", "sgd", "IV", "-")

    # Every gene has exactly one Ensembl Canonical transcript (also a model test).
    assert con.execute(
        "SELECT count(*) FROM lake.ensembl.transcript WHERE canonical"
    ).fetchone()[0] == 4

    # Q0045 (COX1) is the multi-exon, multi-CDS case: 8 exons, ranks 1..8, each
    # with the CDS lying inside it — the CDS→exon self-join is what this proves.
    exons = con.execute("""
        SELECT rank, "start", "end", cds_start, cds_end, cds_phase
        FROM lake.ensembl.exon WHERE transcript_id = 'Q0045_mRNA' ORDER BY rank
    """).fetchall()
    assert [e[0] for e in exons] == list(range(1, 9))
    assert all(e[1] <= e[3] and e[4] <= e[2] and e[5] in (0, 1, 2) for e in exons)

    # snR58 is non-coding: an exon row, but no CDS line to join to.
    assert con.execute("""
        SELECT cds_start, cds_end, cds_phase FROM lake.ensembl.exon
        WHERE transcript_id = 'snR58_snoRNA'
    """).fetchall() == [(None, None, None)]


# --- Network-hitting: the two things a fixture can't stand in for ---


@pytest.mark.skipif(not os.getenv("RUN_INTEGRATION"), reason="set RUN_INTEGRATION=1")
def test_species_info_and_gtf_url_against_live_ensembl():
    """Ensembl's own release metadata + the primary-GTF pick (release 116, yeast).

    Also pins the filename gotcha the bioc-on-ice port had to fix: yeast's GTF in
    release 116 is named ``...R64-1-1.63.gtf.gz``, not ``...116.gtf.gz``.
    """
    assert ensembl.species_info(116, "saccharomyces_cerevisiae") == INFO
    url = ensembl.gtf_url(116, "saccharomyces_cerevisiae")
    assert url.endswith("Saccharomyces_cerevisiae.R64-1-1.63.gtf.gz")

    # Human has four GTF builds in the directory; exactly one is primary.
    assert ensembl.gtf_url(116, "homo_sapiens").endswith("Homo_sapiens.GRCh38.116.gtf.gz")


@pytest.mark.skipif(not os.getenv("RUN_INTEGRATION"), reason="set RUN_INTEGRATION=1")
def test_ingest_end_to_end_downloads_and_lands(lake_settings: Settings):
    """``ingest()`` self-connects, brackets in ops.run, returns a summary (yeast, ~600 KB)."""
    summary = ensembl.ingest(
        species="saccharomyces_cerevisiae", ensembl_release=116, settings=lake_settings
    )
    assert summary["status"] == "success"
    assert summary["version"] == "116:saccharomyces_cerevisiae"
    assert summary["rows"] == 41879
