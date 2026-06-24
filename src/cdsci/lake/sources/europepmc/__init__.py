"""Europe PMC text-mined annotations — PMCID↔term links across many databases.

One tidy table ``lake.europepmc.annotations`` built from the per-database CSVs at
``europepmc.org/pub/databases/pmc/TextMinedTerms/`` (uniprot, chebi, nct, gen, …),
keyed by ``(database, accession, pmcid)``. ``pmcid`` joins to ``pmc.documents``;
``pmid`` bridges to ``icite`` / ``reporter.publink`` / omicidx. See
``docs/design/europepmc.md``.
"""

from .ingest import curate, download_database, ingest, list_databases

__all__ = ["curate", "download_database", "ingest", "list_databases"]
