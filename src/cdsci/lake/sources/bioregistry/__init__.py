"""Bioregistry — canonical identifier prefixes, patterns, and synonyms.

One table, ``lake.ref.bioregistry``, keyed ``prefix``, from the CC0 consensus
TSV export (github.com/biopragmatics/bioregistry). Backs this lake's own
identifier-column naming convention (2026-08-10 session) as a queryable
table instead of a manual bioregistry.io lookup.
"""

from .ingest import curate, download_registry, ingest

__all__ = ["curate", "download_registry", "ingest"]
