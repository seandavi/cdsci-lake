"""iCite — NIH's bibliometrics (RCR, citation counts) for all of PubMed.

Bulk-loaded from the monthly **iCite Database Snapshot** (figshare collection
``4586573``), not the per-PMID API. The curated ``lake.icite`` table is the
shared replacement for the per-project DOI→PMID / RCR enrich caches the cancer-
center layer maintains today (``cu_openalex.cancer_center.enrich``, ADR-0018):
once iCite is in the lake, RCR is a join, free for any cohort or peer (ADR-0021).
"""

from .ingest import curate, download_snapshot, ingest, resolve_latest

__all__ = ["curate", "download_snapshot", "ingest", "resolve_latest"]
