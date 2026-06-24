"""Retraction Watch — retraction / correction / expression-of-concern notices.

One tidy table ``lake.retractionwatch.retractions`` from the Crossref-hosted CC0
CSV, keyed ``record_id`` with multi-value fields as arrays. ``original_paper_doi``
/ ``original_paper_pmid`` join the retracted literature (icite / publink /
openalex / omicidx). See ``docs/design/retractionwatch.md``.
"""

from .ingest import curate, download_csv, ingest

__all__ = ["curate", "download_csv", "ingest"]
