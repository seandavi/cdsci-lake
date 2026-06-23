"""ClinicalTrials.gov — every registered trial, bulk-loaded from the v2 API.

Loaded as the **full JSON** record (the flat CSV export is lossy — it drops the
PMID references, results, and most structured modules). Two lake tables:

* ``ctgov.studies`` — one row per ``nct_id`` with a typed projection of the
  common fields **plus the complete record as a JSON column** (nothing lost).
* ``ctgov.references`` — the ``nct_id``↔``pmid`` crosswalk exploded from each
  study's references; the bridge from trials into the literature / grant graph.
"""

from .ingest import curate, download_studies, ingest, materialize_raw

__all__ = ["curate", "download_studies", "ingest", "materialize_raw"]
