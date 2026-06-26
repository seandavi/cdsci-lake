"""Reliance on Science (Marx) — patent↔paper links, keyed by OpenAlex Work ID.

**CC BY-NC 4.0** — internal non-commercial use only; do **not** redistribute. The
license is carried forward in the ``lake_ops.source`` registry. Two tables:
``reliance.patent_citations`` (patents citing papers) and
``reliance.patent_paper_pairs`` (same-team matches). See ``docs/design/reliance.md``.
"""

from .ingest import DATASETS, curate, download_dataset, ingest

__all__ = ["DATASETS", "curate", "download_dataset", "ingest"]
