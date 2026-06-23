"""BioC-PMC — PubMed Central full text, bulk-loaded from the BioC-PMC packages.

The bulk FTP publishes one tarball per PMCID range (json-unicode = JSON content,
one BioC *collection* per article); a per-article REST API serves incrementals.
One curated table:

* ``pmc.fulltext`` — one row per ``pmcid`` with the crosswalk ids (``pmid``,
  ``doi``), ``license``, ``title``, ``n_passages``, and the **complete BioC JSON
  record** (full text, nothing lost), key ``pmcid``.

Loaded per-range to bound local disk: download tarball → stream to NDJSON →
bronze Parquet (``pmcid, record``) → curate → MERGE → delete locals → next range.
``pmid``/``doi`` make full text join the rest of the lake (iCite, grants, trials).
"""

from .ingest import curate, ingest, list_ranges, materialize_raw

__all__ = ["curate", "ingest", "list_ranges", "materialize_raw"]
