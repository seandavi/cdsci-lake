"""BioC-PMC — PubMed Central full text, bulk-loaded from the BioC-PMC packages.

The bulk FTP publishes one tarball per PMCID range (json-unicode = JSON content,
one BioC *collection* per article); a per-article REST API serves incrementals.
Two curated tables in a one-to-many relationship:

* ``pmc.documents`` — one row per ``pmcid`` with the crosswalk ids (``pmid``,
  ``doi``), ``license``, ``title``, and ``n_passages``. Key ``pmcid``.
* ``pmc.passages`` — one row per BioC passage (the full text, exploded out of the
  nested record): ``passage_index``, ``passage_offset``, ``section_type``,
  ``passage_type``, ``text``, and the full passage ``infons`` JSON. Key
  ``(pmcid, passage_index)`` → ``documents.pmcid``.

Loaded per-range to bound local disk: download tarball → stream to NDJSON →
bronze Parquet (``pmcid, record``) → curate both tables → MERGE → delete locals →
next range. ``pmid``/``doi`` make full text join the rest of the lake (iCite,
grants, trials).
"""

from .ingest import curate, ingest, list_ranges, materialize_raw

__all__ = ["curate", "ingest", "list_ranges", "materialize_raw"]
