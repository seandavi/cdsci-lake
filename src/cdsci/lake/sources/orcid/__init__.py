"""ORCID — the canonical, CC0 researcher-identity authority (issue #56).

One raw landing table, ``lake.orcid.person``, keyed on ``orcid_id``.

**Not** the annual bulk Public Data File: that is ~863 GB of one-XML-file-per-
record with no scannable artifact, and nothing in this lake carries an ORCID iD
to join it against yet. This EL is demand-driven — hand it the iDs you have
(``orcids=`` or ``orcids_sql=``) and it batch-fetches them from the public API.
The deviation and the measured numbers behind it are in ``ingest.py``.

EL only: whether this ever reverse-ETLs to bioc-on-ice is the open question #56
raises and does not settle; publishing is deferred per #63.

License: CC0 (info.orcid.org/annual-data-files).
"""

from .ingest import COLUMNS, fetch, ingest, land, resolve_orcids

__all__ = ["COLUMNS", "fetch", "ingest", "land", "resolve_orcids"]
