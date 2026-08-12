"""ROR — the canonical, CC0 institution-identity authority (issue #57).

One raw landing table, ``lake.ror.organization``, keyed on ``ror_id`` (the bare
LUI). Joins ``lake.openalex.works_authorships`` via
``split_part(institution_ror, '/', -1) = ror_id``. See ``ingest.py`` for the
verified key, why the JSON dump is landed rather than the CSV subset, and why
country is deliberately *not* derived.

EL only: whether this ever reverse-ETLs to bioc-on-ice is the open question #57
raises and does not settle; publishing is deferred per #63.

License: CC0 (ror.readme.io/docs/data-dump).
"""

from .ingest import COLUMNS, check_keys, download_dump, ingest, land

__all__ = ["COLUMNS", "check_keys", "download_dump", "ingest", "land"]
