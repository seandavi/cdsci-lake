"""NIH RePORTER — federally funded research, bulk-loaded from ExPORTER.

Each ExPORTER file group becomes a lake table under the ``reporter`` schema —
``projects``, ``abstracts``, ``publications``, and the ``publink`` grants<->PMID
crosswalk — institution-neutral facts. Matching grants to a center's members by
PI name stays a *project* concern; these tables are the shared substrate.
"""

from .ingest import GROUPS, available_years, curate, download_group, ingest, list_files

__all__ = ["GROUPS", "available_years", "curate", "download_group", "ingest", "list_files"]
