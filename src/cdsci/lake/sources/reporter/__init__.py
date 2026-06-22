"""NIH RePORTER — all federally funded research projects, bulk-loaded.

Loaded from the **ExPORTER** per-fiscal-year project files (CSV in a zip), not
the per-PI API. ``lake.reporter_projects`` holds *every* NIH project for the
fetched years — institution-neutral. Matching grants to a center's members by PI
name stays a *project* concern (``cu_openalex.cancer_center.reporter``, ADR-0020);
this table is the shared, unattributed substrate it draws from.
"""

from .ingest import curate, download_years, ingest, list_files

__all__ = ["curate", "download_years", "ingest", "list_files"]
