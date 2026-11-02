"""Cellosaurus — the cell-line registry (CC BY 4.0).

One table ``lake.cellosaurus.line``: ``cellosaurus.txt`` landed whole, one row
per line of the file in file order, keyed ``line_number``, stamped
``cellosaurus_version``. Ported from bioc-on-ice's ``raw.cellosaurus__release``
(bioc-on-ice#183).
"""

from .ingest import curate, download_txt, file_version, ingest

__all__ = ["curate", "download_txt", "file_version", "ingest"]
