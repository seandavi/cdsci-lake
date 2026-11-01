"""Complex Portal — curated and predicted macromolecular complexes (CC0).

One table ``lake.complexportal.complex``: every ``complextab`` file of one
release landed whole, every column VARCHAR, keyed ``complex_ac``, with the file
name and ``complexportal_version``. Ported from bioc-on-ice's
``raw.complexportal__complex`` (bioc-on-ice#183).
"""

from .ingest import COLUMNS, curate, download_release, ingest, latest

__all__ = ["COLUMNS", "curate", "download_release", "ingest", "latest"]
