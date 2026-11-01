"""HGNC — human gene nomenclature (CC0).

One table ``lake.hgnc.gene``: the complete set landed whole, every column
VARCHAR, keyed ``hgnc_id``, stamped ``hgnc_version``. Ported from bioc-on-ice's
``raw.hgnc__complete_set`` (bioc-on-ice#183).
"""

from .ingest import COLUMNS, curate, download_tsv, ingest

__all__ = ["COLUMNS", "curate", "download_tsv", "ingest"]
