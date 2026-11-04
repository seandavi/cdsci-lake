"""RNAcentral — non-coding RNA sequences cross-referenced to their member databases (CC0).

One table ``lake.rnacentral.id_mapping``: a release's ``id_mapping.tsv.gz``
landed whole, keyed ``line_number``, stamped ``rnacentral_release``. Ported from
bioc-on-ice's ``raw.rnacentral__id_mapping`` (bioc-on-ice#183).
"""

from .ingest import COLUMNS, curate, download_release, ingest, latest

__all__ = ["COLUMNS", "curate", "download_release", "ingest", "latest"]
