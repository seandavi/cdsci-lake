"""IntAct — curated molecular interaction evidence, every organism (CC BY 4.0).

One table ``lake.intact.interaction``: a release's ``psimitab/intact.zip``
(``intact.txt`` and ``intact_negative.txt``) landed whole, every column
VARCHAR, keyed ``(file_name, line_number)``, stamped ``intact_version``.
Ported from bioc-on-ice's ``raw.intact__mitab`` (bioc-on-ice#183).
"""

from .ingest import COLUMNS, curate, download_release, ingest, latest

__all__ = ["COLUMNS", "curate", "download_release", "ingest", "latest"]
