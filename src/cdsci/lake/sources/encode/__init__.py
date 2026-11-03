"""ENCODE — the portal's Experiment and File inventory (no use restrictions; cite ENCODE).

Two tables, ``lake.encode.experiment`` and ``lake.encode.file``: each
``report.tsv`` landed whole, every column VARCHAR, keyed ``accession`` and
``title``, stamped ``retrieval_date``. Ported from bioc-on-ice's
``raw.encode__{experiment,file}`` (bioc-on-ice#183).
"""

from .ingest import EXPERIMENT_FIELDS, FILE_FIELDS, curate, download_report, ingest

__all__ = ["EXPERIMENT_FIELDS", "FILE_FIELDS", "curate", "download_report", "ingest"]
