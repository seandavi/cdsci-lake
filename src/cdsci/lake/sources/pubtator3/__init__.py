"""PubTator3 — NCBI's text-mined and curated paper-concept annotations (US public domain).

Five tables ``lake.pubtator3.{gene,disease,chemical,species,mutation}_annotation``:
each ``<kind>2pubtator3.gz`` landed whole and verbatim, every column VARCHAR,
keyed ``line_number``, stamped ``pubtator3_version``. Ported from bioc-on-ice's
``raw.pubtator3__<kind>`` (bioc-on-ice#183).
"""

from .ingest import COLUMNS, KINDS, download_dumps, ingest, land

__all__ = ["COLUMNS", "KINDS", "download_dumps", "ingest", "land"]
