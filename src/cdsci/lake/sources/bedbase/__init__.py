"""BEDbase — the catalog of BED files and bedsets (per-record DUO licence).

Three tables, ``lake.bedbase.{bed,bedset,bedset_membership}``: BEDbase's monthly
Parquet snapshot landed whole, raw's names and types, keyed ``id`` / ``id`` /
``(bedset_id, bedfile_id)``, stamped ``bedbase_version``. Ported from
bioc-on-ice's ``raw.bedbase__*`` (bioc-on-ice#183).
"""

from .ingest import TABLES, curate, download_snapshot, ingest, snapshot

__all__ = ["TABLES", "curate", "download_snapshot", "ingest", "snapshot"]
