"""``cdsci.lake`` — the shared research-data lake: read client + source ingestors.

A single DuckLake (Postgres catalog + Cloudflare R2 data) holds canonical,
source-faithful biomedical reference corpora — published by this package and by
omicidx — that internal projects consume instead of re-fetching from APIs.

Two surfaces, split by install extra:

* **Read client** (base install, ``pip install cdsci-lake``) — :func:`lake_connect`
  and config. Consumers attach the lake read-only and join across sources; they
  "assume the data exists" and never run ingest. This is the contract surface.
* **Ingest** (``pip install cdsci-lake[ingest]``) — :mod:`cdsci.lake.sources`,
  the bulk source importers (iCite, NIH RePORTER, …). Platform-side only.

``cdsci`` is a PEP 420 namespace (no ``__init__`` at ``src/cdsci/``); sibling
``cdsci.*`` packages may live in other repos. The boundary rule: the lake holds
*what is true about a paper/grant*; a consuming project decides *and it belongs
to our cohort* — entity resolution never enters a source or the lake.
"""

from . import ops
from .config import Settings, get_settings
from .connect import (
    LAKE,
    catalog_path,
    csv_source,
    data_path,
    lake_connect,
    ops_db_path,
    raw_dir,
    snapshot_log,
    snapshots,
    table_exists,
    upsert,
)

__all__ = [
    "LAKE",
    "Settings",
    "get_settings",
    "lake_connect",
    "upsert",
    "csv_source",
    "snapshots",
    "snapshot_log",
    "table_exists",
    "raw_dir",
    "catalog_path",
    "data_path",
    "ops",
    "ops_db_path",
]
