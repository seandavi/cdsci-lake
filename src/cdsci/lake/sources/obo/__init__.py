"""OBO Foundry ontologies — OBO Graphs JSON, one licence per ontology.

One table ``lake.obo.element``: each ontology's release file landed whole, one
row per node and one per edge, every column VARCHAR, keyed ``(ontology, kind,
id, sub, pred, obj)``, stamped ``release_version`` (the file's own). Ported
from bioc-on-ice's ``raw.obo__<ontology>`` tables (bioc-on-ice#183).
"""

from .ingest import KEY, curate, download_json, file_version, ingest

__all__ = ["KEY", "curate", "download_json", "file_version", "ingest"]
