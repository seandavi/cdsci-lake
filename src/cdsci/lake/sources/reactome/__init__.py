"""Reactome — gene→pathway annotation, pathway names, pathway hierarchy (issue #34).

Three raw landing tables from Reactome's flat TSVs:
``lake.reactome.gene_pathway`` (NCBI gene → pathway, **all** hierarchy levels),
``lake.reactome.pathway`` (id → name → species) and
``lake.reactome.pathway_relation`` (parent/child edges). See ``ingest.py`` for
the verified keys, the species-name-not-taxon-id decision, and why ``gene_id``
is VARCHAR.

EL only: reverse-ETL to bioc-on-ice is blocked on the schema decision issue #34
names, and deferred per #63.

License: CC0 (reactome.org/license §1(c), data).
"""

from .ingest import DUMPS, Dump, download_dump, ingest, land

__all__ = ["DUMPS", "Dump", "download_dump", "ingest", "land"]
