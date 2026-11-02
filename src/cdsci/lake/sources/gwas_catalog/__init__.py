"""NHGRI-EBI GWAS Catalog — curated variant-trait associations and their studies.

Two tables, one Catalog release each, every column VARCHAR, stamped
``gwas_catalog_release``: ``lake.gwas_catalog.association`` (the ontology-annotated
full associations download, keyed ``line_number``) and ``lake.gwas_catalog.study``
(the v1.0.3.1 studies download, keyed ``study_accession``). Ported from
bioc-on-ice's ``raw.gwas_catalog__{associations,studies}`` (bioc-on-ice#183).
"""

from .ingest import ASSOCIATIONS, STUDIES, curate, download_release, ingest, latest

__all__ = ["ASSOCIATIONS", "STUDIES", "curate", "download_release", "ingest", "latest"]
