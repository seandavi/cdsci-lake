"""NLM MeSH — the controlled vocabulary + tree hierarchy, from the annual XML.

Complementary to ``openalex.topics`` (algorithmic, on works): MeSH is NLM-curated
and human-assigned to PubMed, and its **tree numbers are the hierarchy** (ADR-0010).
Five tables (Phase 1a — the vocabulary):

* ``mesh.descriptor`` — one row per ``descriptor_ui`` (``name``, ``scope_note``).
* ``mesh.tree`` — exploded ``(descriptor_ui, tree_number)`` + ``parent_tree_number``;
  a descriptor has 0..N tree numbers (polyhierarchy). "Everything under Neoplasms"
  is ``tree_number LIKE 'C04%'``; deep traversal via a recursive CTE.
* ``mesh.qualifier`` — the ~80 subheadings, key ``qualifier_ui``.
* ``mesh.descriptor_qualifier`` — allowable-qualifier bridge ``(descriptor_ui,
  qualifier_ui)``.
* ``mesh.entry_term`` — synonyms ``(descriptor_ui, term, is_preferred)``.

The literature edge (``mesh.article_heading`` exploded from ``omicidx.pubmed_article
.mesh_terms``, which carries the ``D…``/``Q…`` UIs) is Phase 1b — not built here.
"""

from .ingest import (
    curate,
    descriptors_to_ndjson,
    ingest,
    materialize_raw,
    qualifiers_to_ndjson,
)

__all__ = [
    "curate",
    "descriptors_to_ndjson",
    "ingest",
    "materialize_raw",
    "qualifiers_to_ndjson",
]
