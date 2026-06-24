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

Phase 1b adds the literature edge ``mesh.article_heading`` — ``(pmid,
descriptor_ui, qualifier_ui)`` exploded from ``omicidx.pubmed_article.mesh_terms``
(which carries the ``D…``/``Q…`` UIs). ``descriptor_ui`` joins ``mesh.tree`` for
hierarchy rollups; ``pmid`` joins ``icite``/``reporter.publink``.
"""

from .ingest import (
    curate,
    curate_article_headings,
    descriptors_to_ndjson,
    ingest,
    ingest_headings,
    materialize_raw,
    qualifiers_to_ndjson,
)

__all__ = [
    "curate",
    "curate_article_headings",
    "descriptors_to_ndjson",
    "ingest",
    "ingest_headings",
    "materialize_raw",
    "qualifiers_to_ndjson",
]
