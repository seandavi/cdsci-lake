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

Phase 2 adds the drug/chemical layer: ``mesh.supplemental`` (SCRs — specific
chemicals/substances, ``scr_class`` 1 chemical/2 disease/3 protocol/4 organism),
``mesh.supplemental_descriptor`` (the heading-mapped-to bridge SCR→descriptor, with
``is_primary``), ``mesh.supplemental_term`` (SCR synonyms), and
``mesh.pharmacological_action`` (substance ↔ mechanism/effect descriptor). The
chemical literature edge ``mesh.article_chemical`` — ``(pmid, substance_ui)`` from
``omicidx.pubmed_article.chemical_list`` — completes article → substance →
``pharmacological_action`` → mechanism (the analogue of ``article_heading``).
"""

from .ingest import (
    curate,
    curate_article_chemicals,
    curate_article_headings,
    curate_pharmacological_actions,
    curate_supplemental,
    descriptors_to_ndjson,
    ingest,
    ingest_chemicals,
    ingest_headings,
    ingest_pharmacological_actions,
    ingest_supplemental,
    materialize_raw,
    pharmacological_actions_to_ndjson,
    qualifiers_to_ndjson,
    supplemental_to_ndjson,
)

__all__ = [
    "curate",
    "curate_article_chemicals",
    "curate_article_headings",
    "curate_pharmacological_actions",
    "curate_supplemental",
    "descriptors_to_ndjson",
    "ingest",
    "ingest_chemicals",
    "ingest_headings",
    "ingest_pharmacological_actions",
    "ingest_supplemental",
    "materialize_raw",
    "pharmacological_actions_to_ndjson",
    "qualifiers_to_ndjson",
    "supplemental_to_ndjson",
]
