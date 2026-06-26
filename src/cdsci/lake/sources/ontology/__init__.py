"""OBO ontologies (semantic-sql SQLite builds) as the lake's controlled-vocabulary corpus.

`semantic-sql <https://github.com/INCATools/semantic-sql>`_ publishes each OBO
ontology as a SQLite database (the ``bbop-sqlite`` bucket); DuckDB's ``sqlite``
extension scans them directly. Four cross-ontology stacked tables, each row tagged
with its source ``ontology`` (the DB stem, e.g. ``uberon``, ``ncit``):

* ``ontology.terms``    — key ``(ontology, curie)``: ``label``, ``definition``,
  ``obsolete``, ``replaced_by``. The mapping target.
* ``ontology.synonyms`` — key ``(ontology, curie, synonym, scope)``: exact / broad /
  narrow / related synonyms — the free-text match surface.
* ``ontology.xrefs``    — key ``(ontology, curie, xref)``: database cross-references.
* ``ontology.edges``    — key ``(ontology, subject, predicate, object)``: asserted
  direct edges (is_a, part_of, ...).

Ancestor closure is computed on demand with a recursive CTE over ``ontology.edges``
(``WITH RECURSIVE`` on ``predicate IN ('rdfs:subClassOf', ...)``), so the large
``entailed_edge`` closure is intentionally not materialized (see ADR + docs/design/ontology.md).

This is the term-grounding layer: a curated free-text value (e.g. a phenotype
``disease``) is matched to a real ontology term by joining its normalized string
against ``terms.label`` / ``synonyms.synonym`` filtered to the expected ``ontology``,
then validated via ``edges`` (right branch) and ``obsolete`` (not deprecated) — the
model never emits an ID, it only disambiguates among real returned candidates.
"""

from .ingest import RELATIONS, available_ontologies, curate, fetch_db, ingest

__all__ = ["RELATIONS", "available_ontologies", "curate", "fetch_db", "ingest"]
