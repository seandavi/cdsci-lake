# 0006. Ontology source: semantic-sql OBO builds → `ontology.*`, closure on demand

- Status: accepted
- Date: 2026-06-26

## Context

Several lake/consumer use-cases need to ground free-text values in real controlled
vocabularies: mapping a curated phenotype `disease`/`body_site` to an ontology term,
crosswalking codes across NCIT/SNOMED/UMLS, validating that a term sits under the
expected branch. The hard requirement is **no hallucinated identifiers** — an LLM
must never emit an ontology ID from memory; it may only disambiguate among real
candidates returned by a lookup.

[semantic-sql](https://github.com/INCATools/semantic-sql) publishes every OBO
ontology as a SQLite database (the public `bbop-sqlite` S3 bucket, 332 ontologies as
of 2026-06-26). Each DB is the relational-graph encoding of one OWL ontology: a
generic `statements` triple table (literals in `value`, IRI objects in `object`)
plus convenience views (`edge` = asserted direct edges, `rdfs_label_statement`, …)
and a materialized `entailed_edge` transitive closure. DuckDB's `sqlite` extension
scans these directly — the same "extension reads the source, no parser code" pattern
as `census_geo` (`spatial`/`ST_Read`).

Two questions had to be settled: (1) what to project, and (2) whether to import the
`entailed_edge` closure, which is the dominant size driver (NCBITaxon's DB is ~2 GB
gzipped, almost all closure).

## Decision

A standard source at `cdsci.lake.sources.ontology` projecting **four cross-ontology
stacked tables** via the normal `upsert()` MERGE path. Every row carries an
`ontology` discriminator (the DB stem, e.g. `uberon`, `ncit`) so all ontologies live
in one set of tables, queryable together and filterable per column:

- `ontology.terms` — key `(ontology, curie)`: `label`, `definition`, `obsolete`,
  `replaced_by`. The mapping target.
- `ontology.synonyms` — key `(ontology, curie, synonym, scope)`: exact/broad/narrow/
  related (the free-text match surface; only `exact` should feed auto-accept).
- `ontology.xrefs` — key `(ontology, curie, xref)`: database cross-references.
- `ontology.edges` — key `(ontology, subject, predicate, object)`: asserted direct
  edges (is_a, part_of, …), read from semsql's `edge` view.

Projections read the `statements` base table by predicate, verified against a real
`hancestro.db`: `rdfs:label`, `IAO:0000115` (definition), `oio:has{Exact,Broad,
Narrow,Related}Synonym`, `oio:hasDbXref`, `owl:deprecated` (`value='true'`),
`IAO:0100001` (term replaced by). Literals come from `value`; the `oio:` prefix (not
`oboInOwl:`) is what semsql actually stores.

**`entailed_edge` is deliberately NOT materialized.** Ancestor/descendant closure is
computed on demand with a recursive CTE over `ontology.edges`:

```sql
WITH RECURSIVE anc(start, node) AS (
    SELECT subject, object FROM ontology.edges
      WHERE ontology = :o AND predicate = 'rdfs:subClassOf'   -- (+ part_of as needed)
    UNION                                                      -- UNION dedups / guards cycles
    SELECT a.start, e.object FROM anc a
      JOIN ontology.edges e
        ON e.ontology = :o AND e.predicate = 'rdfs:subClassOf' AND e.subject = a.node
)
SELECT * FROM anc WHERE start = :curie;
```

The bucket listing is the registry: `available_ontologies()` reads ListObjectsV2, so
a new OBO ontology is picked up with no code change. `snapshot_version` = the DB
object's S3 Last-Modified date, excluded from change-detection per ADR-0003. The
source is placed in its own `ontology` schema (not `ref`): it is a source-faithful
corpus with its own release cadence, like every other source; `ref` crosswalks join
*into* it.

## Consequences

- One uniform write path (MERGE-`upsert` for all four tables) — no append/delete
  special-casing, no closure-table maintenance. Re-runs are idempotent.
- Dropping `entailed_edge` removes the single largest storage cost and the one
  table that doesn't MERGE well (a pure function of `edge`, no row-level deltas).
  Closure is a query, not a table; `UNION` recursion handles cycles from mixed
  is_a/part_of/cross-ontology edges (DuckDB lacks the SQL `CYCLE` clause).
- Trade-off: bulk ancestor validation over very large hierarchies (NCBITaxon) is a
  recursive scan rather than an indexed closure join. Acceptable — mapping does a
  handful of lookups, not full-corpus closures; revisit by materializing closure
  for *selected* ontologies only if a workload needs it.
- `replaced_by` is stored as semsql emits it (often a full IRI, not a CURIE);
  consumers normalize against the `prefix` mapping if needed. Documented, not fixed.
- New dependency: none for Python — the `sqlite` extension is `INSTALL`ed at runtime
  like `spatial`; gzip is stdlib.

## Alternatives considered

- **Materialize `entailed_edge`** (append-mode + delete-by-ontology per release).
  Gives indexed closure joins but reintroduces a delete path the substrate lacks,
  multi-GB tables (NCBITaxon), and a non-MERGE special case. Rejected; the recursive
  CTE covers the actual need.
- **One SQLite file per ontology, queried in place** (the semsql/OAK default). No
  columnar lake, no cross-ontology query, outside the DuckLake governance/snapshot
  model. Rejected — the whole point is to land it in the lake.
- **Write plain Parquet to R2 and register** instead of `upsert`. Bypasses the
  catalog (no snapshots, no change-detection, orphan files). Rejected per ADR-0003.
- **Put it in `ref`.** Defensible (it is reference data), but it has its own corpus
  cadence and provenance; a dedicated `ontology` schema keeps `ref` for crosswalks.
