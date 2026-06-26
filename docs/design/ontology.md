# Ontology source

OBO ontologies as the lake's controlled-vocabulary corpus, loaded from
[semantic-sql](https://github.com/INCATools/semantic-sql)'s per-ontology SQLite
builds (the public `bbop-sqlite` S3 bucket). DuckDB's `sqlite` extension scans each
DB directly — no parser code — mirroring how `census_geo` uses `spatial`/`ST_Read`.

See [ADR-0006](../adr/0006-ontology-source.md) for the decisions (four-table
projection; closure computed on demand rather than materialized).

## Tables (`ontology` schema)

Every row carries an `ontology` discriminator = the source DB stem (`uberon`,
`ncit`, `hancestro`, …), so all ontologies are stacked in one set of tables and
filtered per use.

| table | key | columns |
|---|---|---|
| `terms` | `(ontology, curie)` | `label`, `definition`, `obsolete`, `replaced_by` |
| `synonyms` | `(ontology, curie, synonym, scope)` | `scope` ∈ exact/broad/narrow/related |
| `xrefs` | `(ontology, curie, xref)` | database cross-references |
| `edges` | `(ontology, subject, predicate, object)` | asserted direct edges (is_a, part_of, …) |

All four carry `snapshot_version` (the DB's S3 Last-Modified date), excluded from
change-detection (ADR-0003).

## Source encoding (semantic-sql)

Verified against a real `hancestro.db`. Everything is projected from the
`statements` triple table by predicate (literals land in `value`, IRI objects in
`object`); `edges` reads semsql's `edge` view (asserted direct edges).

| projection | predicate(s) |
|---|---|
| label | `rdfs:label` |
| definition | `IAO:0000115` |
| synonyms | `oio:hasExactSynonym` / `…Broad…` / `…Narrow…` / `…Related…` |
| xrefs | `oio:hasDbXref` |
| obsolete | `owl:deprecated` (`value = 'true'`) |
| replaced_by | `IAO:0100001` |

Note the prefix is `oio:`, not `oboInOwl:`. `replaced_by` is stored as semsql emits
it (often a full IRI rather than a CURIE).

## Ancestor closure: a query, not a table

The `entailed_edge` transitive closure is intentionally **not** materialized (it's
the dominant size driver — NCBITaxon ≈ 2 GB gz — and a pure function of `edges`).
Compute ancestors on demand with a recursive CTE:

```sql
WITH RECURSIVE anc(start, node) AS (
    SELECT subject, object FROM ontology.edges
      WHERE ontology = 'uberon' AND predicate = 'rdfs:subClassOf'
    UNION
    SELECT a.start, e.object FROM anc a
      JOIN ontology.edges e
        ON e.ontology = 'uberon' AND e.predicate = 'rdfs:subClassOf' AND e.subject = a.node
)
SELECT * FROM anc WHERE start = 'UBERON:0002107';   -- ancestors of "liver"
```

Use `UNION` (not `UNION ALL`) so the recursion dedups and terminates on cyclic
graphs (mixing `part_of`/cross-ontology edges can create cycles; DuckDB has no SQL
`CYCLE` clause). Add other predicates (e.g. `BFO:0000050` part_of) to the filter as
the task needs.

## Term grounding (the no-hallucination contract)

The mapping pattern this source exists to support: a free-text value (e.g. a
phenotype `disease`) is matched to a real term by joining its **normalized** string
against `terms.label` / `synonyms.synonym`, filtered to the column's expected
`ontology`. The model never emits an identifier — it only disambiguates among real
returned candidates. Validate a pick with `edges` (sits under the expected branch)
and `terms.obsolete = false` (not deprecated; offer `replaced_by` if it is).

## Usage

```bash
# list everything available in the bucket
python -m cdsci.lake.sources.ontology list

# load specific ontologies
python -m cdsci.lake.sources.ontology run -o uberon -o ncit -o hancestro

# load ALL available ontologies (omit --ontology)
python -m cdsci.lake.sources.ontology run

# show the projected tables + keys
python -m cdsci.lake.sources.ontology tables
```

Each DB is downloaded + gunzipped into the bronze raw layer (`lake/raw/ontology/`)
once; re-running re-uses the bronze files and MERGEs (idempotent). A single
ontology's failure is recorded and skipped, not fatal to the batch.
