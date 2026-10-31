# 0026. Lake table naming: one schema per upstream source, tables named by content

- Status: accepted
- Date: 2026-10-31

Refines and amends ADR-0001 decision 3 ("per-source silver schemas"). That
decision makes per-source schemas of typed, curated tables the contract and
keeps bronze (raw downloads on R2/local) unregistered. It says nothing about
how schemas and tables are named. This ADR adds the naming rules and amends
decision 3 to permit an untyped companion table beside a typed table in a
source schema (decision 3 below). Source refs keep the ADR-0025 grammar
`<catalog>.<schema>.<table>`. Tracking: cdsci-lake#136 (first rename),
seandavi/bioc-on-ice#183 (migration map that depends on it).

## Context

ADR-0001 fixed per-source schemas of typed tables but did not name them. Three
conventions grew up side by side:

- **Producer-named schema.** `omicidx.pubmed_article` is named for the project
  that wrote it, not the data. The table carries the source name as a prefix to
  make up for it. cdsci-lake#136 moves PubMed out of omicidx, so the schema name
  would be wrong as soon as the producer changes.
- **File-named schema.** NCBI Gene lands as four schemas, one per dump file:
  `ncbi_gene`, `ncbi_gene2pubmed`, `ncbi_gene2go`, `ncbi_gene2accession`. That
  gives `ncbi_gene2pubmed.gene2pubmed`, the file name twice.
- **Source-named schema.** `bugsigdb.signature_taxon`, `ensembl.gene`: the
  schema is the upstream source and the table says what a row is.

Plural and singular are mixed (`reporter.projects`, `openalex.works`,
`ontology.terms` next to `bugsigdb.signature`, `ensembl.gene`). The plural can
be the only thing telling two tables apart: `bugsigdb.signatures` (the EL load
of the release CSV, key columns cast to typed values) beside
`bugsigdb.signature` (a transform model over it).

## Decision

1. **One schema per upstream source, named for the source.** Never for the
   producing project. `pubmed`, `ncbi_gene`, `bugsigdb`, `ensembl`. Every file
   from one upstream lands in that source's schema.
2. **Tables named by content, with no source prefix.** The schema already names
   the source: `pubmed.article`, `ncbi_gene.gene2pubmed`.
3. **snake_case, singular nouns.** A table is named for one row: `article`,
   `signature`, `gene`. **The `raw_` table prefix is only for a verbatim,
   untyped landing** (an all-VARCHAR CSV dump, say) **that sits beside a typed
   table of the same content in the source schema**. For example, a
   hypothetical source `acme` would land its CSV with every column VARCHAR as
   `acme.raw_sample` next to the typed `acme.sample`. A typed extract-load
   copy of an upstream file is named by its content with no prefix:
   `ncbi_gene.gene2pubmed`, `ensembl.gtf`. Every
   ingestion-path write is EL (ADR-0013), so a prefix on every EL table would
   carry no information. There is no separate raw schema.
4. **Published names are separate and owned by each product.** A release table
   name comes from the product's release table list, as a SELECT over the lake
   table: bioc-on-ice publishes `annotation.pubmed__article` from
   `lake.pubmed.article`. Renaming a lake table changes that SELECT, never the
   name end users query.
5. **Rename on migration, not in one sweep.** Each existing table takes its new
   name the next time it is migrated or reworked. The first is PubMed:
   `omicidx.pubmed_article` -> `pubmed.article` (cdsci-lake#136).

## Consequences

- A lake name tells a reader where the data came from and what a row is,
  without knowing which repository wrote it. Moving a source between producers
  (omicidx to cdsci-lake) no longer forces a rename.
- NCBI Gene folds into one `ncbi_gene` schema as its tables migrate; the
  `ncbi_gene2*` schemas go away with them.
- An untyped landing and its typed table sit together in one source schema, and
  the `raw_` prefix marks which is which. Lineage and ownership stay per source
  schema.
- Lake renames are internal. Consumers of the lake (transform models, the MeSH
  ingest's `lake.omicidx.pubmed_article` default, product release lists) update
  in the same change as the rename. End users of a published release see no
  change.
- Until every table has migrated, the lake has both old and new names. That is
  expected: old names are not a precedent for new tables.

## Rejected alternatives

- **Producer-named schemas** (`omicidx.*`, `cdsci.*`). Ties the name to
  whoever writes the table today. A producer change renames every table, and one
  producer's schema mixes unrelated sources.
- **One flat schema with source prefixes** (`lake.main.pubmed_article`). Loses
  the per-source namespace that ownership, grants and lineage hang on, and puts
  the source name in every table name.
- **A `raw_` prefix on every extract-load table** (`ncbi_gene.raw_gene2pubmed`).
  Every ingestion-path write is EL (ADR-0013), so the prefix would mark nearly
  every table and tell the reader nothing.
- **A separate raw schema** (`raw.acme_sample`, or `acme_raw.sample`). Splits one source across two schemas and brings
  the source prefix back. A `raw_` table prefix inside the source schema marks
  the layer without either cost.
