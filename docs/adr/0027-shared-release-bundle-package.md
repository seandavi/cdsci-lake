# 0027. A shared `cdsci-publish` package for the release-bundle mechanics

- Status: Accepted — owner decision, 2026-11-04 (open questions below remain open)
- Date: 2026-11-04

Tracking: cdsci-lake program map #95. Amends
[bioc-on-ice ADR-0015](https://github.com/seandavi/bioc-on-ice/blob/main/docs/adr/0015-sources-write-to-cdsci-lake-products-derive-at-release.md)
decision (a), "no shared release builder" (that amendment lands in bioc-on-ice
itself, not here). Step 1 of 7 in a product switch-over; the products keep
their own publish code until their own PR adopts this package.

## Context

Three products publish the same bundle shape: flat Parquet, plus
`<product>.duckdb`, `views.sql`, `restore.sql` and `manifest.json`, in one
directory. bioc-on-ice decided the format first
([ADR-0014](https://github.com/seandavi/bioc-on-ice/blob/main/docs/adr/0014-public-releases-are-parquet-plus-duckdb-views.md)),
following omicidx's
([ADR-0004](https://github.com/seandavi/omicidx/blob/main/docs/adrs/0004-public-serving-parquet-plus-views-sql.md)).
Both dropped a co-published Frozen DuckLake catalog on the same evidence:
three months of Worker analytics showed no real `ATTACH` traffic while flat
Parquet took 99% of requests, and the catalog's `data/` copy more than
doubled each bundle.

cancer-on-ice's `publish.py` is a line-for-line port of bioc-on-ice's, already
drifted: a different `ORDER BY` for one large table, an `EXCLUDED` table list,
a `HISTORY_NOTE` string appended to versioned tables' comments, an
empty-output-directory check in `publish()` that bioc's lacks, and
`json.dumps(..., default=str)` where bioc's has none. None of those
differences are merged here; they are product policy, decided later in each
product's own switch-over PR.

omicidx's `publish_bundle.py` builds the same shape through Prefect, with a
different manifest: `publish_date`, `dated_path`, `transform_sha`,
`lake_snapshot_id`, `raw_partition_counts`, and a `tables` list carrying only
`schema`, `table`, `row_count`. It has no shared code with bioc-on-ice's
`publish.py` today.

Nothing about the mechanics — streaming a SELECT to Parquet through `COPY`,
building DuckDB views over absolute URLs, writing `restore.sql`'s row-count
check, assembling `manifest.json` — is bioc-on-ice-specific. bioc-on-ice's
version is already written so every product-specific piece (the table list,
column/table docs, the provenance query, the DB file name, the base URL) is a
parameter or a module-level list, not inline logic.

## Decision

- A new package, distribution name `cdsci-publish`, import name `cdsci.publish`,
  lives in this repo at `packages/cdsci-publish/`, a `[tool.uv.workspace]`
  member. `cdsci-lake` itself stays at the repo root, unmoved: consumers
  pinning it by git rev with no subdirectory are unaffected. `cdsci-publish`
  does not depend on `cdsci-lake`; it depends only on `duckdb` and `fsspec`.
  Products install it by git subdirectory, pinned to a `cdsci-lake` revision.
- `cdsci.publish` owns the mechanics extracted unchanged in behaviour from
  bioc-on-ice's `publish.py`: `views_sql`, `restore_sql`, `_build_db` (and its
  `_HttpAsLocal` fsspec trick for building before anything is uploaded),
  `_git_sha`, `_lit`, `_url`, and `publish(...)`. Every product-specific value
  is a parameter: the table list, the doc lookup, the provenance query (a SQL
  string or `None`), the DB file name, the base URL, and the code checkout
  `_git_sha` reads (so a product's publish reports its own commit, not
  cdsci-lake's).
- **`manifest.json`'s shape is the shared contract**, described by a JSON
  Schema shipped in the package (`manifest.schema.json`):
  `release`, `published_at`, `git_sha`, `base_url`, `tables[{schema, table,
  path, row_count, bytes, sha256, comment, columns[{name, type, doc}]}]`,
  `provenance`, plus one additive field, `manifest_version: 1`.
  `additionalProperties` is `true` throughout, so a product can carry extra
  fields (omicidx's `lake_snapshot_id`, say) alongside the required ones.
  Within `manifest_version` 1, new fields are additive only; a breaking
  reshape is `manifest_version: 2`.
- `cdsci.publish.concordance(old_manifest, new_manifest)` compares what a
  reader of two releases actually sees: the table set, each table's row
  count, its column names and types in order, and its table/column docs.
  Byte identity, `sha256`, `bytes`, timestamps and `git_sha` are build-time
  fields and are not compared (owner: "row counts and schema are good enough
  for this check"). Products use it in their switch-over PRs to compare
  old-publisher output against new-publisher output on the same input.
- Product policy — retention, release cadence, the gate/Hugging Face upload
  steps, which tables a product publishes, how a product scopes or unions
  across sources — stays a product parameter or a product's own code, not
  something this package decides. Domain meaning (identifier namespaces,
  biological or cancer semantics) stays product-local, per this repo's
  AGENTS.md.
- Adoption order: bioc-on-ice first (closest to today's extraction source),
  then cancer-on-ice (reconciling its drift against bioc's behaviour, not
  against this package), then the gate / Hugging Face upload steps as a
  best-of extraction once two products share them, then omicidx (its own
  manifest fields fit through `additionalProperties`; its Prefect wrapper
  stays), then Croissant metadata generated from `manifest.json` once the
  contract has more than one producer to generate it from.

## Consequences

- This amends bioc-on-ice ADR-0015(a), which decided against a shared release
  builder on the evidence available then (one product, step 0 of its own
  migration map). That amendment is recorded here and lands as a matching
  edit in bioc-on-ice's own ADR history when bioc-on-ice's switch-over PR
  adopts the package; it does not happen in this PR.
- `cdsci.lake.publish` (`ReleaseManifest`, `TableFileIndex`, Frozen DuckLake,
  the ADR-0025-era per-dataset release model) is superseded as the path for
  product releases. It is not touched or removed in this PR: `bioc-on-ice`
  does not import it today, and nothing here depends on it. It retires once
  every importer of `cdsci.lake.publish` for product release-building is
  gone; `docs/design/scientific-publication-platform.md` is not rewritten
  here beyond a pointer to this ADR.
- This repo's `AGENTS.md` said public release semantics make "Frozen DuckLake
  ... the primary query adapter." That line is now wrong for the products
  that ship under ADR-0014/ADR-0004: flat Parquet plus DuckDB views is the
  public format, with Iceberg as optional interoperability. This PR corrects
  that line in `AGENTS.md`; see the open question on DuckDock below.

## Open questions for the owner

1. **DuckDock.** `duckdock` (dormant since 2026-09-22) is built on
   `cdsci.lake.publish`'s `ReleaseManifest` and Frozen DuckLake. Archive it, or
   re-point it at the `cdsci.publish` manifest contract once a second product
   adopts this package?
2. **The design doc.** `docs/design/scientific-publication-platform.md`
   still names a Frozen DuckLake as the primary public query path throughout
   (executive summary, architecture, milestones). That draft is not touched
   beyond a one-line pointer to this ADR in this PR. Does it get a full
   revision to match ADR-0014/ADR-0004/this ADR, or does it stay as a
   superseded historical draft once DuckDock's direction (question 1) is
   settled?
