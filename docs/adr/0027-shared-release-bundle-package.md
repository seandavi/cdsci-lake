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
  `_git_sha`, `_lit`, `_url`, and `publish(...)` (`_build_db` and `_lit` are public as
  `build_db` and `sql_literal` since step 4; `publish` refuses a non-empty output
  directory, cancer-on-ice's rule, and a caller re-running after a crash clears its own). Every product-specific value
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

### Step 4b: the upload and the Hugging Face push (2026-11-04)

`cdsci.publish.upload` and `cdsci.publish.hf` hold the bucket upload and the Hugging
Face push, the best of bioc-on-ice's and cancer-on-ice's copies (both descend from
omicidx's `publish_bundle` / `hf_release`). Each is behind an extra, `[upload]`
(pyarrow) and `[hf]` (huggingface-hub); the base install is unchanged. Neither reads a
credential: the caller passes a pyarrow filesystem (`r2_filesystem(account, key,
secret)` builds the R2 one from values the product resolved) or an `HfApi`.

Behaviours chosen, and from where:

- Upload: `v{release}/` then `latest/`, each with views/restore/DB regenerated for its
  own URL, manifest.json last (all three). A completed dated folder of other files is
  never overwritten; an interrupted one is retried; a complete one with this bundle's
  files finishes only `latest/` (bioc). `latest/` loses its manifest.json before any
  overwrite; every Parquet file is re-hashed against its manifest sha256 first; the
  release must be a real date; only complete dated folders count for retention or are
  pruned (cancer's counting; an incomplete one may be another run's upload in progress,
  so it is left for a person, per review of #161). Prune-only around
  the release `latest/` serves (bioc). `extra_files` copied verbatim beside manifest.json
  replaces cancer's hard-coded `gate.json`.
- Retention is a pure function `(complete dates, current, today) -> dates to keep`,
  `current` always kept: `keep_newest(n)` (bioc ADR-0016, 4), `keep_daily_weekly(daily,
  weekly)` (omicidx, 7 + 5 ISO weeks) and `keep_monthly(months, grace_days)` (cancer
  ADR-0001, 12 + 7). A product passes one, or its own callable.
- Push: one `create_commit`, remote files not in the release deleted, `.gitattributes`
  kept (omicidx); download-relative `views.sql`/`restore.sql`, the product's DuckDB file
  not pushed, nothing at `hf://` (bioc); an existing tag is a no-op only when it holds
  this manifest.json byte for byte and fails otherwise, and a 409 on the tag right after
  its commit is retried (cancer). The tag string and the dataset-card builder are
  parameters: the card is product prose, not shared; omicidx's card with `hf://` query
  examples was not adopted (bioc ADR-0016's 429 rationale).
- Not moved: the gate-proof check (`gate.json` matches manifest.json) and cancer's
  "the tag already holds this manifest" pre-upload check. Both are product ordering
  policy around the gate, which is the next step; a product runs them before calling
  `upload` / `hf_push`. (The proof check moved in step 4c, as `check_proof`.)

### Step 4c: the release gate (2026-11-04)

`cdsci.publish.gate` holds the acceptance check a product runs on a built bundle before
`upload` / `hf_push`, the best of bioc-on-ice's `gate.py` (which called
`cdsci.lake.publish.verify.verify_release_over_release`, the ADR-0013/0025-era model this
package supersedes) and cancer-on-ice's port of it. Stdlib + duckdb only; it does not
import `cdsci.lake`. Everything product-specific is a parameter of `gate(bundle,
reference, ...)`: the README queries, the catalog the views live under, the keys and
references to check (bioc builds them from its `TableContract`s), the row-count bounds and
whether growth fails, the two owner-approved column allow-lists, and `extra_checks` for
the product's own rules (cancer's `value_null_unless_reported`, bioc's
`contract_declared` policy). A check is a `Check(name, passed, required, detail)` named
tuple, replacing `AcceptanceCheck` for this purpose.

Behaviours chosen, and from where:

- Reads the bundle's Parquet and manifest.json, and the reference's manifest.json only
  (both). Required: the bundle has tables and each table's manifest row count, columns
  and types match its Parquet (both); against the reference, which must list tables, no
  table is missing, columns and types are unchanged, row counts stay within bounds (both);
  declared keys not null and unique, declared references covered (bioc, from the old
  verifier, now keyed by plain mappings). README queries are warnings only (both).
- The reference must be a strictly older YYYY-MM-DD release (both). It is fetched with
  urllib and a named User-Agent, since Cloudflare refuses urllib's default; a 404 is a
  first release and runs the bundle checks only; anything else fails closed (cancer). A
  local directory is read directly (bioc's tests).
- Row counts: shrinking below `low` times the reference fails; growing past `high` times
  it fails by default, or warns when the product says growth is legitimate
  (`growth_required=False`, cancer's new-vintage rationale). Default bounds are bioc's
  (0.9, 1.5); cancer passes (0.8, 3.0). A reference count of 0 bounds nothing above.
- Column changes: a table in `allow_dropped` may lose exactly the columns named for it
  (bioc's `--allow-schema-change` × row-history columns); one in `allow_column_change` may
  change in any way, the exact diff recorded (bioc's `--allow-column-change`). Both are
  written to gate.json as `schema_changes` and `column_changes`, bioc's keys.
- gate.json is bound to manifest.json by sha256 and not written when the manifest changed
  while the gate ran (cancer); `check_proof(bundle)` is the pre-upload check both products
  duplicated.
- Not adopted: bioc's DuckDB `read_text` fetch of the reference (no User-Agent control),
  bioc's symmetric-set column detail (cancer's diff is more exact, and bioc's own
  `column_diff` describes it), the old verifier's `AcceptanceReport` / `run_id`.

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
