# 0011. The multi-producer write contract: omicidx converges onto `lake_connect`/`upsert`/`ops`

- Status: accepted
- Date: 2026-07-10

> Turns the "omicidx and this repo are peer publishers" premise of ADR-0001 §1
> (restated in ADR-0007, ADR-0008) into a shared **write contract**. omicidx stops
> hand-rolling its DuckLake writes and imports this library's write path. Settled in
> a grilling session; the deliberation is preserved as the seven decisions below.

## Context

Two live publishers write the one shared catalog (Postgres `lake` + R2
`cdsci-lake`): this repo's ingestors and omicidx. They evolved **parallel,
divergent write paths** for the same job:

| Concern | cdsci.lake | omicidx (pre-convergence) |
|---|---|---|
| change gate | `upsert` — `IS DISTINCT FROM` over auto-derived columns | `merge_to_ducklake` — `_row_hash` column the projection hand-computes |
| run/watermark state | `ops.run` / `ops.watermark` (catalog-adjacent ledger, ADR-0006) | `HighWaterMark` + `SemaphoreStore` (JSON files in the bucket) |
| snapshot attribution | `author="cdsci:<source>"` + canonical JSON (ADR-0008) | `author="prefect:ducklake-load"` + a different JSON |
| connection | `lake_connect` (GSM creds, no secret-dir isolation) | `get_ducklake_connection` (env creds, isolates `secret_directory`) |

The costs are concrete: the `_row_hash` path carries a **double-list drift hazard**
(projected columns vs the columns hashed inside `md5(to_json({...}))` can diverge →
silent MERGE update-miss); the two attribution shapes defeat ADR-0008's "one query
attributes any snapshot"; and the connection code is duplicated and **already
drifted** (omicidx has a `secret_directory` fix this repo's path lacks). This is the
"parallel copy" tax — every fix and convention must be applied twice.

Publishing omicidx reproducibly (omicidx ADR-0004) runs *through* this library, so
the write path can't stay a private divergent copy. omicidx is therefore the
**second write-side adapter** that turns this library's write path from a
hypothetical seam into a proven one.

## Decision

omicidx takes a dependency on `cdsci-lake` (base install — the write path has no
dependency on the `[ingest]` ingestors) and deletes its parallel write code. Seven
points settle the contract:

1. **Depend, don't fork.** omicidx imports `lake_connect` / `upsert` / `ops`; it
   deletes `get_ducklake_connection`, `merge_to_ducklake`, `replace_to_ducklake`,
   and `HighWaterMark`. Extracting this repo's *ingestors* into their own package
   (leaving a `cdsci-lake-core` substrate) is **deferred** — the base install
   already gives the substrate/producer split logically; carve it only when a
   second core-only consumer exists.

2. **`IS DISTINCT FROM`, no hash.** All keyed writes go through `upsert`; the
   `_row_hash` columns are **dropped** from omicidx's projections and stored tables
   (a one-time `ALTER … DROP COLUMN`/rebuild). `upsert` derives the compare-column
   set from `DESCRIBE`, so the ergonomic reason for a digest is gone and the
   drift hazard is deleted, not merely excluded.

3. **State splits by job.** omicidx's semaphore files were doing two jobs. Raw-
   extract **crawl gating** (per-partition done-set, thousands of keys — a shape
   this repo's bulk-dump sources never have) **stays in semaphore files**. The
   lake-load **watermark + run-recording** move onto `ops.watermark` / `ops.run`.
   `ops` is not Postgres-locked — the `local` backend gives a portable sibling
   `ops.duckdb`, so local/published reproducibility keeps a file-based ledger for
   free; omicidx *gains* the run history (`last loaded / changed / errored / which
   snapshot`) it never persisted.

4. **Producers register their own sources.** The `SOURCES` tuple stops being the
   only registry. `bootstrap` ensures the `lake_ops` **schema only** — it seeds
   nothing. Registration happens through a new **`ops.register_sources(con, *,
   writer, sources)`**, and `_attach_ops`/`lake_connect` must **never** call it (a
   substrate that force-registered on connect would let a foreign producer re-seed
   another's rows on every connect). Two registration paths, by producer kind:
   - **External producers** (omicidx) call `register_sources(writer="omicidx", …)`
     explicitly, once at their load entrypoint (the top of `ducklake_load_flow`;
     idempotent, self-healing) — their source list lives in their repo.
   - **The library's built-in sources** (cdsci's `SOURCES`, which already lives in
     `ops.py`) **self-register lazily inside `ops.run`**: on run entry, a source
     that matches `SOURCES` and isn't yet in `lake_ops.source` is registered with
     its own `writer`. cdsci has 11 entrypoint-less ingestor CLIs that all route
     through `ops.run(source=…)`, so this gives them correct attribution with no
     per-ingestor call — and a foreign `run(source="sra")` (not in `SOURCES`) touches
     no cdsci rows. (When the ingestors are eventually split out of the substrate,
     `SOURCES` moves with them and this self-register becomes an explicit call like
     any other producer's.)

   A **`writer`** field is added to `Source` / `lake_ops.source` so the shared
   ledger is per-producer queryable (`cdsci`, `omicidx`); the `register_sources`
   `writer` param is authoritative for the row (the `Source.writer` field is a
   default). The dependency arrow stays correct — omicidx's source list lives in
   omicidx.

5. **One attribution shape.** `ops.Run` carries `writer`; the snapshot `author`
   becomes `f"{writer}:{source}"` (`omicidx:sra`, `cdsci:icite`) instead of a
   hardcoded `cdsci:`. `commit_extra_info` standardises on the canonical keys
   (`writer, source, target, version, run_id, op`) plus an optional per-producer
   **`extra`** dict merged in — omicidx keeps `prefect_run_id` for the snapshot→
   Prefect-run link. This completes the reconciliation ADR-0008 deferred.

6. **One connection, pluggable credentials.** `lake_connect` becomes the single
   connection builder; its **credential source is pluggable** (GSM for this repo's
   `gcloud` context, env for omicidx's Prefect workers). Two real credential
   backends = a real seam. omicidx's `secret_directory` isolation is **ported into
   `lake_connect`**, fixing this repo's latent "ambiguous `pg_main` secret" /
   "unknown storage `local_file`" exposure. Attach mechanics, secret hygiene, and
   HTTP-retry tuning now live once.

7. **One write verb — `upsert` (refined by ADR-0013).** The contract sanctions a
   single write verb. Every EL table is a keyed projection of a raw source, so
   `upsert` covers them all (delta snapshots even for full dumps). A `rebuild` /
   CREATE-OR-REPLACE primitive is **not** added: the only thing that would need it —
   derived tables — is a transform-layer artifact, deferred with the transform
   layer (ADR-0012, ADR-0013). No `truncate` either. (Phase-1 scoping refined this
   §7; the original proposal added a second `rebuild` verb.)

## Consequences

- The double-list `_row_hash` drift hazard is **gone from the codebase**, not
  worked around. One MERGE implementation, one attribution shape, one connection
  builder across both publishers.
- The shared `lake_ops` ledger and catalog attribution are now **multi-producer
  aware** (`writer` on `source`, `writer`-prefixed snapshot `author`); "show me all
  omicidx loads / whose snapshot is this" is one query, feeding the cross-producer
  observability goal.
- omicidx becomes the **reference producer**: the seam it validates and the `writer`
  registration + env-cred path it exercises are the spec the next producer (scp,
  cmgd) copies.
- **New surface here:** `Run.writer`, `run(extra=)`, `register_sources`,
  `bootstrap` no longer seeds sources (external producers call `register_sources`;
  built-in `SOURCES` self-register lazily via `ops.run` — see §4), a `writer`
  column on `lake_ops.source`, and a pluggable cred source + `secret_directory`
  isolation in `lake_connect`. (No `rebuild` primitive — see §7 / ADR-0013.)
- **Resolved (Phase-1):** omicidx's derived `linkage` table is a *transform-layer*
  artifact, not EL — it stays parked and un-wired until the transform layer lands
  (ADR-0013), not wired into the lake load. The "gold stays in consumers" boundary
  (ADR-0001 §3) never had to be invoked: it's a crosswalk built by a transform, and
  transforms are deferred.
- **Reversible cost:** omicidx's env-cred branch and the `writer` column are cheap
  to carry; if the ingestor-extraction (decision 1, deferred) later happens, the
  contract surface (`lake_connect`/`upsert`/`ops`) is exactly what moves into
  `cdsci-lake-core` unchanged.

## Alternatives considered

- **Share conventions only** (ADRs + a JSON shape, no code dependency). Rejected:
  leaves the `_row_hash` bug and two machineries alive; the publication story stays
  "trust two implementations agree."
- **Carve `cdsci-lake-core` now.** Rejected as premature — one importer is a
  hypothetical seam; the base install already separates substrate from ingestors.
- **Move all state (incl. the raw crawl) onto `ops`.** Rejected: `ops.watermark` is
  one cursor per `(source, name)`, not a done-set of thousands of partition keys;
  it would reinvent the semaphore inside Postgres.
- **A second `rebuild` verb for derived tables.** Initially accepted (§7), then
  refined away in Phase-1 scoping (ADR-0013): derived tables are transform-layer
  artifacts, so the EL contract never faces the keyless-recompute case and stays
  `upsert`-only.
- **Add a `truncate` primitive.** Rejected: it's a sanctioned footgun, and the case
  it targets (a schema reset) belongs to a transform-layer rebuild, not the EL path.
