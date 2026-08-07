# 0015. The transform + reverse-ETL layer: SQL files + sqlglot, DuckDB as the sole engine

- Status: accepted
- Date: 2026-08-07

> Un-defers the transform layer ADR-0012/0013 parked, and gives ADR-0014's asset/
> lineage model its first non-EL producer. Settled in conversation; the
> deliberation is preserved as the decisions below.

## Context

ADR-0012/0013 deferred the transform layer entirely — the lake write path is
`upsert`-only EL, and anything computed *across* tables (derived tables,
crosswalks, reverse-ETL publishes) was left for "when the transform layer
lands." ADR-0012 named two triggers for adopting a SQLMesh-style gateway config,
one of which — "SQLMesh is adopted for a producer's transforms" — has since
fired, but only in **omicidx** (`#120`, ahead of and independent from any
decision here). cdsci-lake itself still has no transform layer, and the
motivating de-duplication (one gateway file shared by EL and T) never
materialized because SQLMesh was never adopted on this side.

Two things changed the calculus for *this* repo:

- **SQLMesh is heavy for the actual shape of the problem.** Virtual dev
  environments, plan/apply diffing, a macro DSL, and its own state store all
  exist to manage incremental, partition-aware re-evaluation at scale. Every
  candidate derived table here (`ref.id_crosswalk`, omicidx's parked
  `publication_accession_linkage`) is a DAG of `CREATE OR REPLACE TABLE ... AS
  SELECT` over one DuckDB-family engine — full re-run each cycle is cheap
  enough that the incrementality machinery is buying nothing yet.
- **ADR-0014 already generalized `ops` into a hub-and-adapters metadata model**
  (asset / run / lineage-edge / version) and named SQLMesh explicitly as *one
  possible* lineage provider, not a mandate. The transform layer only needs to
  produce assets + lineage edges into that hub — it doesn't need to be SQLMesh
  to satisfy ADR-0014.

Separately, publication is expanding past the lake itself: reverse-ETL to
Parquet / Postgres / `omicidx.duckdb` marts already exists ad hoc (omicidx's
`parquet_export` flow), and a new target has appeared — publishing marts as
public, anonymous-read **Apache Iceberg** lakes (`iceberg-registry`). DuckDB
already reads/writes Parquet natively and speaks Postgres and Iceberg via
extensions, so one execution engine covers ingestion, transform, and every
publish target — there is no multi-engine problem to solve here.

## Decision

**cdsci-lake owns a lightweight transform + reverse-ETL module
(`cdsci.lake.transform`, behind a new `[transform]` extra — `sqlglot` is the
only added dependency; no new execution engine).**

1. **Models are plain SQL files**, one `CREATE OR REPLACE TABLE ... AS SELECT`
   per file, executed by DuckDB. No macro DSL, no virtual environments, no
   plan/apply. This is where ADR-0013's parked `rebuild` verb finally lives —
   scoped to this module only; the EL write path stays `upsert`-only,
   unchanged.
2. **`sqlglot` does two jobs, not orchestration:** (a) parse each model's table
   references to build the dependency DAG and a topological execution order;
   (b) `sqlglot.lineage` to derive column-level lineage edges. No connection
   config, no dialect translation — one dialect (DuckDB) throughout.
3. **Reverse-ETL targets are config, not code — for `parquet`/`duckdb`.** A
   model (or a downstream publish step) declares `target: {type: lake_table |
   parquet | postgres | duckdb | iceberg, ...connection}`; the runner executes
   through DuckDB. Adding a `parquet`/`duckdb` target is a config entry + a
   thin adapter, not a new client library — this is the "configurable
   reverse-ETL" omicidx's `parquet_export` never had, including its dated-copy
   + re-derived-`latest` write pattern (worth porting: writing `latest` by
   re-reading the dated Parquet over httpfs, not a server-side bucket copy —
   R2 flakes on multi-GB server-side copies). `postgres` and `iceberg` are
   *not* literal passthroughs — see Consequences.
4. **Every run is recorded through the existing `ops` hub** — model runs and
   published targets alike become `lake_ops.run` + `lake_ops.asset` rows (+
   lineage edges), per ADR-0014 §5. No second metadata store; this module is
   the "thin adapter" the transform stage owed that ADR.
5. **`iceberg-registry` stays pull, not push.** This module's job stops at
   landing correct Iceberg files + manifests; registry discovery/listing is
   the registry's own deterministic crawler's job. No registration payload is
   owned here — keeps the adapter reusable for any future Iceberg sink, not
   coupled to one registry's shape.
6. **Not a verdict on omicidx's SQLMesh.** It stays as-is, represented in the
   ADR-0014 model as a `sqlmesh_model` asset type with lineage federated by
   reference (ADR-0014 §3) — this ADR doesn't ask omicidx to migrate. If it
   ever does, this module is the convergence target, the same shape as
   ADR-0011's write-path convergence.

## Consequences

- Fires ADR-0012's deferred gateway trigger, but resolves it opposite to what
  that ADR anticipated: **no gateway/config-file is needed.** `sqlglot` carries
  no connection config to unify with `lake_connect`, and DuckDB's own
  `ATTACH`/extension config already is the one place a target is named.
  ADR-0012 stands, satisfied rather than superseded.
- The transform layer graduates from "deferred" to specified; `rebuild`
  becomes real and scoped. `ref.id_crosswalk` and reverse-ETL are unblocked.
- Reverse-ETL stops being a bespoke script per target and becomes the same
  primitive as every other model — config-driven target, uniform lineage,
  visible in the same dashboard ADR-0014 already puts over `ops`.
- **Cost:** no incremental/partition-aware re-evaluation — every model re-runs
  in full each cycle. Acceptable at current scale; revisit if a model's cost
  or cadence makes full re-run expensive (a future, narrower ADR, not blocking
  this one). `sqlglot`-derived lineage on hand-written SQL is also less
  battle-tested than SQLMesh's on gnarly CTEs/window functions — treat it as
  best-effort, not a correctness guarantee, until proven on real models.
- **`postgres` target is not thin config, as of omicidx's current pattern.**
  omicidx's existing Postgres reverse-ETL does zero-downtime A/B-slot table
  swaps (write inactive `_a`/`_b`, atomic view repoint, drop old slot) with
  per-table hardcoded DDL and JSONB projections — that needs a declarative
  DDL/column-mapping schema to become real config, which is unscoped design
  work, not a one-line adapter. Until that schema exists, treat `postgres` as
  a hand-written adapter per model, same as today, not a config-only target.
- **`iceberg` target: the REST catalog gateway is already built, not
  prerequisite infra.** DuckDB's Iceberg write path (CREATE/INSERT since
  1.4.0; MERGE/ALTER/partitioning since 1.5.3 — no longer preview-flagged)
  only writes through an attached **Iceberg REST catalog**, and
  [icegate](https://github.com/seandavi/icegate) already is one — a stateless
  proxy in front of Cloudflare R2 Data Catalog, live in production for
  `bioc-on-ice` (anonymous public read, key-scoped write), with DuckDB write
  verified by icegate's own acceptance suite. Client-side config is three
  lines: `INSTALL/LOAD iceberg`, `CREATE SECRET (TYPE ICEBERG, TOKEN …)`,
  `ATTACH '<catalog>' (TYPE ICEBERG, ENDPOINT …)`. `iceberg-registry`'s
  admission check (`GET /v1/config?warehouse=…`) is satisfied by the same
  gateway icegate already fronts. What's left for cdsci-lake specifically is
  config, not build: a catalog/namespace behind icegate for this repo's
  tables and a write-scoped key. One real code difference survives: DuckDB's
  UPDATE/DELETE on Iceberg are merge-on-read (positional deletes) only, so the
  `iceberg` adapter is CREATE-if-absent + MERGE/overwrite-insert, not a literal
  `CREATE OR REPLACE TABLE` passthrough like `parquet`/`duckdb` get — but that's
  a DuckDB/Iceberg semantics gap, not an infra gap.
- omicidx may run SQLMesh and this module side by side indefinitely — the same
  "coexistence during convergence" pattern ADR-0011 already validated for the
  write path.

## Alternatives considered

- **Adopt SQLMesh for cdsci-lake too**, mirroring omicidx. Rejected: heavy for
  a DAG of DuckDB-native SQL on one engine; would fork the "two producers, two
  transform tools" problem ADR-0011 eliminated at the EL layer, just one layer
  up.
- **No transform layer; keep derived tables as ad hoc scripts.** Rejected —
  `ref.id_crosswalk` and every reverse-ETL target are exactly the "computed
  across tables" case ADR-0013 named and deferred; deferring further blocks
  both, and reverse-ETL already exists ad hoc in omicidx without lineage.
- **A full custom orchestrator/scheduler alongside the SQL runner.** Rejected
  — out of scope. Scheduling is already "the simplest mechanism near the data"
  (ADR-0001 §6); this ADR is scoped to model execution + lineage, not to
  triggering runs.
