# 0006. `lake_ops`: the operational ledger (sources, runs, watermarks, contracts)

- Status: accepted
- Date: 2026-06-23

> Implements ADR-0001 §6 ("Own the operational state; keep the orchestrator
> thin"). Design detail and table DDL live in `docs/design/lake_ops.md`.

## Context

Ingestors today are operational-state-free. Each `ingest()` sniffs change by
bracketing the upsert with `max(snapshot_id)` before/after and returns an
ephemeral summary dict; nothing is persisted. So the platform cannot answer:

- **When did we last load `<source>`, and which DuckLake snapshot did it produce?**
- **Did the last run change anything, or was it idempotent?** (and did it error?)
- **Where do incrementals resume?** OpenAlex wants `updated_date > last_pull`;
  ctgov paginates by `nextPageToken`; PMC by range — none of these cursors survive
  the process. This is the concrete blocker for watermark-driven incrementals.
- **What is the stable shape downstream consumers may depend on?** (the
  `dataset_contract` half of ADR-0001 §3's "versioned views are the contract").

ADR-0001 §6 already named the answer: *a small Postgres `lake_ops` model
(sources, versions, runs→snapshot ids, watermarks, contracts) as the operational
ledger.* This ADR settles **where it lives** and **how ingestors write to it**.

## Decision

### 1. `lake_ops` is catalog-adjacent native state, **not** DuckLake data

The ledger lives in the **catalog database**, alongside the DuckLake metadata —
**not** as DuckLake tables in the `lake` data plane:

- **Postgres backend (production):** a `lake_ops` schema in the same Postgres
  `lake` database that holds the DuckLake catalog, attached as a second DuckDB
  attachment `ops` via the `postgres` extension.
- **Local backend:** a sibling DuckDB file (`<catalog-dir>/ops.duckdb`) attached
  as `ops`. (A separate file, not the `.ducklake` catalog file, to avoid a
  read-write double-attach of one file.)

Why not store the ledger as DuckLake tables in `lake.lake_ops.*`:

- **Watermarks mutate in place.** Every cursor bump is an in-place `UPDATE`; in
  DuckLake that is a new snapshot per bump, polluting the data plane's time-travel
  with operational churn.
- **The ledger must be writable when the lake is read-only.** A run that attaches
  the lake `READ_ONLY` (or a maintenance/observability job) still needs to record
  that it ran. Native catalog tables are independent of the lake's attach mode.
- **One backup domain.** The catalog and `lake_ops` are already the
  backup-critical, non-regenerable state (ADR-0001 Consequences); co-locating them
  keeps that boundary single.
- **Plain SQL semantics.** `INSERT`/`UPDATE` with identity keys and `jsonb`, no
  MERGE/change-detection dance — the ledger is mutable by nature, the data plane
  is append/MERGE by nature. Different substrates for different semantics.

### 2. Four tables (full DDL in the design doc)

- **`lake_ops.source`** — the source registry: one row per source (`reporter`,
  `icite`, …) with its lake schema, cadence, distribution, license, and watermark
  strategy. Declared in code, upserted at attach (the registry is the source of
  truth; the table is its materialization for SQL/observability).
- **`lake_ops.run`** — append-only run ledger: one row per `ingest()` invocation
  with `source`, `target`, `started_at`/`finished_at`, `status`
  (`running`/`success`/`idempotent`/`error`), `snapshot_before`/`snapshot_after`,
  `rows_after`, the `version` label, and `error` text. This subsumes the
  before/after-snapshot bracketing every ingestor currently hand-rolls, and the
  per-source "versions" log is just `SELECT DISTINCT version` over it.
- **`lake_ops.watermark`** — mutable incremental cursors, keyed `(source, name)`,
  value as `jsonb`, with `updated_at` and the `run_id` that set it. In-place
  `UPDATE`. This is what unblocks `updated_date > last_pull`.
- **`lake_ops.dataset_contract`** — the stable consumer contract: `(schema,
  table, contract_version)` → declared columns/types, the stable view name, and
  status. Lands with the versioned-views workstream (roadmap "Versioned consumer
  views + `dataset_contract` registry"); specified here so the model is whole,
  generated from DuckLake stats rather than hand-maintained.

### 3. Ingestors record runs through one helper, not by hand

A new `cdsci.lake.ops` module exposes a run context manager and watermark
accessors:

```python
from cdsci.lake import ops

with ops.run(con, source="icite", target=target, version=version) as r:
    r.rows = curate(con, paths, version, target=target, limit=limit)
# on enter: snapshot_before captured, status='running' row inserted
# on exit:  snapshot_after captured, status set to success/idempotent/error, row finalized
```

```python
since = ops.get_watermark(con, "openalex", "updated_date")   # None on first run
... curate works where updated_date > since ...
ops.set_watermark(con, "openalex", "updated_date", new_high, run_id=r.run_id)
```

Each ingestor's `ingest()` drops its manual `snap_before`/`snap_after` block and
wraps the curate in `ops.run(...)`; its return dict is derived from the run
record. `lake_connect()` (write mode) ensures the `ops` attachment + `lake_ops`
schema/tables exist (idempotent `CREATE … IF NOT EXISTS`) and seeds `source`.

## Consequences

- **Watermark-driven incrementals are unblocked** — the first real payoff
  (OpenAlex `updated_date`, ctgov page token, Retraction Watch daily diff).
- **Auditability** — "last loaded / did it change / did it error / which
  snapshot" is one `SELECT` away; observability and scheduling read the same
  ledger.
- **Less duplicated code** — the before/after-snapshot pattern copied across all
  seven ingestors collapses into `ops.run(...)`.
- **A second attachment** on the write path (Postgres) / a sibling file (local).
  Read-only consumers don't attach `ops`.
- **The ledger is now backup-critical** alongside the catalog. Losing it loses
  watermarks (incrementals fall back to a full re-read, which is still correct —
  upserts are idempotent — just expensive) and run history.
- **Contract enforcement is still deferred** to the versioned-views work; this
  ADR reserves its table, it does not yet gate loads on it.

## Alternatives considered

- **`lake_ops` as DuckLake tables (`lake.lake_ops.*`).** Rejected: watermark
  in-place updates become snapshots, the ledger can't be written when the lake is
  attached read-only, and mutable rows fight MERGE semantics. (§1.)
- **A separate operational datastore** (own Postgres DB, SQLite, a JSON file).
  Rejected: more infrastructure for state the catalog database already hosts;
  splits the backup domain.
- **Lean on an orchestrator's metadata DB** (Airflow/Dagster). Rejected by
  ADR-0001 §6 — "keep the orchestrator thin"; scheduling is the simplest
  mechanism near the data, not a framework owning operational truth.
- **Keep deriving state from DuckLake snapshots alone.** Rejected: snapshots
  record *what* the data is, not *why/when/from-where* a run produced it, and
  carry no resumable cursor.
