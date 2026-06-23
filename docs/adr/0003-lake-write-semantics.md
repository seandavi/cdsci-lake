# 0003. Lake write semantics: keyed MERGE-upsert with change-detection

- Status: accepted
- Date: 2026-06-23

## Context

Every source refreshes periodically (iCite monthly, ExPORTER per-FY, ClinicalTrials
daily, SCP monthly, PMC on rebuild). We want each refresh to produce a **meaningful
DuckLake snapshot** — the *delta*, not a full rewrite — so time-travel is useful and
storage doesn't balloon (each snapshot retains its data files until expiry).

A naïve `CREATE OR REPLACE TABLE` rewrites everything every load: every snapshot is
a full copy, and "what changed this month" is unanswerable.

## Decision

All sources write through **`cdsci.lake.upsert(con, target, source_sql, key,
exclude_change_cols=…)`**, which MERGEs on the natural key:

- **INSERT** rows whose key is new.
- **UPDATE** a matched row **only when a non-key column actually differs**
  (`t.col IS DISTINCT FROM s.col` over the compared columns). Unchanged rows are
  not rewritten.
- An **unchanged re-run writes nothing and adds no snapshot** (idempotent); each
  snapshot records only real deltas.

Natural keys are per source: `icite.metadata` → `pmid`; `reporter.{projects,
abstracts}` → `appl_id`, `publications` → `pmid`, `publink` → `(pmid,
project_number)`; `ctgov.studies` → `nct_id`, `references` → `(nct_id, pmid)`;
`scp.*` → its dimension tuple; `pmc.documents` → `pmcid`, `passages` → `(pmcid,
passage_index)`.

**Per-load stamps are excluded from change-detection.** A `snapshot_version` column
set to the load's tag on every row would otherwise differ on every row each load and
force a **full rewrite** (e.g. all 40M iCite rows monthly). `exclude_change_cols=
["snapshot_version"]` keeps it written (via `UPDATE SET *`) but out of the
`IS DISTINCT FROM` predicate — so only rows whose *real* data moved are rewritten,
and the stamp then records **the snapshot in which a row last actually changed**.
(Sources that don't stamp one — `reporter`, `ctgov` — are unaffected.)

## Consequences

- Time-travel is real: `ducklake_table_changes(...)` between snapshots returns the
  genuine inserts/updates; monthly loads cost ~the delta, not the table.
- `snapshot_version` doubles as per-row "last-changed" provenance.
- Storage of superseded versions is reclaimed by `cdsci.lake.maintenance`
  (expire snapshots → cleanup files), which is catalog-global and deliberate.
- The MERGE compares full rows by name; a source schema change (new column) needs a
  re-curate. Explicit per-source projections (not `SELECT *`) keep schemas stable.

## Alternatives considered

- **`CREATE OR REPLACE`** — simplest, but a full rewrite every load: no row-level
  history, every snapshot a full copy. Rejected (the reason MERGE exists).
- **Append a row per (key, snapshot)** — keeps all versions but bloats fast and
  pushes dedup to every read. Rejected; DuckLake snapshots already version.
- **Compare `snapshot_version` like any column** — forces the monthly full rewrite
  above. Rejected; it's the bug this ADR's exclusion fixes.
