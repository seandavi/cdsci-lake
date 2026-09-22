# 0008. Self-describing snapshots: attribute every write in the catalog

- Status: accepted
- Date: 2026-06-24

> Implements the first in-place fix from ADR-0007 (identify snapshots by source
> without leaving the single shared catalog).

## Context

On the shared catalog, a snapshot's only intrinsic identity is its `changes` map
(table-ids). ADR-0007 showed the cost: cdsci's 225 snapshots carried NULL
`author`/`commit_message`/`commit_extra_info`, so "whose snapshot is this?" was
answerable only by resolving table-id → schema through the Postgres metadata, or
by the `lake_ops.run` snapshot-id **range** (ambiguous under concurrency). omicidx,
by contrast, stamps each snapshot via DuckLake's `set_commit_message`
(`author='prefect:ducklake-load'` + JSON `commit_extra_info`). Two attribution
systems on one catalog; ours external and fuzzy.

DuckLake exposes `CALL <catalog>.set_commit_message(author, message, extra_info)`,
which annotates the snapshot produced by the **enclosing transaction's** commit. It
is transaction-scoped, not a session setting, and `extra_info` is an opaque string
(we put JSON in it). Verified on our version (DuckLake v1.0 / DuckDB 1.5.x).

## Decision

**Every cdsci write self-attributes the snapshot it produces.**

1. **`ops.Run.attribute(op)`** — a context manager that opens a transaction, calls
   `set_commit_message` with `author = "cdsci:<source>"`, a message, and a JSON
   `commit_extra_info`:

   ```json
   {"writer":"cdsci","source":"<source>","target":"<catalog.schema>",
    "version":"<snapshot_version>","run_id":"<uuid>","op":"<sub-step>"}
   ```

   runs the block, and commits (rolls back on error). One snapshot per block.

2. **Auto-wired into `upsert`** — the shared MERGE chokepoint detects the active
   run (an `ops` context-var set for the duration of `ops.run`) and wraps its write
   in `run.attribute(<table>)`. So **every upsert-based source** (icite, reporter,
   ctgov, scp, census_geo, europepmc, retractionwatch, reliance, openalex) is
   attributed with no per-source change. PMC's append path (no upsert) calls
   `attribute` explicitly per write (documents + each passage shard).

3. **Re-entrant** — a nested `attribute` (PMC `curate` wrapping a `_load` that calls
   the now-self-attributing `upsert`) joins the outer transaction; the outermost
   owns the BEGIN/COMMIT and the message. No nested `BEGIN`, one snapshot.

4. **`run_id` binds catalog ↔ ledger by a stable key**, not the `(before, after]`
   id-range — the ADR-0007 fix for concurrency-ambiguous attribution.

### Idempotency is preserved (the load-bearing property)

A no-op MERGE inside an attributed transaction produces **no snapshot** — verified:
`BEGIN; set_commit_message; <MERGE that changes nothing>; COMMIT` leaves the
snapshot count unchanged, as does an empty transaction. So the no-op-is-free,
time-travel-is-meaningful contract (ADR-0003) holds unchanged; only a real change
creates a snapshot, and that snapshot now carries its attribution. Outside an
`ops.run` (e.g. a direct test/util call) `upsert` runs unattributed.

## Consequences

- The catalog answers "whose snapshot, which run, which step?" on its own — no
  ledger join, no table-id resolution. Audit/reclamation can filter by `author` /
  `commit_extra_info->>'source'` / `run_id` / `op`.
- First loads emit **one** attributed snapshot per table instead of two
  (`CREATE TABLE` + `MERGE` now share the attribute transaction).
- The JSON shape is **reconciled in spirit with omicidx's** (`entity`/`source`/
  `operation`/`prefect_run_id`); a future shared key set lets one query attribute
  any publisher's snapshot. `commit_extra_info` is unvalidated text by DuckLake, so
  the convention is enforced only by us.
- **Not yet attributed:** maintenance/DDL snapshots (drop/expire in
  `maintenance_cli`) and any ad-hoc SQL. Worth attributing those next so no
  snapshot is anonymous.
- Attribution depends on DuckLake's transaction-scoped `set_commit_message`; the
  explicit-transaction wrapper is therefore mandatory and bounds each attributed
  write to one transaction (keep per-block writes bounded — see PMC shards).

## Amendment 2026-09-22 (cdsci-lake#89)

SQLMesh's DuckDB engine adapter runs with `SUPPORTS_TRANSACTIONS = False`, so
this ADR's `set_commit_message`-in-a-transaction mechanism cannot reach
SQLMesh-written snapshots (verified empirically). For the transform layer only,
attribution is sourced from `lake_ops` instead: `ops.sync_sqlmesh_snapshot_attribution`
brackets DuckLake snapshot ids per model against a watermark, excludes any
snapshot that already carries its own `commit_extra_info` (in-catalog
attribution wins over the side table), and records matches in
`lake_ops.snapshot_attribution(snapshot_id, run_id, source)`; `/api/snapshots`
joins this table when a snapshot's own `commit_extra_info` has none. The EL write
path (`ops.run` / `Run.attribute`) is unaffected and keeps in-catalog attribution
as originally specified.

A snapshot's `changes` map names a full-refresh replace by dotted
`schema.table`, but reports a data-only change (SQLMesh's INCREMENTAL/SCD2
model kinds, which don't recreate the table) by bare internal table id;
`_changed_tables` resolves those ids against the catalog's own
`ducklake_table`/`ducklake_schema` metadata, so both kinds are attributable, not
just full-refresh.

**Write semantics:** `lake_ops.snapshot_attribution` uniqueness is
`snapshot_id` (delete-then-insert, ADR-0006's portability convention) -- a
re-sync of the same snapshot (e.g. after a watermark reset) replaces its one
row rather than accumulating duplicates. The watermark write is the *last*
write in the sync (after the run + attribution inserts, on both the matched and
no-match paths), so a crash mid-sync leaves the watermark exactly where it was
and a retry re-scans and recovers the same range instead of silently losing
attribution.

Sync writes rows only for project `cdsci_lake`: `sync_sqlmesh_snapshot_attribution`
rejects any other `project` argument, and its caller
`sync_project_attribution` additionally skips any model in SQLMesh's own
`context.models` whose `Model.project` isn't the context's project --
SQLMesh injects PROD models belonging to *other* projects into that dict
(any state snapshot outside the loader's own projects), so scanning it
unfiltered would otherwise attribute a foreign producer's (e.g. omicidx's)
model under `cdsci_lake`.
