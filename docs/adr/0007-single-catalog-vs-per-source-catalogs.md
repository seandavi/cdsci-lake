# 0007. Stay single-catalog; defer per-source catalogs

- Status: accepted
- Date: 2026-06-24

> Revisits the storage topology assumed by ADR-0001 (one shared DuckLake) in
> light of two concrete pains surfaced while rolling back a broken PMC load.
> Reconsider triggers are listed under *Consequences*.

## Context

The platform runs **one** DuckLake catalog (Postgres `lake` + R2 `cdsci-lake/`)
shared by every publisher — all cdsci sources **and** omicidx — so snapshot ids
are a single catalog-wide sequence (120…514 today). Two issues became concrete
while purging a failed PMC bulk load (8.36M docs / 974M passages, ~360 GB):

1. **Per-source reclamation is coupled.** A DuckLake data file is not owned by one
   snapshot; it carries a lifespan `[begin_snapshot, end_snapshot)` and is pinned
   by **every** live snapshot in that interval — regardless of which table that
   snapshot wrote (a snapshot is a full point-in-time view of the *whole* catalog).
   `ducklake_expire_snapshots` selects only by `older_than` (global, by time) or an
   explicit `versions` id list — never by source/schema/tag. So dropping `pmc` and
   expiring its 60 own snapshots freed only **7 of 359 GB**: 157 interleaved
   openalex/europepmc/reliance snapshots from the same days still pin the PMC files.
   Full reclamation requires a global `older_than` pass that ages out everyone's
   intermediate snapshots together. See `cdsci.lake.maintenance.purge_schema`.

2. **cdsci snapshots are not self-describing.** Of 321 snapshots, the 96 omicidx
   ones carry `author` + `commit_message` + JSON `commit_extra_info` (set via
   `CALL <catalog>.set_commit_message(...)`); the 225 cdsci ones are NULL —
   attributable only by reverse-resolving `changes` table-ids → schema via the
   Postgres metadata, or by the `lake_ops.run` snapshot-id range (ambiguous under
   concurrency). Two disjoint attribution systems on one catalog.

We weighed three responses: a different table format (**Iceberg**), a serving
engine (**StarRocks**), and **splitting into per-source DuckLake catalogs**.

- **StarRocks is the wrong layer.** It is an MPP serving engine, not a table
  format, and would fix neither issue. Consumers serve queries through **their own
  DuckDB** against the lake (ADR-0001), so an always-on engine is not wanted now.
- **Iceberg would structurally fix issue 1** (its snapshots are *per-table*, so
  expiry/reclamation never crosses sources) but at real cost: a separate catalog
  service, DuckDB Iceberg **writes** are immature (pyiceberg/Spark territory),
  losing DuckLake's whole-lake consistent snapshots, and migrating 10 ingestors +
  the read-client contract. It is an **ecosystem/longevity** bet, not a reclamation
  fix, and not justified by these pains today.
- **Per-source DuckLake catalogs** (one Postgres metadata DB + R2 path each) give
  Iceberg-like lifecycle isolation *without leaving DuckDB*: `ATTACH` each, query
  `catalog.schema.table`, join across catalogs normally (verified). Reclaiming a
  single-source catalog (e.g. `pmc`) becomes a plain `older_than` expiry with no
  cross-publisher pinning. The cost is a consumer-facing rename and giving up the
  global snapshot id (which per-source loads don't use).

Crucially, **the catalog is "just storage" with low lock-in**: data is plain
Parquet on R2, the catalog holds only metadata + the `data_path`. Splitting later
is a metadata/path reorg (re-register or re-load files), not a format change — so
deferring is reversible and cheap to revisit.

## Decision

**Keep the single shared catalog for now.** Do not split into per-source catalogs,
do not adopt Iceberg, do not adopt StarRocks. Address the two pains with
DuckLake-native, in-place fixes instead of restructuring storage:

1. **Make new snapshots self-describing** — wrap each load's writes so the run's
   snapshot carries `author = "cdsci:<source>"`, a message, and a JSON
   `commit_extra_info` (`{writer, source, schema, run_id, op, version}`) via
   `set_commit_message`, reconciled with omicidx's shape so one query attributes
   any snapshot. (Bind `lake_ops.run` to the catalog by `run_id`, not id-range.)
2. **Treat space reclamation as a time-based global cadence** — set
   `expire_older_than` / `delete_older_than` and run scheduled global maintenance,
   accepting that interleaved files only free on such a pass. Per-source rollback
   stays available via `purge_schema` (drop + version-targeted expiry) for *removal
   from the head*, understanding it reclaims only files whose whole lifespan was
   that source's snapshots.

## Consequences

- **Accepted now:** per-source file reclamation is partial — a source's files free
  only when every snapshot spanning their lifespan expires, i.e. on a global
  `older_than` pass. Until then dropped data lingers on R2 (cheap; ~$/100GB-mo).
- **Stays simple:** one `ATTACH … AS lake`, whole-lake consistent snapshots, and
  cross-source joins with no multi-attach. The read-client contract
  (`lake.<schema>.<table>`) is unchanged.
- **Reconsider and split a resource into its own catalog when any of:**
  1. reclaiming a high-churn giant (**pmc**, **openalex**) becomes a material cost
     or cadence problem under the shared-catalog pinning above;
  2. a source needs an **independent retention/expiry policy** the global cadence
     can't express;
  3. a load's snapshot churn or rewrite volume degrades catalog operations for
     other publishers.
  The migration path is known (multi-`ATTACH`, `catalog.schema.table`, cross-catalog
  joins; isolate the giants, keep small sources together, alias the core catalog as
  `lake` for back-compat) — capture it as its own ADR at that time.
- **Reconsider the format (Iceberg) only** if a driver beyond these emerges:
  multi-engine/external interop or long-term vendor-neutrality. Not a reclamation
  decision.
