# 0014. Unified metadata & lineage: one model over heterogeneous backends

- Status: accepted
- Date: 2026-07-11

> The metadata analogue of the write contract (ADR-0011). Detail + DDL live in
> `docs/design/metadata_lineage.md`.

## Context

The catalog is becoming a multi-producer, multi-stage platform: EL (extract →
lake `upsert`, ADR-0013), transforms (SQLMesh, incoming), and reverse-ETL
(published parquet / serving Postgres / `omicidx.duckdb`, incoming). **Lineage and
observability across all of it are first-class requirements**, not afterthoughts.

The operational metadata that answers "what exists, what produced it, from what,
when, and did it work" already lives in several places and will grow:

- `ops.lake_ops.{source,run,watermark}` — Postgres, catalog-adjacent (ADR-0006).
- **DuckLake snapshots** + commit attribution (`author`/`commit_extra_info`) — the
  catalog metadata (ADR-0008).
- **Structured logs**, headed for ClickHouse via a shipper (the
  `dashboard_and_scheduling` design; ADR-0009 for the log surface).
- Once **SQLMesh** lands, the richest lineage of all — column-level, via SQLGlot —
  in SQLMesh's own state.

Two problems: (1) `ops.run` today records only **lake writes**, so the reverse-ETL
assets and the transform DAG are invisible to it — a failed publish or serving-load
doesn't show up anywhere. (2) Orchestrators (Dagster/Prefect) solve this by
**bundling** the metadata/lineage/observability layer into the executor; we want
that layer without the executor lock-in, and omicidx is the lone Prefect tenant.

## Decision

**Own the metadata/lineage layer as a contract over heterogeneous backends, rather
than bundled inside an orchestrator.** Execution stays whatever fits (SQLMesh, cron/
systemd, jobs); the metadata model is the thing that converges.

1. **One canonical model — five entities** that every stage and tool populates:
   - **Asset** — an identified materialized output (a lake table, a SQLMesh model,
     a published parquet, a serving table, a duckdb file); typed, ref'd, owned by a
     `writer`.
   - **Run** — a materialization event (status / duration / rows / version); this is
     `ops.run` generalized past lake-writes.
   - **Lineage edge** — asset *built-from* asset; a directed graph, **asset-level**.
   - **Version** — a point-in-time of an asset (DuckLake snapshot id, SQLMesh
     interval, published `vN`).
   - **Log** — the time-series detail attached to a run.

2. **`ops` is the hub; the other stores are adapters.** Storage stays
   heterogeneous — Postgres `lake_ops` (hub), DuckLake catalog (data versions),
   SQLMesh state (transform lineage), ClickHouse (logs) — and they converge on the
   model, surfaced by **one view** (the operations dashboard/API).

3. **Hybrid: materialize the skeleton, federate the detail.** The asset / run /
   lineage-edge / version **skeleton** is materialized in `lake_ops` (small,
   graph-queryable, and runs are already written there). The **heavy detail** — logs
   (ClickHouse) and column-level lineage (SQLMesh) — is **federated by reference**
   (`run_id`, model name), not copied into the hub. This resolves the
   materialize-vs-virtual fork: materialize what's small and high-value, federate
   what's high-volume or already authoritative elsewhere.

4. **SQLMesh is a lineage *provider*, not just an executor.** Its column-level
   lineage is authoritative for the transform sub-graph; the hub holds the
   asset-level edges and links out to SQLMesh for column detail. **Do not reinvent
   transform lineage.**

5. **Generalize `ops.run`** from `(source, target=lake table)` toward `(writer, run,
   asset)` where an asset's `type` is one of `lake_table | sqlmesh_model | parquet |
   postgres | duckdb`; add `lake_ops.asset` and `lake_ops.lineage`, plus a JSONB
   `metadata` column on `run` (arbitrary per-run metadata — the Prefect-artifact
   analogue). Per ADR-0006's portability note, the new tables carry **no**
   `SERIAL`/`PK`/`FK` (the `ops` DB may be Postgres reached through DuckDB's narrow
   DDL surface); uniqueness is enforced in code.

## Consequences

- The dashboard becomes the single "one view": an **asset catalog + lineage graph +
  run timeline**, fanning out to ClickHouse/SQLMesh by id. It already joins
  runs↔snapshots (`get_snapshots`) — this extends it to assets + lineage.
- **Every producer and every stage is visible**, including the reverse-ETL assets
  `ops` was blind to; a failed publish/serve now surfaces.
- **No orchestrator is load-bearing** — the layer Dagster/Prefect bundled is now
  ours, fed by thin adapters. This is what makes retiring Prefect a downgrade-free
  move (the observability/lineage it provided is replaced, not lost).
- **Cost:** the transform and reverse-ETL stages must each record runs/assets/edges
  (a thin adapter apiece) — the price of whole-pipeline visibility, and a
  platform-wide win, not omicidx-specific.
- **Sequencing:** land (or at least stub) this **before** SQLMesh adoption, so its
  lineage feeds the model on day one instead of becoming a second silo.

## Alternatives considered

- **Keep metadata in the executor** (Dagster/Prefect assets). Rejected — bundles the
  layer we want to own into a heavy runner with lock-in; Prefect's asset model is
  weaker than SQLMesh + `ops`, and omicidx is its only tenant.
- **Materialize everything into one store** (ETL all logs + column lineage into
  `lake_ops`). Rejected — metadata-ETL of high-volume logs and a duplicate of
  SQLMesh's authoritative lineage; federate-by-reference is cheaper and resilient to
  a backend's schema drifting.
- **Pure virtual view** (join across backends at query time, materialize nothing).
  Rejected — the asset/run/edge skeleton is small and high-value as first-class
  queryable state; graph queries over live cross-backend joins are fragile.
