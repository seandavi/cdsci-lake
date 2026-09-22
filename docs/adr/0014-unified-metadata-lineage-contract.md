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

## Amendment 2026-09-22

Follow-up from cdsci-lake#80/#100 (the M1 release-builder worker hit two gaps this ADR left open).

### 1. Asset `ref` grammar is pinned

`docs/design/metadata_lineage.md`'s open question #2 is resolved for **internal lake table** assets: the canonical ref is the dotted form `lake.<schema>.<table>` — lowercase identifiers only, no scheme, no credentials (e.g. `lake.demo.events`, `lake.ensembl.gene`). `cdsci.lake.contracts.check_lake_asset_ref` is the single validator for this grammar; `publish.release.SourceAssetVersion.ref` (always an internal lake table reference in a release's `source_asset_versions`) is validated against it and no longer accepts the `ducklake://` scheme the M0 worker introduced.

Other asset types the `lake_ops.asset` table already carries (`file`, `postgres`, `duckdb`, and now `release`) are not restricted to this grammar — they keep their own scheme (`r2://...`, `postgres://<db>.<schema>.<table>`, `file://...`) or, for a release-as-asset (§2 below), a distinct dotted form that is not the internal lake grammar. `cdsci.lake.contracts.check_asset_ref` is the shared *baseline* validator both `ops.register_asset`/`ops.record_lineage` and `check_lake_asset_ref` build on: no stray whitespace, no embedded credentials, regardless of scheme. `check_lake_asset_ref` layers the stricter dotted-grammar check on top for the internal-lake-table case specifically.

A release registers as an asset under `release.<dataset_id>.<release_id>` (e.g. `release.demo-catalog.R1`) — not the internal lake grammar, since `dataset_id`/`release_id` are producer-chosen strings (may contain hyphens or uppercase) rather than SQL identifiers. It is validated only by the baseline `check_asset_ref`.

### 2. `lake_ops.publication_receipt`

```sql
CREATE TABLE IF NOT EXISTS lake_ops.publication_receipt (
    receipt_id     TEXT,        -- client-generated uuid
    release_id     TEXT,        -- ReleaseManifest.release / PublicationReceipt.release
    dataset_id     TEXT,        -- ReleaseManifest.dataset / PublicationReceipt.dataset
    asset_ref      TEXT,        -- the release-as-asset ref, 'release.<dataset_id>.<release_id>'
    spec_version   TEXT,
    status         TEXT,        -- ArtifactStatus value at record time
    receipt        TEXT,        -- publish.release.PublicationReceipt.to_json()
    run_id         TEXT,
    recorded_at    TIMESTAMPTZ
    -- uniqueness: (release_id, dataset_id, asset_ref), in code (delete-then-insert)
);
```

Same portability posture as `asset`/`lineage`: no `SERIAL`/`PK`/`FK`, ids client-generated, uniqueness enforced by the writer (`ops.record_publication_receipt`). One receipt row per release build today (a single `asset_ref` per `(release_id, dataset_id)`); a producer that later publishes distinct per-format receipts for the same release (parquet vs. Iceberg, M6) will need a format-qualified `asset_ref` — not needed yet, so not speculatively built.

### 3. `asset_version` is not added

Per-version detail (snapshot id, schema digest, row count) stays on the `publication_receipt` row itself (its embedded `PublicationReceipt.checksums`/`row_counts`) and on `asset.current_version` plus retained DuckLake snapshots — consistent with this ADR's original §5 "Version is not (yet) its own table" stance. Promote `asset_version` to a first-class table only if per-version history beyond a receipt row plus DuckLake snapshots is actually needed (e.g. querying every schema digest a table has ever published, not just the latest); no such requirement has surfaced as of this amendment.

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
