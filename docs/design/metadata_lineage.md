# Unified metadata & lineage — design

Detail behind **ADR-0014**. This doc is expected to churn as SQLMesh and reverse-ETL
land; the ADR holds the settled decision, this holds the mechanics + open questions.

## The five entities → `lake_ops` skeleton (proposed DDL)

Materialized in `lake_ops` (the hub). Follows ADR-0006's portability rule: **no
`SERIAL`/`PRIMARY KEY`/`FOREIGN KEY`** (the `ops` DB may be Postgres reached through
DuckDB's `postgres` extension); uniqueness enforced in code, ids client-generated,
JSON as text.

```sql
-- Asset: an identified materialized output.
CREATE TABLE IF NOT EXISTS lake_ops.asset (
    ref          TEXT,        -- physical locator, e.g. 'lake.omicidx.sra_study',
                              --   'r2://data-omicidx/latest/sra_study.parquet'
    writer       TEXT,        -- owning producer ('omicidx', 'cdsci', ...)
    asset_type   TEXT,        -- lake_table | sqlmesh_model | parquet | postgres | duckdb
    name         TEXT,        -- logical name (schema.table, model name, ...)
    first_seen   TIMESTAMPTZ,
    last_run_id  TEXT,        -- most recent run that materialized it
    current_version TEXT      -- snapshot id / interval / vN (see Version)
    -- uniqueness: (writer, ref), in code
);

-- LineageEdge: `dst` was built from `src`. Asset-level (column-level stays in SQLMesh).
CREATE TABLE IF NOT EXISTS lake_ops.lineage (
    src_ref      TEXT,        -- upstream asset ref
    dst_ref      TEXT,        -- downstream asset ref
    edge_type    TEXT,        -- declared | sqlmesh
    run_id       TEXT,        -- run that (re)established the edge, if applicable
    discovered_at TIMESTAMPTZ
    -- uniqueness: (src_ref, dst_ref), in code
);
```

`lake_ops.run` (ADR-0006) is **generalized**: `target` becomes an asset `ref` of any
`asset_type` (not only a lake table), and a JSONB/text `metadata` column is added for
arbitrary per-run metadata (the Prefect-artifact analogue). `source`, `watermark`,
and `dataset_contract` stay as-is. **Version** is not (yet) its own table — it's the
`current_version` on `asset` + the existing `run.snapshot_before/after`, resolved
against DuckLake snapshots at the view.

## Adapters — who populates what

| Backend | Populates | How |
|---|---|---|
| **EL loaders** | `run`, `asset` (lake_table), `lineage` (raw→lake, declared) | `ops.run` (already) + a small asset/edge write |
| **SQLMesh** | `asset` (sqlmesh_model), `lineage` (asset-level), transform `run`s | a **sync step** ingests SQLMesh lineage; column-level stays federated |
| **Reverse-ETL** | `run`, `asset` (parquet/postgres/duckdb), `lineage` (lake/model→published) | wrap each publish/serve step in `ops.run` + declare its edge |
| **DuckLake catalog** | Version detail, snapshot attribution | already linked: `run.snapshot_after` + `commit_extra_info->>'run_id'` (ADR-0008) |
| **ClickHouse** | Logs | **federated** — referenced by `run_id`, never copied into the hub |

## The view (one pane)

Extend the operations dashboard (`backend/`) from `runs`+`snapshots` to
**asset catalog + lineage graph + run timeline**, fanning out by id:
- run/asset detail → ClickHouse `structured_logs` for logs;
- transform asset → SQLMesh for column-level lineage on drill-down.

## Open questions (flagged, not settled)

1. **SQLMesh lineage ingestion mechanics.** How/when does SQLMesh lineage reach
   `lake_ops.lineage` — a post-`sqlmesh run` sync via its Python API / CLI
   (`sqlmesh lineage`), reading its state DB, or a plan hook? At what cadence? This is
   the biggest unknown and blocks nothing until SQLMesh adoption starts — decide it
   *as part of* that adoption (ADR-0014 sequencing).
2. **Asset `ref` scheme.** One canonical locator grammar across types
   (`lake.<schema>.<table>`, `r2://...`, `postgres://<db>.<schema>.<table>`,
   `file://.../omicidx.duckdb`) so edges join cleanly. Needs pinning before edges are
   written.
3. **Version as its own entity?** Currently folded into `asset.current_version` +
   `run` snapshots. Promote to a `lake_ops.version` table only if per-version history
   beyond DuckLake snapshots is needed (e.g. published `vN` retention).
4. **Reverse-ETL: SQLMesh model vs. post-`sqlmesh` job** (per the EL/T-boundary
   discussion). Parquet exports likely fit as SQLMesh post-hooks (get lineage free);
   the A/B serving-Postgres swap likely stays a job that self-records to `ops`. Where
   each publish step lands decides which adapter row above it uses.
