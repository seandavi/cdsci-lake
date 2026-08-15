# SQLMesh-on-DuckLake multi-repo sandbox

Throwaway experiment: two SQLMesh projects sharing **one** DuckLake catalog and
**one** state db, entirely local. Answers whether cdsci-lake could run its own
SQLMesh project alongside omicidx's against the shared lake (ADR-0015 context).

```
catalog.ducklake   shared DuckLake metadata      (gitignored)
lake_data/         shared Parquet                (gitignored)
state.db           shared SQLMesh state          (gitignored)
repo_a/            stock `sqlmesh init` example  → sqlmesh_example.*
repo_b/            second project                → repo_b_example.*
```

Run (no repo dependency added — uvx pulls SQLMesh 0.236.1 on demand):

```bash
cd sqlmesh
uvx --from "sqlmesh[duckdb]" sqlmesh -p repo_a plan --auto-apply
uvx --from "sqlmesh[duckdb]" sqlmesh -p repo_b plan --auto-apply
uvx --from "sqlmesh[duckdb]" sqlmesh -p repo_b fetchdf "SELECT * FROM repo_b_example.item_report"
```

Both configs use **absolute** paths for `path`/`data_path`/`database`. DuckLake
stores the data path in the catalog and rejects an attach whose `DATA_PATH`
disagrees, so a relative path only works from one cwd — with two repos planned
from a parent directory, that breaks.

## Findings

1. **Without `project:`, a shared state db is destructive.** Planning repo_b
   alone reported `Removed Models: sqlmesh_example.*` and dropped repo_a's whole
   virtual schema. The physical tables (`sqlmesh__sqlmesh_example.*`) survived —
   only the views vanished. This is not a SQLMesh bug: `context.py:692,702`
   guards the preserve-other-projects logic with `any(self._projects)`, and the
   default project name is `""`, which is falsy.

2. **With `project:` on both, planning one repo alone preserves the other.**
   SQLMesh loads prod's snapshots and re-injects those owned by projects not in
   the current context. Recovery was free — because the physical layer had
   survived, restoring repo_a was a virtual-layer update with no backfill.

3. **Cross-repo references resolve without loading the other repo.**
   `repo_b_example.item_report` joins `sqlmesh_example.full_model` (repo_a's)
   and plans fine from `-p repo_b` alone.

4. **Cross-repo edges bind to the snapshot-versioned physical table**, e.g.
   `sqlmesh__sqlmesh_example.sqlmesh_example__full_model__635791289`, not the
   view — and the cascade works. Adding a column to repo_a's model and planning
   **repo_a alone** flagged repo_b's model as an Indirectly Modified Child and
   repointed its view at the new snapshot (`…__2373126298`). Afterwards
   `-p repo_b plan` reports no changes: the two repos stayed consistent without
   ever being loaded in the same context.

## What this implies for the real lake

omicidx's `transform/config.py` sets **no** `project=`, so finding 1 applies to
it as written: a second project planning prod against that state would remove
`sradb.*`/`geometadb.*` from the virtual layer — the exact views
`parquet_export.py` publishes. Prerequisite for any shared-state experiment is
`project="omicidx"` there first (owner's call — omicidx `RUN-SCOPE.md` gate 6).
