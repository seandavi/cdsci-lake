# 0021. Plain-SQL transforms, stdlib DAG, sqlglot lineage owned by `lake_ops`

- Status: accepted
- Date: 2026-09-29

Supersedes ADR-0019 decision 3 (SQLMesh as the shared transform layer on one
state db) and decision 6 (environment/promotion workflow), and ADR-0014
decision 4 (column-level lineage federated to SQLMesh, "do not reinvent").
Keeps ADR-0019 decisions 1 (one internal DuckLake) and 2 (published products
independent of the lake). Tracking: cdsci-lake#110.

## Context

ADR-0019 adopted SQLMesh for cross-producer change propagation: a plan in one
repo repoints a dependent model in another. Its own review trigger was "no
model consumed cross-producer within two quarters." The value depended on
cross-producer model edges and SQLMesh-managed incrementals. As of 2026-09-29
neither exists:

- All 15 cdsci-lake models are `kind FULL`. The lake's incremental tables are
  EL tables advanced by `lake_ops` watermarks, not SQLMesh intervals.
- Every cdsci-lake model reads only cdsci-lake EL tables (`lake.bugsigdb`,
  `lake.ensembl`, `lake.ncbi_*`, `lake.ontology`, `lake.uniprot`). omicidx's
  38 SQLMesh models are all `kind VIEW`.
- The bioc-on-ice and cancer-on-ice pilots read around the shared lake, not
  through it (cdsci-lake#103, #104).

The ownership model has also firmed up: each producer owns its source tables
*and* its derived tables, and a producer consumes another's data by data
contract. Cross-project virtual-layer repointing (a plan in repo A moves repo
B's `prod` view) is the opposite of that rule.

Meanwhile the shared state carries real costs. Every participating repo must
run the same SQLMesh version against one state db (`sqlmesh>=0.236` is
unbounded today). A sync layer (#81, #85–#89) exists only to copy runs,
assets, lineage and snapshot attribution back into `lake_ops`, which is the
operational authority (ADR-0006, ADR-0013).

The ADR-0015 runner (`transform/models.py`, `graph.py`, `runner.py`,
`lineage.py`, about 400 lines) is still in the tree and already does
discovery, sqlglot dependency extraction, topological ordering, assertion
tests, and in-catalog snapshot attribution through `ops.run`.

## Decision

1. **Transforms are plain SQL files run by the in-repo runner.** A model is
   one `SELECT` in `transform/models/<schema>/<table>.sql`, with header
   directives (`-- description:`, `-- license:`, `-- column <name>:`,
   `-- materialized:`) and an optional sibling `<table>.test.sql` of
   zero-rows-to-pass assertions (ADR-0015 decision 1). The 15 ported models
   convert back and SQLMesh leaves cdsci-lake (#112). Each producer runs its
   own models. A read of another producer's table is an external input, not a
   graph edge. An upstream change reaches a downstream producer through its
   data contract and a scheduled rerun.

2. **The DAG is stdlib `graphlib`.** Edges come from sqlglot table references
   that resolve to another model. Order comes from
   `graphlib.TopologicalSorter` with an alphabetical tie-break so runs are
   reproducible. A cycle is an error (#111).

3. **sqlglot extracts lineage at two levels** (#113):
   - *table*: every table a model reads, external inputs included;
   - *column*: `(target_column ← source_table.source_column)` per resolvable
     output column.

   The runner passes the input tables' schemas from the lake catalog so
   `SELECT *` and unqualified columns resolve. Extraction is best-effort: an
   unresolvable column logs a warning and yields no edges. Lineage never
   fails a model run.

4. **`lake_ops` stores lineage; it is current-state per model** (#114).
   - Table level goes in the existing `lake_ops.lineage` with `edge_type =
     'sqlglot'` (the provider vocabulary of `docs/design/metadata_lineage.md`),
     `src_ref`/`dst_ref` in the `lake.<schema>.<table>` grammar.
   - Column level goes in a new table:

     ```sql
     lake_ops.column_lineage (
         dst_ref TEXT, dst_column TEXT,
         src_ref TEXT, src_column TEXT,
         run_id TEXT, recorded_at TIMESTAMPTZ
     )
     ```

     Uniqueness is `(dst_ref, dst_column, src_ref, src_column)`, enforced in
     code (ADR-0006 portability: no PK/FK).
   - On each successful model run the runner registers the model as an asset
     (`asset_type = 'lake_table'`, `writer` = the producer). It then
     **replaces** that model's rows: delete `sqlglot` edges and column rows where
     `dst_ref` is the model, then insert the fresh ones, all attributed to the
     run's `run_id`. Replacement is what makes a dropped dependency disappear.
     The insert-if-absent `record_lineage` stays for other edge types such as
     `publishes`.

5. **Lineage history lives in git and in releases, not in `lake_ops`.** The
   model SQL is versioned in git, so lineage at any commit is re-derivable.
   The lineage that matters publicly is frozen per release by the release
   builder's provenance projection. `lake_ops` therefore keeps only current
   state. Add append-only rows with `valid_from`/`valid_to` if a consumer
   needs internal lineage as-of a past run.

## Consequences

- #81 and its sync children (#86, #87, #88), #84 (environment staleness) and
  #90 (environment dimension) lose their premise and close as superseded. #85
  and #89's code is removed by #112. `lake_ops.snapshot_attribution` keeps its
  historic rows but gets no new writers.
- #82 flips direction: the runner stays and the SQLMesh port retires.
- No cross-producer change propagation. If one producer's derived table
  someday needs to rebuild automatically on another producer's model change,
  and a contract plus schedule can't carry it, reopen this decision.
- Gaps accepted with their triggers:
  - *run a subgraph* (`--select model+`): add when a full run is too slow;
  - *incremental transforms* (`-- materialized: incremental` with a `lake_ops`
    watermark and delete-then-insert over the new range, the EL pattern): add
    with the first derived table too large to rebuild;
  - *pre-prod build* (scratch schema, run tests, swap): add before another
    producer's derived tables depend on ours.
- Column lineage has no SQLMesh-grade guarantee on complex CTEs and window
  functions. It is observability, not a correctness gate.
- omicidx may keep SQLMesh locally as a producer-local choice. Its state
  schema and any `sqlmesh__*` physical tables in the shared catalog are not
  touched by this ADR; dropping them needs owner sign-off.

## Alternatives considered

- **Keep SQLMesh on shared state (ADR-0019).** Rejected for now. It buys
  cross-producer propagation nobody uses and that the ownership model
  forbids, and it costs version lockstep plus a sync layer.
- **SQLMesh per producer, no shared state.** Keeps the model DSL and audits,
  but still needs the sync layer back into `lake_ops` and adds a heavy
  dependency to get `CREATE OR REPLACE TABLE` over one engine (ADR-0015's
  original objection).
- **Append-only lineage history in `lake_ops`.** Deferred (decision 5): git and
  release provenance already answer "what was the lineage then".
