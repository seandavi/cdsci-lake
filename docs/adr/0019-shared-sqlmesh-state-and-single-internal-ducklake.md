# 0019. One internal DuckLake, one shared SQLMesh state: transform unification across producers

- Status: proposed
- Date: 2026-08-15

> Revisits ADR-0015's rejection of SQLMesh for this repo. ADR-0015 evaluated
> SQLMesh as a *single-project* transform engine; the multi-repo mode — which
> is where the unification value lives — was not considered. Settled in
> conversation after a local two-project spike; the deliberation is preserved
> as the decisions below. **Proposed, not accepted:** decision 3 has a
> prerequisite in a repo this ADR does not own.

## Context

ADR-0015 (2026-08-07) built `cdsci.lake.transform` — SQL files + `sqlglot`,
659 LOC — and rejected SQLMesh on three grounds: it is heavy for a DAG of
full-refresh `CREATE OR REPLACE TABLE` over one DuckDB engine; ADR-0014's `ops`
hub already covers the metadata need, so the transform layer need only produce
assets + lineage edges; and adopting it would fork "two producers, two
transform tools" one layer above ADR-0011's write-path convergence.

Three things have changed in the week since.

- **SQLMesh's multi-repo mode does more than record lineage — verified
  locally.** A two-project sandbox (`sqlmesh/`, two projects on one DuckLake
  catalog and one state db) showed that a change in repo A, **planned alone**,
  flags repo B's dependent model as an Indirectly Modified Child and repoints
  its view at the new snapshot; a subsequent plan of repo B reports no changes.
  Cross-repo references resolve without loading the other repo, binding to the
  snapshot-versioned physical table. That is cross-project *change propagation*,
  not just edge recording — the thing ADR-0015 assumed we could cheaply
  reproduce on `ops`. Reproducing it means fingerprints, a dependency resolver,
  and a repoint mechanism, none of which `lake_ops` has.
- **The lightweight layer is drifting toward SQLMesh's feature set.**
  `docs/design/transform_layer_capabilities_research.md` proposes borrowing, one
  at a time: inline column comments with SQLMesh's precedence rule, `grain`, a
  `depends_on` escape hatch, a gateway/catalog config shape, and external-model
  declarations. Five features, reimplemented piecemeal, is the argument against
  reimplementing them.
- **Publication has moved off the lake.** ADR-0015 already assumed icegate
  (§Consequences, `iceberg` target) and ADR-0018 made the published catalog the
  documented surface. The lake is the internal working surface; the public
  contract is what is explicitly published. SQLMesh's physical-layer schemas
  (`sqlmesh__*`) and per-environment virtual schemas are therefore internal
  clutter, not a contamination of anything a consumer sees. (Empirically:
  retiring an abandoned SQLMesh marts layer on 2026-08-14 was five schema drops
  and six expired snapshots, no data files — the clutter is cheap to reverse.)

What has **not** changed: incrementality still buys nothing here. Every
candidate model is a full re-run over one engine. That weight stays on the
"heavy" side of the ledger and this ADR does not pretend otherwise.

## Decision

1. **One internal DuckLake, explicitly.** The Postgres `lake` catalog + R2
   `cdsci-lake` is the single catalog for all internal EL and transform work,
   across every producer. A second internal lake requires its own ADR stating
   the rationale — "it seemed cleaner" is not one. This extends ADR-0007
   (single catalog, not per-source) from per-*source* to per-*purpose*: neither
   a new source nor a new processing stage nor a new consumer earns a catalog.
   The unification in decisions 2–3 is only coherent inside one catalog; two
   catalogs would silently reintroduce the federation problem being solved.
2. **Published data products are independent of the lake and of each other.**
   Publication to icegate/Iceberg is an explicit act with its own shape,
   lifecycle, and audience per product — a published product is not obliged to
   mirror the lake's schema layout, and two products need not be mutually
   consistent or co-versioned. This is what makes decision 1 affordable: one
   internal catalog does not impose one public shape. The lake's internal
   organization stops being a public-facing decision.
3. **SQLMesh becomes the shared transform layer across producers, on one state
   db**, with every participating project naming itself via `project:`.
   cdsci-lake's models port from SQL-files-plus-`sqlglot` to `MODEL (...)` DDL.
   Cross-producer dependency edges (cdsci-lake models reading omicidx marts,
   and the reverse) become real graph edges rather than a convention.
   **Prerequisite, and the reason this ADR is `proposed`:** omicidx's
   `transform/config.py` currently sets no `project=`. Until it does, a second
   project planning prod against that state *deletes omicidx's virtual layer* —
   see Consequences. That change is the omicidx owner's call under its
   `RUN-SCOPE.md` gate 6. **Nothing in this ADR is actioned before it lands.**
4. **Reverse-ETL stays ours.** SQLMesh does not subsume the publish step — the
   same conclusion omicidx reached (its parquet export stays a separate job) and
   `docs/research/sqlmesh-on-ducklake.md` reached before adoption.
   `transform/targets.py` (146 LOC, the `parquet`/`duckdb`/`lake_table`/
   `iceberg` adapters) and its ADR-0015 config-not-code contract survive intact,
   invoked after a SQLMesh apply. What retires on port is the machinery SQLMesh
   replaces: `graph.py` (72), `lineage.py` (66), `runner.py` (104), `models.py`
   (135) — the DAG, the `sqlglot` lineage, the executor, the file-discovery
   convention.
5. **ADR-0014's lineage contract is satisfied by federation, not by native
   production.** ADR-0014 §3 already names SQLMesh as a lineage *provider* and
   federates column-level detail by reference; this makes that the real path
   rather than the fallback. `lake_ops` remains authoritative for EL runs,
   watermarks, and the source registry — it is not replaced. Transform assets +
   asset-level edges sync into `ops` from SQLMesh state.
6. **Per-producer environments are the default; `prod` is the cross-producer
   contract surface.** Each producer plans into its own named environment.
   A model is promoted into `prod` **iff another producer depends on it** —
   `prod` is therefore small and curated, not "where things go". This is the
   same principle as decision 2 applied one layer down: membership of the
   shared surface is an explicit act, not a side effect of building something.
   No up-front knowledge of the dependency graph is required, because
   promotion is late and cheap (see Consequences).
7. **ADR-0012's deferred gateway trigger fires, as that ADR anticipated.**
   ADR-0012 named "SQLMesh is adopted for a producer's transforms" as trigger 2
   and said: define the lake connection once, in the file SQLMesh reads, shared
   with `lake_connect`. That is now the path. ADR-0015's opposite resolution
   ("no gateway file is needed") is superseded on this point only.

## Consequences

- **Planning `prod` is the dangerous act, not sharing state.** The spike's
  first stage: with no `project:` set, planning one repo alone reported
  `Removed Models:` for the other repo's models and dropped its entire virtual
  schema. Physical tables survived; only the views vanished — so recovery was a
  virtual-layer update with no backfill, but the window is a publish step
  reading nothing. SQLMesh's protection is guarded by `any(self._projects)`
  (`core/context.py:692,702`) — an unset name is `""`, which is falsy, so a
  single unnamed project silently disables the protection **for everyone**.
  Mitigation: a CI check asserting every participating config sets `project:`.
  Decision 6 narrows the exposure further: a producer that only ever plans its
  own environment cannot damage `prod` at all.
- **A `prod` model is executed by whoever plans `prod`.** Measured: repo_a made
  a breaking change and planned `prod` alone; repo_b's dependent model was
  flagged `Indirect Breaking` and **rebuilt by repo_a's plan** (`[full
  refresh]`). Under a shared `prod`, omicidx's scheduled apply would
  rebuild cdsci-lake's models — which for a cross-source model over `icite` /
  `openalex` / `pmc` is tens of millions of rows. That is shared *compute* and
  ambiguous ownership,
  not just shared blast radius, and it is the strongest argument for
  decision 6. Promotion into `prod` is the moment that coupling is accepted.
- **Promotion is a virtual-layer repoint, so decision 6 needs no foresight.**
  Measured: a model built in a named environment and then planned into `prod`
  reported `SKIP: No physical layer updates to perform` / `SKIP: No model
  batches to execute`, and both environments ended up pointing at the *same*
  physical object. Snapshots are shared state; environments select which
  versions are exposed. Getting the prod/not-prod call wrong therefore costs
  one plan, not a migration — which is why this ADR can defer the
  data-flow question instead of demanding it be answered up front.
- **A named environment pins its upstream versions.** Measured: after repo_a's
  breaking change, `prod` moved to a new physical object while the named
  environment stayed on the old one (different data, same model). Cross-project
  *references* still resolve — other projects' models are injected from `PROD`
  regardless of target environment (`get_environment(c.PROD)`, hardcoded) — but
  a producer adopts upstream changes when it re-plans, not when they ship. This
  needs a **staleness check**: a query against SQLMesh state that flags an
  environment pinned behind `prod`. Not new infrastructure; it does not exist
  yet either.
- **Practice is deliberately unsettled.** Decision 6 fixes the default and the
  promotion rule, not the day-to-day workflow — environment naming, who plans
  when, and how promotion is reviewed are expected to be learned by operating
  it. Revisit once there is real usage to generalize from; do not
  pre-specify a process here.
- **Adopting SQLMesh without the shared state is worse than not adopting it.**
  A second, isolated SQLMesh project buys zero unification and still pays the
  migration — two disconnected transform tools instead of two different ones.
  Decision 3 is all-or-nothing with its prerequisite; there is no useful
  partial adoption.
- **Migration cost:** 32 `*.sql` models across 7 directories port to `MODEL
  (...)` DDL. ADR-0018's describe-at-source fields map onto SQLMesh's native
  `description` / `column_descriptions` rather than a bespoke header
  convention — overlapping work, partly recovered rather than lost. The
  `models/<schema>/<table>.sql` → `<schema>.<table>` convention is replaced by
  explicit `name` in the DDL.
- **Incrementality remains unused.** Every model stays a full re-run; the
  plan/apply and virtual-environment machinery is carried for the cross-project
  graph, not for incremental evaluation. If that stays true indefinitely, this
  ADR is paying for one feature and using another — an honest, revisitable
  trade, not a hidden one.
- **`sqlglot`-derived best-effort lineage (ADR-0015's stated weakness) is
  replaced by SQLMesh's**, which is more battle-tested on CTEs and window
  functions and expands `SELECT *` where base tables are declared as external
  models.
- **A second internal lake now requires an ADR** (decision 1). This is a real
  constraint on future work, including work that would find a separate catalog
  locally convenient.
- **The `sqlmesh/` sandbox is throwaway.** It is a spike, gitignored data, not
  the migration target; it should be deleted or clearly marked once this ADR is
  accepted or rejected.

## Alternatives considered

- **Keep ADR-0015 as-is; build cross-project propagation on `lake_ops`.**
  Rejected as the primary path: fingerprints + dependency resolution + view
  repointing is precisely the machinery SQLMesh already ships, and the
  capabilities research shows the module drifting there one feature at a time
  anyway. Remains the fallback if the omicidx prerequisite is declined.
- **Adopt SQLMesh in cdsci-lake with its own separate state db.** Rejected —
  see Consequences: pays migration, buys no unification.
- **One shared `prod` for every producer's models** (this ADR's first draft).
  Rejected in favour of decision 6 once the spike showed that a `prod` plan
  executes the other producer's models and that promotion is free. Shared
  `prod` remains the right answer for a genuinely bidirectional dependency
  graph; it is the wrong default when the graph is mostly one-directional or
  simply unknown — which is the current situation.
- **Move cdsci-lake transforms into omicidx's existing SQLMesh project.**
  Rejected: collapses two repos' release cadences and ownership into one, which
  multi-repo exists specifically to avoid; also puts cdsci-lake models behind
  omicidx's `RUN-SCOPE` gates.
- **A second DuckLake catalog for transform outputs**, keeping the EL catalog
  clean. Rejected by decision 1 — it reintroduces cross-catalog federation to
  solve an aesthetic problem that ADR-0018 and icegate already solved by making
  publication explicit.
