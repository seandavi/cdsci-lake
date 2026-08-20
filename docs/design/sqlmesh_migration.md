# Migrating cdsci-lake's transform layer to SQLMesh

Execution plan behind **ADR-0019**. Covers the model port, the lineage
transition, and what happens to `lake_ops`. Assumes ADR-0019 is accepted; the
phases are gated so an early phase can be abandoned without stranding work.

## Starting position (verified 2026-08-15)

- `cdsci.lake.transform`: 659 LOC across 6 modules; **32** `*.sql` models in 7
  directories (`bugsigdb`, `ensembl`, `ncbi_gene`, `ncbi_gene2accession`,
  `ncbi_gene2go`, `ncbi_gene2pubmed`, `ref`, `uniprot`).
- `lake_ops` in the Postgres catalog has **four** tables: `run`, `source`,
  `watermark`, `dataset_contract`. ADR-0014's `asset` / `lineage` / `version`
  tables **do not exist yet**. The lineage transition is therefore greenfield —
  there are no existing lineage rows to migrate, only a contract to satisfy.
- omicidx's SQLMesh: 38 `kind VIEW` models, `prod` only, state in the lake
  Postgres `sqlmesh` schema, `project="omicidx"` added 2026-08-15 (uncommitted).

## Phase 0 — prerequisite and safety net

Gate for everything else. Nothing in Phase 1+ starts until this is green.

1. ~~**Land `project="omicidx"`**~~ — **done.** It is on omicidx `main` (in
   `372311c`) and live in the shared state: prod's 50 snapshots now carry
   `project='omicidx'` alongside the 50 older unnamed ones.
2. ~~Expect a full re-fingerprint on omicidx's next plan.~~ **Wrong — it was a
   metadata-only change.** This step predicted new physical objects for all 38
   models plus a pile of orphans. What actually happened when omicidx's timer
   next ran: `change_category: 6` (metadata), `version` and `dev_version`
   unchanged, `physical_schema` unchanged. Verified afterwards — still exactly
   45 physical views across `sqlmesh__{src,stg,sradb,geometadb}`, no
   duplication, and `sradb.*`/`geometadb.*` resolving normally.
   Adding a project name touches `metadata_hash`, not `data_hash`, so nothing
   rebuilds and nothing is orphaned. Kept here because the reasoning that
   produced the wrong prediction — "a fingerprint change means new physical
   objects" — is worth not repeating: check the change category first.
3. **CI check: every participating config sets `project:`.** One unnamed
   project silently disables the cross-project guard for *everyone*
   (`any(self._projects)`, `core/context.py:692,702`). This is a one-line
   assertion and it is the only thing standing between a routine plan and a
   deleted virtual layer. It belongs in both repos.

## Phase 1 — port the 32 models

Mechanical, one model at a time, verifiable per model. The current header
convention maps almost completely onto `MODEL (...)`:

| today | SQLMesh |
|---|---|
| path `models/<schema>/<table>.sql` | `name <schema>.<table>` (explicit) |
| `-- description: …` | `description '…'` |
| `-- column <name>: …` | `column_descriptions (<name> = '…')` |
| `-- materialized: view` | `kind VIEW` |
| default (table) | `kind FULL` |
| `<name>.test.sql` zero-row assertions | `AUDIT` — same zero-row semantics, native |
| `-- license: …` | **no native field** — see below |

`license` is the one field with no clean home. Options, in order of laziness:
fold it into `description` (a prefix, greppable, zero machinery); or `tags`
(`tags (license__cc0)` — filterable but stringly-typed); or keep a
project-level YAML keyed by model name. Decide once, apply to all 32. Do **not**
invent a MODEL extension for it.

Order of work: `ncbi_gene2accession/mapping.sql` first — with
`ref/id_crosswalk.sql` retired (2026-08-15, unused — see `docs/ROADMAP.md`) it is
the best remaining DAG exerciser. If it ports cleanly the rest are rote.

**Never run a bare `sqlmesh create_external_models` on shared state.** Our
context loads ~65 models — our own plus every model injected from prod — so the
command introspects omicidx's upstreams too and writes them into *our*
`external_models.yaml`. The next plan then proposes re-labelling omicidx's
external models `project cdsci_lake`. Caught in a plan preview during the port;
the fix is to prune the file to the tables our own models actually read. The
same "the context is bigger than this repo" trap applies to anything that
enumerates models — see the scope filter required in #85.

**Everything ports into cdsci-lake's own environment** (ADR-0019 §6), not
`prod`. Nothing is promoted during this phase: no other producer depends on
these models today, and promotion is a one-plan operation available at any
later point. This also keeps Phase 1 unable to affect omicidx's virtual layer
at all, which is what makes it safe to do incrementally.

**Consumer-visible change, and the reason this phase needs a decision, not just
typing.** Today a transform model materializes as a real DuckLake table. Under
SQLMesh it becomes a *view* in the virtual layer over a fingerprinted physical
table. Reads of `lake.<schema>.<table>` keep working, but: the physical name
churns on every model change, and per-table DuckLake time-travel now points at
objects whose lifetime SQLMesh controls. Anything depending on the physical
identity of these tables — a published Iceberg product, a bookmarked snapshot
id — must be checked before cutover, not after.

## Phase 2 — lineage and `lake_ops`

The substantive phase. Principle from ADR-0019 §5: **`lake_ops` is not
replaced; it stops being the producer of transform lineage and becomes its
consumer.**

Division of authority after migration:

| concern | authority |
|---|---|
| EL runs, watermarks, source registry | `lake_ops` (unchanged, ADR-0006) |
| transform model definitions, fingerprints, dependency graph | SQLMesh state |
| column-level lineage | SQLMesh state, **federated by reference** (ADR-0014 §3) |
| asset registry + asset-level edges across EL *and* T | `lake_ops` (to build) |
| dataset contracts, published products | `lake_ops` (unchanged) |

Work items:

1. **Build ADR-0014's `asset` + `lineage` tables** in `lake_ops`. Greenfield —
   no migration. Shape per `docs/design/metadata_lineage.md`, with
   `asset_type ∈ {lake_table, sqlmesh_model, parquet, postgres, duckdb, iceberg}`.
2. **A post-apply sync step** reads SQLMesh state and upserts into
   `lake_ops.asset` + `lake_ops.lineage`: one asset row per model
   (`asset_type='sqlmesh_model'`, carrying project, fingerprint, physical
   object name), one asset-level edge per dependency. Idempotent on
   `(project, model_name, fingerprint)` so re-running syncs nothing new.
   This is the "sync step" `metadata_lineage.md` left as its biggest open
   question; the answer is **post-apply hook reading state**, not a plan hook
   and not a `sqlmesh lineage` CLI scrape.
3. **One `lake_ops.run` row per apply**, bracketing the SQLMesh apply the same
   way ingestors bracket a load — so the transform stage appears in the same run
   timeline as every EL source and the ADR-0014 dashboard needs no special case.
4. **Do not mirror column-level lineage into `lake_ops`.** ADR-0014 §3 and
   ADR-0019 §5 both say federate: store the reference, drill down into SQLMesh.
   Copying it means a second copy that goes stale between applies.
5. **Retire `sqlglot` lineage** (`transform/lineage.py`) only once the sync
   produces edges for every ported model — the two can coexist for one cycle
   and be diffed against each other. That diff is the acceptance test for this
   phase, and it is worth doing: ADR-0015 flagged the `sqlglot` path as
   best-effort on `SELECT *` and gnarly CTEs, so disagreements are expected and
   informative rather than alarming.

## Phase 3 — reverse-ETL stays ours

`transform/targets.py` (146 LOC) and its config-not-code contract survive
untouched (ADR-0019 §4). It moves from "invoked by our runner" to "invoked
after a SQLMesh apply". Publishes register in `lake_ops` exactly as they do
today. The `postgres` A/B-slot caveat and the `iceberg` merge-on-read caveat
from ADR-0015's consequences carry over unchanged — SQLMesh changes nothing
about either.

## Phase 4 — retire the replaced machinery

Delete once Phases 1–3 are verified, not before: `graph.py` (72),
`lineage.py` (66), `runner.py` (104), `models.py` (135) — 377 LOC. Keep
`targets.py`. `__main__.py` (124) shrinks to a publish/sync CLI; model
execution becomes `sqlmesh plan`. Drop the `sqlglot` dependency from the
`[transform]` extra if nothing else uses it.

## Rollback

Through Phase 2 the old path is intact: the `*.sql` files remain, and
`python -m cdsci.lake.transform` still rebuilds tables. Rollback is "stop
running plan, resume running the runner" plus dropping the SQLMesh-created
virtual views. After Phase 4 rollback means restoring deleted modules from git —
so Phase 4 is the point of no return and should trail Phase 3 by at least one
full cycle.

## Maintenance interaction (do not skip)

SQLMesh creates a new physical object per fingerprint and relies on its
**janitor** to drop expired ones. On DuckLake every such create/drop is
snapshots + data files. The lake already accumulates orphans fast enough to
need `vacuum` (238 files reclaimed on 2026-08-14). After migration, the routine
becomes: SQLMesh janitor first (drops dead physical objects), then
`maintenance_cli vacuum` (expires + reclaims). Running vacuum alone will
under-collect; running it before the janitor wastes the pass.

## Open questions

1. **`license` field placement** — decide in Phase 1 (options above).
2. ~~Does cdsci-lake share omicidx's `prod` environment?~~ **Resolved** by
   ADR-0019 decision 6: cdsci-lake plans its own environment; models are
   promoted into `prod` only when another producer depends on them. Promotion
   is a virtual-layer repoint (measured: `SKIP: No model batches to execute`),
   so this is revisitable per model at any time.
3. **Who runs the apply?** Simpler than first written: omicidx retired Prefect
   (omicidx `372311c`, "Excise Prefect: retire the worker, schedule everything
   on systemd timers"), so both producers now schedule the same way —
   `SCHEDULING.md`'s systemd timer + ntfy convention. Per-producer environments
   mean each timer applies to its own environment, so no lock is needed for the
   common case; two timers can still both plan `prod` once models are promoted.
   Needs an ordering story *before* the first promotion, not before Phase 1.
4. ~~`ref.id_crosswalk` physical identity~~ — **moot**: the table was retired
   2026-08-15 (unused). The general form still applies to whichever models get
   promoted to `prod`: confirm no published product depends on a model's
   physical identity before cutover.
