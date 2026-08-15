# Transform layer: capabilities worth borrowing from dbt and SQLMesh

- Status: research, not a decision
- Date: 2026-08-11

> Scoped deliberately narrow: ADR-0015 rejected adopting either tool and ADR-0012
> deferred a gateway config file. Neither is relitigated here. The question is
> which *capabilities* — not tools — close a gap that already exists in
> `cdsci.lake.transform`, at a cost proportionate to a DAG of five DuckDB-native
> SQL files. Everything below was checked against current dbt/SQLMesh docs (URLs
> at the end) and against the code in this repo; the one feasibility claim that
> mattered (`sqlglot` comment attachment) was verified by running it.

## 1. Comments as documentation

**dbt.** Descriptions live in `schema.yml` (`description:` on a model and on each
column) and are documentation-only by default. `persist_docs` — **off by
default**, configurable per model via the inline `config()` block — pushes them
into the database as relation and column `COMMENT`s (Postgres, Redshift,
Snowflake, BigQuery, Databricks, Spark; not implemented for sources). `dbt docs
generate` emits `manifest.json` (the project as dbt parsed it) plus
`catalog.json`, whose contents come from *querying the warehouse back*: per node
a `metadata.comment` and `columns[].{name, type, comment, index}`. The static
`index.html` docs site is a consumer of those two JSON files, not the source of
them.

**SQLMesh.** `description` and `column_descriptions` are fields in the `MODEL
(...)` DDL block. The notable behavior: **if `column_descriptions` is absent,
SQLMesh detects comments in the query's column selections and registers each
column's final comment with the engine** — the docs live next to the expression
they describe, not in a header block. Registration is on by default
(`register_comments`), to physical tables and prod-environment views only, and
is skipped on engines with no comment support.

**cdsci-lake today.** `models.py` parses `-- description:`, `-- license:`, and
`-- column <name>:`; `runner.py` stamps them via `ops.register_sources` and
`ops.ensure_column_comments`. That is dbt's `persist_docs` half already built,
and on by default rather than opt-in. Two things are missing.

**Proposal 1a — inline column comments (borrow SQLMesh's precedence rule).**
`-- column <name>:` restates the column name and sits far from the SELECT that
produces it: `models/bugsigdb/signature_taxon.sql` has its column directive on
line 3, ~40 lines above the projection. Verified locally on this repo's pinned
`sqlglot` 30.15.0: a trailing `--` or `/* */` comment on a projection is
attached to that expression as `Select.expressions[i].comments`. So detecting
them is a few lines against a parse the module already performs, merged with the
header directives on the same precedence SQLMesh uses (explicit header wins).
**Cost: ~10 lines in `models.py`, no new dependency, no change to `runner.py`.**

**Proposal 1b — an artifact, but not a docs site.** Nothing reads the comments
back out today. The lesson from `catalog.json` is that the artifact is a
*warehouse query*, not a build system — cdsci-lake's equivalent is a
`duckdb_columns()` / `duckdb_tables()` query, and the natural surface is a panel
in the ops dashboard already under construction (`backend/repository.py` has no
comment query today), not a generated static site. A `transform docs` CLI
emitting Markdown/JSON is the fallback if the dashboard slips. **Cost: ~15 lines
either way. Do it after the dashboard lands, not before.**

Note that `-- license:` has no counterpart in either tool. It is a real
cdsci-lake-specific field (ADR-0015 wayfinding: schema-based license inference is
actively wrong) and should not be dropped for parity with anything.

## 2. Model specification

**dbt.** An inline `{{ config(...) }}` block or `.yml` carries `materialized`,
`tags`, `meta`, `alias`, `persist_docs`, hooks, `grants`. Tests are separate yml
entries. Contracts (`contract: {enforced: true}`) require a `name` and
`data_type` for **every** column — partial contracts are rejected — plus optional
`constraints` (`not_null`, `primary_key`, `unique`, `foreign_key`, `check`);
checked at build time by both a preflight column/type comparison and by emitting
the constraints into the DDL.

**SQLMesh.** The `MODEL (...)` DDL block is the whole specification: `kind`,
`cron`/`cron_tz`, `interval_unit`, `start`/`end`, `owner`, `project`, `tags`,
`grain`/`grains`, `references`, `depends_on`, `audits`, `columns`, `enabled`,
`gateway`, `partitioned_by`, `table_format`, `physical_properties`, `signals`,
`stamp`. Built-in audits are parameterized calls in that block —
`audits (not_null(columns := (id)), unique_values(columns := (id, item_id)),
accepted_values(column := status, is_in := (...)))` — blocking by default, with
`blocking := false` to downgrade to a notification.

**cdsci-lake today.** `_DIRECTIVE` covers `description|license|materialized`;
`_COLUMN_DIRECTIVE` covers per-column comments; `_TEST_BLOCK` parses named
zero-rows-to-pass assertions out of a sibling `<name>.test.sql`.

**Proposal 2a — `-- grain: <cols>`. The one real gap.** Every `.test.sql` file in
the repo today — `bugsigdb.{study,experiment,signature,signature_taxon}` (and
`ref.id_crosswalk` before it was retired) — is the same hand-written uniqueness
check:

```sql
-- test: bsdb_id is unique
SELECT bsdb_id FROM lake.bugsigdb.signature GROUP BY bsdb_id HAVING count(*) > 1
```

That is SQLMesh's `grain` (and dbt's `unique` test) retyped per model, 100% of
the test surface. A `-- grain: bsdb_id, member_index` directive that synthesizes
the identical query into `Model.tests` at load time changes nothing downstream —
same zero-rows contract, `runner.py` and `_run_tests` untouched — and deletes
five files. It also yields a machine-readable key declaration that ADR-0016's
`scripts/lint_id_columns.py` and ADR-0017's staging layer can both read, which a
hand-written SQL assertion never will. **Cost: ~15 lines in `models.py`; net
deletion.**

**Proposal 2b — `-- depends_on: <target>`.** `graph.py` records an edge only for
refs `sqlglot` resolves to two dotted parts; bare names and table-valued
functions (`read_parquet(...)`) are dropped as external leaves. That is correct
for a model reading an external Parquet file, but a model that reaches another
model through a function or a non-literal path gets **no edge and no error** —
just a wrong topological order. SQLMesh's `depends_on` is precisely this escape
hatch. **Cost: ~5 lines (`models.py` field + one set union in
`_model_dependencies`).** Caveat: no model needs it today, so this is insurance,
not a fix. Land it with 2a or when the first case appears.

**Deliberately not proposed.**

- **Contracts / typed column declarations.** Heavy (every column, every type) and
  aimed at a problem this layer doesn't have: contracts protect consumers across
  incremental migrations, but every model here is `CREATE OR REPLACE` + full
  re-run, so a shape change is a rebuild, not a migration.
- **`not_null` / `accepted_values` audits.** Unlike uniqueness there is no
  existing boilerplate to delete, and `-- test:` already expresses either in one
  line. YAGNI until a repeated shape appears.
- **`cron` / `owner`.** Scheduling is systemd `--user` timers per
  `monode/infrastructure/SCHEDULING.md` and ADR-0001 §6; a `cron` field in a
  model file would be a second, non-authoritative copy. `owner` is noise in a
  single-maintainer repo.
- **`tags`.** Only earns its place once `run-all` needs to execute a subset —
  the trigger would be a second timer with a different cadence, which doesn't
  exist yet.
- **`signals`.** Readiness gating over *time intervals*; these models are
  full-refresh with no intervals, and upstream readiness is already the ops
  run's business.

## 3. Config for gateway flexibility

**dbt.** `profiles.yml` (recommended location `~/.dbt/`, deliberately outside the
project): a profile contains `outputs:` — one block per environment, each with an
adapter `type` and its credentials — plus a default `target:`, overridden per run
with `--target` / `--profile`.

**SQLMesh.** `gateways:` maps a name to up to four connections — `connection`
(the warehouse), `state_connection` (SQLMesh's own metadata; the docs recommend a
separate Postgres in production because "storing state in data warehouses can
slow down your project or produce corrupted data"), `test_connection`,
`scheduler` — plus `default_gateway`, `--gateway` per command, and a per-model
`gateway` property. The DuckDB connection type adds `catalogs:` — a **named map
of ATTACH targets**, plain paths or typed external ones — with the first entry as
the default catalog, plus `extensions:` and `secrets:`:

```yaml
gateways:
  my_gateway:
    connection:
      type: duckdb
      catalogs:
        memory: ':memory:'
        postgres:
          type: postgres
          path: 'dbname=postgres user=postgres host=127.0.0.1'
          read_only: true
```

**Finding: no gap. ADR-0012 stands, and the research reinforces it.** Both files
exist to solve two problems this repo does not have — several execution
engines/dialects behind one project, and a state store that must be separate
from the warehouse (SQLMesh's `state_connection` has no analogue here; `lake_ops`
*is* the state and it is already where it belongs). The structural idea worth
having from both is the split between *what a model is* (in the project) and
*where it connects* (outside it), and cdsci-lake already implements exactly that
with `Settings` + `get_secret`: `__main__.py`'s `publish` command takes
`namespace`/`table` as arguments and reads endpoint, catalog, and token from
settings. Neither ADR-0012 trigger has fired.

**One thing to record for trigger time.** When trigger 1 (a frozen-snapshot /
federation read target) fires, the shape to copy is SQLMesh's DuckDB `catalogs:`
map — a name → ATTACH spec dictionary — not dbt's profile/target/outputs nesting.
It is ATTACH-shaped, which is what this repo's targets literally are
(`targets.py` already ATTACHes under hardcoded aliases `_publish_target` and
`_publish_ice_cat`), and it extends to read targets without inventing an
environment concept. **Cost now: zero — add one sentence to ADR-0012's revisit
note. Build nothing.**

## 4. Reverse-ETL config and format

**dbt.** `exposures` are yml declarations of downstream consumers: `name`,
`type` (`dashboard|notebook|analysis|ml|application`), `owner`, `depends_on`
(refs/sources/metrics), plus optional `label`, `url`, `maturity`, `description`,
`tags`, `meta`. They are documentation and a selector (`dbt run -s
+exposure:weekly_jaffle_metrics` builds everything feeding it). **dbt publishes
nothing** — reverse-ETL is left entirely to third-party vendors.

**SQLMesh.** `external_models.yaml` (`name`, `description`, `columns`, generated
by `sqlmesh create_external_models` from engine metadata) describes *inbound*
tables SQLMesh doesn't manage — the opposite direction from publishing. Python
models can write anywhere but are arbitrary code, not config. Signals gate
readiness. **Neither is a publish-target declaration either.**

So: no prior art to copy for the *mechanism*. ADR-0015 §3 is ahead of both tools
here, and nothing found argues against its direction.

**Proposal 4a — borrow the exposure's *split*, as a directive.** What exposures
get right is that the declaration names the consumer and the dependency and
carries **no credentials**. Applied here, the per-model half is small enough for
a flat one-line directive, because everything else is per-environment and already
lives in `Settings`:

```sql
-- publish: iceberg namespace=annotation table=signature_taxon
-- publish: parquet path=s3://…/v{date}/t.parquet latest=s3://…/latest/t.parquet
```

Repeated lines are multiple targets. This covers every field of
`targets.Target.config` that actually varies per model, needs no YAML sidecar
(ADR-0012's constraint holds), and requires no change to `Target` or `publish()`
— only a parse in `models.py` and a loop in the CLI. It replaces a `publish`
command that today can express exactly one hardcoded target type. **Cost: ~20
lines.**

**On ADR-0015's two named gaps.**

- **Postgres A/B slots.** Neither tool ships a column-mapping/DDL schema for
  reverse-ETL, because neither does reverse-ETL. The closest transferable
  artifact is dbt's contract block — per-column `name` + `data_type` +
  `constraints`, emitted into DDL — which *is* the declarative DDL schema
  ADR-0015 says is missing. Worth borrowing **scoped to the postgres target's
  own config** if and when that target is built; explicitly not worth adopting
  as a whole-model contract (see §2). No change now.
- **Iceberg CREATE-if-absent + MERGE.** SQLMesh's `table_format: iceberg` reaches
  the same end state but through its own snapshot/state machinery, none of which
  transfers. `targets.py`'s create-if-absent + `DELETE`/`INSERT` remains the
  right primitive for a full refresh under DuckDB's merge-on-read semantics.

**Unrelated bug found while reading** (flagging, not proposing):
`__main__.py:117` calls `publish(con, f"{LAKE}.{target}", icegate_target)`, but
`targets.publish()` declares a required keyword-only `date`. The `publish` CLI
command raises `TypeError` before reaching the adapter.

## Recommendation, in priority order

1. **`-- grain: <cols>`** (§2a) — best value per line in the whole report. ~15
   lines, deletes all five `.test.sql` files, and hands ADR-0016's id lint and
   ADR-0017's staging layer a machine-readable key they can't get from a
   hand-written assertion.
2. **Inline column comments** (§1a) — ~10 lines, verified feasible on the pinned
   `sqlglot`; kills the name duplication and the drift between a header block and
   the SELECT it describes.
3. **`-- publish:` directive** (§4a) — makes ADR-0015 §3 real for the two targets
   that already work, with connection config staying in `Settings`.
4. **`-- depends_on:`** (§2b) — trivial insurance against a silently wrong
   topological order; land with 1 or on first need.
5. **Docs artifact as a dashboard panel over `duckdb_columns()`** (§1b) — after
   the dashboard lands. No static site, ever.
6. **Nothing for gateway config** (§3) — add one sentence to ADR-0012 naming
   SQLMesh's DuckDB `catalogs:` map as the trigger-time shape.

Explicitly not recommended: model contracts, `cron`/`owner`/`tags`, signals,
audits beyond grain, external-model YAML, and any generated documentation site.
Separately, fix the `publish()` `date` `TypeError`.

## Sources read

- https://docs.getdbt.com/reference/resource-configs/persist_docs
- https://docs.getdbt.com/reference/artifacts/catalog-json
- https://docs.getdbt.com/docs/collaborate/govern/model-contracts
- https://docs.getdbt.com/docs/build/exposures
- https://docs.getdbt.com/docs/core/connect-data-platform/profiles.yml
- https://sqlmesh.readthedocs.io/en/stable/concepts/models/overview/
- https://sqlmesh.readthedocs.io/en/stable/concepts/models/external_models/
- https://sqlmesh.readthedocs.io/en/stable/concepts/audits/
- https://sqlmesh.readthedocs.io/en/stable/guides/configuration/
- https://sqlmesh.readthedocs.io/en/stable/guides/signals/
- https://sqlmesh.readthedocs.io/en/stable/integrations/engines/duckdb/
- https://www.tobikodata.com/blog/metadata-everywhere (inline column comment behavior)
