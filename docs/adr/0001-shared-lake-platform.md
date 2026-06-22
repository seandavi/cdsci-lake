# 0001. cdsci-lake: the shared research-data lake platform

- Status: accepted
- Date: 2026-06-22

> Charter ADR for this repo. The deliberation that produced it lives in the
> `cu-research-intelligence` repo as ADR-0022 (substrate), ADR-0023 (package +
> thin orchestrator) and ADR-0024 (extract to an internal repo). This records the
> settled decisions for `cdsci-lake`.

## Context

Internal research projects (the UCCC cancer-center analytics, future ones) need
the same canonical corpora — NIH RePORTER, iCite, OpenAlex subsets, PMC,
ClinicalTrials.gov — alongside what omicidx already publishes (pubmed, SRA/GEO).
Re-fetching per project is wasteful and inconsistent. A shared DuckLake already
exists (Postgres catalog `lake` + R2 `cdsci-lake`, omicidx-published,
schema-namespaced). The ingest code began inside a consumer's repo, which is the
wrong home: it is infrastructure with multiple consumers.

## Decision

1. **One shared lake, converge on it.** A single DuckLake — Postgres catalog
   (`lake`) + Cloudflare R2 data (`cdsci-lake`). No sibling catalog, no
   federation. This repo and omicidx are peer **publishers** into it; projects
   are **consumers**.

2. **This repo is the lake platform, separate from any consumer.** It ships a
   **read client** (base install: `lake_connect`, config) and the **ingestors**
   (`[ingest]` extra). Consumers depend on the read client and *assume the data
   exists*. `cdsci` is a PEP 420 namespace so sibling `cdsci.*` packages may live
   in other repos.

3. **Per-source silver schemas are the contract.** Each source publishes a
   schema (`icite.*`, `reporter.*`, …) of typed, curated tables exposed via
   **versioned views**, so upstream churn doesn't break consumers. **Bronze** =
   raw downloads on R2/local, unregistered. **`ref`** schema = cross-source
   crosswalks. **Gold** (cohorts, entity resolution) stays in consumer projects —
   never in a source schema or the lake.

4. **Time-travel-friendly writes.** Sources **MERGE-upsert** on a natural key,
   updating only when a non-key column differs — never bulk `CREATE OR REPLACE`.
   Each snapshot records real deltas; an unchanged re-run adds none.

5. **Credentials from Google Secret Manager** (`cdsci-infra`), via `gcloud`.
   Scoped Postgres roles — `lake_writer` (ingest) and `lake_reader` (consumers) —
   plus read-scoped R2 tokens; the bootstrap admin credential is replaced once
   roles exist.

6. **Own the operational state; keep the orchestrator thin.** Ingestors are
   plain, idempotent, CLI-invokable functions (no framework coupling); retries
   live in the HTTP client, data-state in DuckLake snapshots. A small Postgres
   `lake_ops` model (sources, versions, runs→snapshot ids, watermarks, contracts)
   is the operational ledger. Scheduling is the simplest mechanism near the data.

## Consequences

- Clean ownership/lifecycle; write creds confined to this repo, read creds to
  consumers. Contract discipline (versioned views + a `dataset_contract`
  registry) becomes mandatory. Another repo + CI, but a sibling to omicidx — a
  pattern already run. Low lock-in: DuckLake data is plain Parquet; the catalog
  and `lake_ops` are backup-critical.

## DuckLake maintenance

Writes accumulate snapshots and data files. Cleanup is **two steps**: expire old
snapshots (`ducklake_expire_snapshots`) to make their files unreferenced, then
delete the now-unused files (`ducklake_cleanup_old_files` / delete orphaned
files). See `cdsci.lake.maintenance` and `docs/design/ducklake-maintenance.md`.
