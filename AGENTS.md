# AGENTS.md

Conventions for AI agents and humans working on `cdsci-lake` and the coordinated DuckLake publication program.

## What this project is

`cdsci-lake` is the shared **private authoring platform**: one internal DuckLake, producer write contracts, operational state, transforms, lineage providers, and release-building mechanics. Independent producers own their source code and schemas; consumers depend on data contracts rather than importing ingestors.

The public products remain separate:

- `bioc-on-ice` owns biological schemas, identifiers, source/taxon/genome scopes, and biological acceptance.
- `cancer-on-ice` owns suppression, geography, measures, temporal interpretation, aggregate-only policy, and cancer acceptance.
- DuckDock will own public release discovery, introspection, verification, and Frozen DuckLake access.
- `icegate` remains the optional stateless Iceberg REST auth/routing/credential-vending gateway.

## Sources of truth

Read these before changing architecture or shared contracts:

1. `CONTEXT.md` — canonical project vocabulary.
2. Accepted ADRs under `docs/adr/` — decisions and rationale.
3. `docs/design/scientific-publication-platform.md` — integrated **draft** proposal; it is not normative until its decisions land in accepted ADRs/specifications.
4. GitHub program map [cdsci-lake#95](https://github.com/seandavi/cdsci-lake/issues/95) — authoritative work state and cross-repository dependencies.

When code, documentation, and an accepted ADR disagree, stop and surface the contradiction. Do not silently choose the newest-looking document. Proposed ADRs are proposals, not authority.

## Hard architecture rules

- **Internal DuckLake writes use DuckDB/DuckLake.** `upsert_latest_snapshot` is the internal silver-table model; history depends on retained DuckLake snapshots.
- **Production public Iceberg writes use PyIceberg only.** Do not use DuckDB `DELETE`, `UPDATE`, `MERGE`, or `CREATE OR REPLACE` against a production public Iceberg table. The current DuckDB Iceberg target is not a pattern to extend.
- **Public release semantics are format-neutral.** Immutable manifest + Arrow-compatible schema + Parquet define a release; Frozen DuckLake is the primary query adapter and Iceberg is optional interoperability.
- **Temporal models are explicit.** Name `append_immutable`, `upsert_latest_snapshot`, `scd2_release`, or approved `scd2_bitemporal`; never describe a table only as “versioned.”
- **Domain meaning stays domain-local.** Shared modules own mechanics and validation, not biological or cancer schema values.
- **DuckDock never exposes the mutable internal lake.** Public artifacts may not contain private bucket locations, local paths, credentials, restricted assets, watermarks, or raw logs.
- **icegate stays protocol-specific.** Do not add persistent registry, lineage, scheduler, or publication state to the transparent Iceberg proxy.

## Coding rules

- Lazy and minimal: implement the smallest complete issue; deletion beats speculative abstraction.
- New sources are source-owned SQL/modules, not plugins in a universal ingestion framework.
- A shared seam must have at least two real consumers or a production plus test adapter.
- Keep the base package usable as a read client; put heavy producer/publication dependencies behind extras.
- Non-trivial behavior requires an offline fixture test.
- Preserve null-safe keys, idempotent no-op writes, scoped ownership, and snapshot/run attribution.
- Never interpolate untrusted identifiers or SQL into internal trusted-code interfaces and later expose them as public gateway inputs.
- Never commit credentials, `.env`, generated memory, session logs, or private release candidates.

## Operations and lineage

- `lake_ops` is the operational authority for runs, watermarks, assets, versions, asset-level lineage, and publication receipts.
- SQLMesh and SQLGlot are lineage providers; normalize their output rather than inventing incompatible stores.
- Public provenance/lineage is a release-scoped projection and must exclude private operational detail.
- Structured event logs must carry stable `run_id`; publication traces and later public-query traces are separate domains.
- Actual scheduling stays with the owning repository/systemd/cloud scheduler until several jobs prove a shared non-trivial scheduler abstraction.

## Program coordination

- GitHub issues are authoritative for work status, ownership, blockers, and evidence.
- Accepted ADRs/specifications are authoritative for decisions.
- Tests are executable contracts.
- Agent artifacts, observational memory, handoff files, and scratch notes are non-authoritative and disposable.
- Do not create a committed scratchpad backlog. Move substantive findings into an issue comment, ADR/spec edit, test, or this design document.
- Keep one writer per repository/worktree. Parallelize cdsci platform, bioc conversion, cancer conversion, and DuckDock lanes only when ownership does not overlap.
- Shared-contract changes have one platform owner; product lanes report gaps rather than forking shared behavior locally.

## Project agents

Project agents live under `.pi/agents/lake-program/` and default to fresh context:

- `lake-program.program-scout` — fast read-only reconnaissance.
- `lake-program.platform-worker` — bounded cdsci shared-platform implementation.
- `lake-program.bioc-worker` — bounded bioc conversion work.
- `lake-program.cancer-worker` — bounded cancer conversion work.
- `lake-program.duckdock-worker` — bounded DuckDock work.
- `lake-program.contract-reviewer` — independent read-only contract review.

The parent/coordinator retains sequencing, architecture, issue disposition, and final acceptance. Give every child a cold-start task packet with repository/cwd, issue, governing files, exact seam, authority, acceptance, validation, and stop conditions. Use forked context only for bounded trajectory advice that genuinely depends on parent history; use fresh context for scouts, writers, and reviewers.

## Validation

Before reporting a cdsci-lake code change complete, run the focused tests and then, when feasible:

```bash
uv run ruff check src/ tests/
uv run pytest -q
```

Publication changes additionally run the relevant contract, SCD2, direct-Parquet, Frozen DuckLake, registry, and optional Iceberg acceptance described in `docs/design/scientific-publication-platform.md`.

Report honestly: distinguish offline fixture evidence, local HTTP acceptance, live non-destructive probes, and isolated destructive/security tests. Never claim live validation that was not observed.
