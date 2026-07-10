# 0012. Defer the gateway / config-file target model

- Status: accepted
- Date: 2026-07-10

> A defer-with-triggers decision in the shape of ADR-0007. Records *why* the lake
> keeps env-based, two-target config for now and the exact conditions under which a
> gateway/config-file model becomes worth it.

## Context

The write contract (ADR-0011 §6) selects a connection target via
`lake_backend: "local" | "postgres"` — env-driven pydantic-settings, with a
pluggable credential source. A richer alternative was raised: a sqlmesh-style
**gateway** config file, where named targets (`local`, `shared`, later
`frozen-<producer>`) each declare a backend + credentials, selectable per run.

Two clarifications shaped the decision:

- **Portability is already delivered without it.** The `local` backend (file
  catalog + local-fs data) plus the portable sibling `ops.duckdb` (ADR-0011 §3) let
  a published producer run end-to-end locally today. A config file adds *no*
  portability — only multi-target ergonomics and the option to share one connection
  definition with sqlmesh.
- **The real prize is config-sharing with sqlmesh, and it isn't bankable yet.**
  sqlmesh is a *potential* transform layer, not a chosen one. Building a config
  format that mirrors sqlmesh's gateway schema before sqlmesh is adopted bets on an
  untaken tool; the de-duplication only pays off once sqlmesh is real and both the
  EL (`lake_connect`) and the T (sqlmesh) read the same file.

## Decision

**Keep env + `lake_backend` as the two-target seed. Do not build a gateway /
config-file model now.** `lake_backend` already *is* a named-target selector;
adding a third target later is one enum value + branch, and moving to a file is a
serialization change, not a re-architecture. No structure is being deferred, only a
file format.

Adopt the gateway/config-file model at the **first of**:

1. a **frozen-snapshot target** appears — the federation read-path, where one
   producer reads another's pinned published snapshot as an external model (a third
   target the two-value enum stops expressing cleanly); or
2. **sqlmesh is adopted** for a producer's transforms — at which point define the
   lake connection **once** in the file sqlmesh reads, shared with `lake_connect`,
   rather than twice.

**Constraint until then:** do not mirror sqlmesh's config schema speculatively. If
sqlmesh is adopted, share its file; if it isn't, a small platform-owned profiles
file is the fallback — decided at trigger time, not now.

## Consequences

- Config shape stays orthogonal to the write contract; ADR-0011's "pluggable cred
  source" is the only seam that must survive, and a file is just one expression of
  it.
- When a trigger fires, the migration is mechanical: enumerate the existing targets
  into the file, point `lake_connect` at the selected profile. Rename
  `lake_backend` → target/gateway vocabulary at that time.
- Risk accepted: two producers each carry a few env vars for the `shared` target in
  the interim. Cheap, and exactly the duplication the file would remove once it
  earns its place.
