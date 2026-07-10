# 0013. The lake write path is EL-only: `upsert`-only, derived tables defer to the transform layer

- Status: accepted
- Date: 2026-07-10

> Refines ADR-0011 §7, from a Phase-1 scoping grill of omicidx's migration.

## Context

ADR-0011 §7 sanctioned two write verbs — `upsert` and `rebuild` — the latter for
keyless / fully-recomputed derived tables. Scoping omicidx's migration surfaced a
cleaner boundary:

- A derived table (omicidx's `publication_accession_linkage` — a cross-entity
  pmid↔accession inversion unioning `sra_study` + `geo_series` + `bioproject`) is a
  **transform-layer artifact**, and the transform layer (SQLMesh / T) is
  deliberately deferred (ADR-0012).
- Every genuine **EL** table is a keyed projection of a *raw source* and writes
  cleanly via `upsert` — delta snapshots even for a full external dump
  (`sra_accessions` on `accession`).
- So once derived tables move to the transform layer, **no EL table needs
  `rebuild`**. The only justification for the second verb evaporates.

## Decision

**The lake write path (EL) is `upsert`-only.** A table earns a place on it only if
it is a keyed projection of a raw source. Anything computed *across* tables —
inversions, unions, aggregations, cross-source crosswalks — is a **transform-layer
artifact** and defers with the transform layer (ADR-0012).

`rebuild` and `truncate` are **not** part of the EL contract. `rebuild` becomes a
transform-layer concern, specified if/when that layer lands. The rule for producers
is one line: **lightest possible touch into the lake — raw → curated entity tables,
nothing more.**

## Consequences

- One write verb (`upsert`). No `rebuild` / `truncate` footguns; the smallest
  contract surface.
- omicidx's orphaned derived loaders (`publication_accession_linkage`,
  `geo_series_with_rnaseq_counts`) stay **parked and un-wired**, awaiting the
  transform layer; `sra_accessions` stays in EL as `upsert`-on-`accession`.
- Producers get a bright EL/T line — the same split the federated-producer model
  already assumes (each producer's transform is its own, optional, deferred concern).
- When the transform layer is chosen (ADR-0012's triggers), it owns `rebuild` and
  the derived tables, and the parked loaders migrate there.
