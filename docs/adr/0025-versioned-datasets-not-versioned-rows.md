# 0025. Versioned datasets, not versioned rows

**Status**: Accepted
**Date**: 2026-10-02

Tracking: cdsci-lake#121. Supersedes the scd2/history parts of design doc §5.3,
§6.4, §7.2, §7.4 and §11.4, and the ADR-0014 Amendment 2026-09-22 source-ref
grammar (`lake.<schema>.<table>` only).

## Context

The platform modelled change as row history: SCD2 intervals (`valid_from` /
`valid_to`), the `scd2_release` and `scd2_bitemporal` temporal models, a
`Materialization` enum on every file index, and `cdsci.lake.history`
(`plan_scd2_release`) to plan closes and opens within a complete scope. Every
product client in practice reads only current rows, and the machinery cost real
rules: complete-scope retirement, release-ordering guards, and a dependency on
lake time travel (cdsci-lake#103).

## Decision

- A release is an **immutable full snapshot** of a dataset, identified by a
  release id and a release date. The id is the UTC build date `YYYY-MM-DD`; a
  further release on the same day is `YYYY-MM-DD.2`, `.3`, and so on. Upstream
  versions go in provenance, never in the id. Ids are ordered by
  `release_sort_key`, never as raw strings.
- Each dataset sets its own cadence and its own retention: keep every release by
  default, or declare `keep_last = N`. **Pinned releases are never pruned.**
- A dataset publishes `<dataset>/releases.json` (the index) and
  `<dataset>/latest.json` (a pointer to the newest release), both rewritten
  atomically, index first. `publish_release` builds, verifies, finalizes,
  promotes and prunes in one call; a release that fails acceptance leaves no
  `manifest.json` and the index untouched.
- Temporal models are `append_immutable` and `upsert_latest_snapshot`. The
  `Materialization` enum and `TableFileIndex.materialization` are removed.
- Source refs follow `<catalog>.<schema>.<table>`: `lake` is the shared internal
  DuckLake, any other first segment names a product-local catalog (for example
  `canceronice.measure.observation`).
- Column types may nest: `list<T>` and `struct<name: T, ...>`.
- The release manifest spec is **2.0**: `release_date` is emitted next to
  `release`, `materialization` is gone.

## Why not row history

- Every product client queries only current rows; row history served no reader.
- A pinned release reproduces a past state better than a time-travel predicate.
- For daily-diff sources (PubMed `updatefiles`) the upstream diffs already are the
  history.
- SCD2 forces complete-scope retirement, release-ordering guards and a
  lake-time-travel dependency (cdsci-lake#103). Full snapshots need none of them.
- Row history already in live Iceberg tables is frozen as an archive; this
  decision backfills nothing and writes to no Iceberg table.
