# 0020. Production public Iceberg writes are PyIceberg-only

- Status: accepted
- Date: 2026-09-22

Supersedes ADR-0015's `iceberg` target decision (decision 3, §"Decision"
around the reverse-ETL target list; Consequences' "`iceberg` target" bullet).

## Context

ADR-0015 approved a DuckDB-native `iceberg` reverse-ETL target: attach an
Iceberg REST catalog (icegate) and do a create-if-absent, then unscoped
`DELETE FROM` + `INSERT INTO` full-table refresh. In production this deleted
2.4M pre-existing rows out of a shared-writer table
(`annotation.identifier_mapping`) because the delete was unscoped to the
publishing job's own rows (cdsci-lake#63). `cdsci-lake#63` disabled the target
by making `publish()` raise `NotImplementedError` before any catalog
connection; this ADR records the decision as policy, not just a patch.

DuckDB's Iceberg `DELETE`/`UPDATE` are merge-on-read (positional deletes)
only, and `CREATE OR REPLACE TABLE` is not an Iceberg write primitive —
there is no DuckDB-native way to express a scoped, transactional Iceberg
write without hand-rolling manifest bookkeeping DuckDB doesn't expose.
PyIceberg owns the Iceberg transaction/commit protocol directly and is the
tool the wider Python Iceberg ecosystem already uses for exactly this.

## Decision

**Production public Iceberg tables are written only through PyIceberg.**

- DuckDB `DELETE`, `UPDATE`, `MERGE`, or `CREATE OR REPLACE` against a
  production public Iceberg table is forbidden, in this module and any
  future one.
- The `iceberg` reverse-ETL target type (`cdsci.lake.transform.targets`)
  stays disabled — `publish()` raises `NotImplementedError` for
  `type="iceberg"` before any catalog connection — until a PyIceberg-backed
  adapter lands as M6 of the DuckLake publication program
  (`docs/design/scientific-publication-platform.md` §13).
- The current DuckDB Iceberg write path is not a pattern to extend to any
  other producer or table while it remains disabled.

## Consequences

- No reverse-ETL target publishes to Iceberg until M6; producers needing an
  Iceberg publish today have no config-only path and must wait or use a
  hand-written PyIceberg script outside this module.
- ADR-0015's `parquet`/`duckdb`/`lake_table` targets are unaffected — this
  ADR only supersedes the `iceberg` target's design.
- Internal DuckLake writes are unaffected — this ADR is scoped to
  *production public Iceberg*, not the internal DuckDB/DuckLake substrate.
