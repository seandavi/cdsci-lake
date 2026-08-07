"""``cdsci.lake.transform`` — the SQL-file transform + reverse-ETL layer (ADR-0015).

Un-defers the transform layer ADR-0012/0013 parked: a model is a plain SQL file
(one ``CREATE OR REPLACE TABLE ... AS SELECT`` per file), DuckDB is the sole
execution engine, and ``sqlglot`` does two jobs only — dependency-graph
extraction (:mod:`.graph`) and best-effort column lineage (:mod:`.lineage`) —
never orchestration. This is where ADR-0013's parked ``rebuild`` verb lives,
scoped to this module; the EL write path (:func:`cdsci.lake.connect.upsert`)
stays ``upsert``-only.
"""

from __future__ import annotations
