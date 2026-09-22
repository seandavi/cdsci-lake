"""``cdsci.lake.transform.targets`` — reverse-ETL adapters (ADR-0015 decision 3).

A target is config, not code: ``{"type": "parquet" | "duckdb" | "lake_table" |
"iceberg", ...}``. DuckDB stays the sole execution engine for ``parquet``,
``duckdb``, and ``lake_table`` — native Parquet, native DuckDB ``ATTACH``.
``postgres`` is deliberately absent — omicidx's existing reverse-ETL does
zero-downtime A/B-slot table swaps with per-table hardcoded DDL, which needs a
declarative DDL/column-mapping design this module doesn't have yet (tracked
separately, not in scope for ADR-0015's first pass).

The ``iceberg`` target type is disabled (cdsci-lake#63, M0 of the DuckLake
publication program): its DuckDB ``DELETE FROM`` + ``INSERT INTO`` full-table
refresh clobbered 2.4M pre-existing rows in a shared-writer table
(``annotation.identifier_mapping``) because the delete was unscoped to this
call's own rows. Production public Iceberg writes are PyIceberg-only per
program policy (AGENTS.md, design doc §16 decision 6); DuckDB ``DELETE`` /
``UPDATE`` / ``MERGE`` / ``CREATE OR REPLACE`` against a production public
Iceberg table is forbidden. ``publish()`` raises before any catalog
connection when asked for ``type="iceberg"``. The type stays in ``Target``'s
``Literal`` so an existing config fails loudly at publish time rather than
silently at parse/construction time. A shared PyIceberg adapter is tracked as
M6 of the publication program (``docs/design/scientific-publication-platform.md``).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

import duckdb

from ..log import logger


def _mkparent(path: str) -> None:
    """Create a local path's parent dir; a no-op for remote (``s3://``) paths."""
    if "://" not in path:
        Path(path).parent.mkdir(parents=True, exist_ok=True)


@dataclass(frozen=True)
class Target:
    """One reverse-ETL publish target for a model's output table.

    ``config`` shape by ``type``:

    * ``parquet`` — ``{"path": "s3://.../v{date}/t.parquet", "latest_path": "...latest/t.parquet"}``
      (``latest_path`` optional).
    * ``duckdb`` — ``{"path": "...", "table": "name"}`` (``table`` defaults to the
      source table's own name).
    * ``lake_table`` — ``{}``; a no-op, the model's own write already *is* the
      publish (named for symmetry — a model can list it as a target explicitly).
    * ``iceberg`` — disabled (cdsci-lake#63). ``publish()`` raises
      ``NotImplementedError`` for this type; see the module docstring.
    """

    type: Literal["parquet", "duckdb", "lake_table", "iceberg"]
    config: dict[str, Any] = field(default_factory=dict)


def publish(
    con: duckdb.DuckDBPyConnection, source_table: str, target: Target, *, date: str | None = None
) -> None:
    """Publish ``source_table`` (a catalog-qualified lake table) to ``target``.

    ``date`` is a caller-supplied stamp for the ``parquet`` dated-copy pattern
    only — passed in rather than computed here, so this function stays pure
    with respect to wall-clock time. Required for ``parquet``, unused by every
    other target type.
    """
    if target.type == "parquet":
        if date is None:
            raise ValueError("date is required for the 'parquet' target type")
        _publish_parquet(con, source_table, target.config, date)
    elif target.type in ("duckdb", "lake_table"):
        _publish_duckdb(con, source_table, target.config)
    elif target.type == "iceberg":
        raise NotImplementedError(
            "the 'iceberg' reverse-ETL target is disabled (cdsci-lake#63): its DuckDB "
            "DELETE+INSERT full-table refresh clobbered a shared-writer table's "
            "pre-existing rows. Production public Iceberg writes are PyIceberg-only "
            "(AGENTS.md; docs/design/scientific-publication-platform.md §16 decision 6); "
            "DuckDB DELETE/UPDATE/MERGE/CREATE OR REPLACE against public Iceberg is "
            "forbidden. No replacement adapter exists yet (tracked as M6 of the "
            "DuckLake publication program)."
        )
    else:
        raise ValueError(f"unknown reverse-ETL target type: {target.type!r}")


def _publish_parquet(
    con: duckdb.DuckDBPyConnection, source_table: str, config: dict, date: str
) -> None:
    """Dated copy + re-derived ``latest`` — ports omicidx's ``parquet_export`` pattern.

    Writes the dated, immutable copy first, then re-derives ``latest`` by
    *reading that dated Parquet back* rather than a server-side bucket copy —
    R2 flakes on multi-GB server-side copies (the reason omicidx's
    ``parquet_export`` does this; found in ADR-0015 wayfinding).
    """
    dated = config["path"].format(date=date)
    _mkparent(dated)
    con.execute(f"COPY (SELECT * FROM {source_table}) TO '{dated}' (FORMAT parquet);")
    logger.info("transform: published {} -> {}", source_table, dated)
    latest = config.get("latest_path")
    if latest:
        _mkparent(latest)
        con.execute(
            f"COPY (SELECT * FROM read_parquet('{dated}')) TO '{latest}' (FORMAT parquet);"
        )
        logger.info("transform: re-derived latest {} -> {}", dated, latest)


def _publish_duckdb(con: duckdb.DuckDBPyConnection, source_table: str, config: dict) -> None:
    """Copy into a target DuckDB file, or a no-op when the target IS the lake (``lake_table``)."""
    path = config.get("path")
    if not path:
        return  # lake_table: the model's own CREATE OR REPLACE already is the publish
    _mkparent(path)
    con.execute(f"ATTACH '{path}' AS _publish_target;")
    try:
        table = config.get("table", source_table.rsplit(".", 1)[-1])
        con.execute(
            f"CREATE OR REPLACE TABLE _publish_target.{table} AS SELECT * FROM {source_table};"
        )
        logger.info("transform: published {} -> {}::{}", source_table, path, table)
    finally:
        con.execute("DETACH _publish_target;")

