"""``cdsci.lake.transform.targets`` — reverse-ETL adapters (ADR-0015 decision 3).

A target is config, not code: ``{"type": "parquet" | "duckdb" | "lake_table" |
"iceberg", ...}``. DuckDB stays the sole execution engine — native Parquet,
native DuckDB ``ATTACH``, the ``iceberg`` extension through an attached REST
catalog (icegate). ``postgres`` is deliberately absent — omicidx's existing
reverse-ETL does zero-downtime A/B-slot table swaps with per-table hardcoded
DDL, which needs a declarative DDL/column-mapping design this module doesn't
have yet (tracked separately, not in scope for ADR-0015's first pass).
"""

from __future__ import annotations

import os
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
    * ``iceberg`` — ``{"endpoint", "token_env", "catalog", "namespace", "table"}``.
    """

    type: Literal["parquet", "duckdb", "lake_table", "iceberg"]
    config: dict[str, Any] = field(default_factory=dict)


def publish(
    con: duckdb.DuckDBPyConnection, source_table: str, target: Target, *, date: str
) -> None:
    """Publish ``source_table`` (a catalog-qualified lake table) to ``target``.

    ``date`` is a caller-supplied stamp (the ``parquet`` dated-copy pattern) —
    passed in rather than computed here, so this function stays pure with
    respect to wall-clock time.
    """
    if target.type == "parquet":
        _publish_parquet(con, source_table, target.config, date)
    elif target.type in ("duckdb", "lake_table"):
        _publish_duckdb(con, source_table, target.config)
    elif target.type == "iceberg":
        _publish_iceberg(con, source_table, target.config)
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


def _publish_iceberg(con: duckdb.DuckDBPyConnection, source_table: str, config: dict) -> None:
    """CREATE-if-absent + full-refresh MERGE, not a literal ``CREATE OR REPLACE`` passthrough.

    DuckDB's Iceberg ``UPDATE``/``DELETE`` are merge-on-read (positional
    deletes) only — ``CREATE OR REPLACE TABLE`` is not an Iceberg write
    primitive. A full-table refresh is create-if-absent, then delete-all +
    insert-all. Writes through an attached REST catalog — icegate, already
    live in production for ``bioc-on-ice`` (ADR-0015).
    """
    con.execute("INSTALL iceberg; LOAD iceberg;")
    token = os.environ[config["token_env"]]
    con.execute("CREATE OR REPLACE SECRET _publish_ice (TYPE ICEBERG, TOKEN ?);", [token])
    con.execute(
        f"ATTACH '{config['catalog']}' AS _publish_ice_cat "
        f"(TYPE ICEBERG, ENDPOINT '{config['endpoint']}', SECRET _publish_ice);"
    )
    try:
        namespace, table = config["namespace"], config["table"]
        con.execute(f"CREATE SCHEMA IF NOT EXISTS _publish_ice_cat.{namespace};")
        full = f"_publish_ice_cat.{namespace}.{table}"
        exists = con.execute(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_catalog = ? AND table_schema = ? AND table_name = ?",
            ["_publish_ice_cat", namespace, table],
        ).fetchone()
        if exists is None:
            con.execute(f"CREATE TABLE {full} AS SELECT * FROM {source_table} LIMIT 0;")
        con.execute(f"DELETE FROM {full};")
        con.execute(f"INSERT INTO {full} SELECT * FROM {source_table};")
        logger.info("transform: published {} -> iceberg {}", source_table, full)
    finally:
        con.execute("DETACH _publish_ice_cat;")
