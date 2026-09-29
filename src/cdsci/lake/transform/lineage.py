"""``cdsci.lake.transform.lineage`` — table- and column-level lineage via ``sqlglot`` (ADR-0021).

Two levels, both keyed by catalog-less ``schema.table`` (the same shape as a
model's ``target``):

* :func:`table_dependencies` — every table a model reads, external inputs (EL
  tables) included. :mod:`.graph` keeps only the subset that are other models.
* :func:`column_lineage` — ``target.column <- source_table.source_column`` for
  every resolvable output column.

Pass ``schema`` (see :func:`catalog_schema`) so ``SELECT *`` and unqualified
columns resolve against the real input tables. Without it, resolution is
best-effort as before. Either way lineage is observability, not a write-path
gate: an unresolvable column logs a warning and contributes no edges, and
nothing here raises into a model run.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

import duckdb
import sqlglot
from sqlglot import exp
from sqlglot.lineage import lineage as _sqlglot_lineage
from sqlglot.optimizer.qualify import qualify

from ..log import logger
from .models import Model

# {catalog: {schema: {table: {column: type}}}} -- sqlglot's MappingSchema input shape.
Schema = dict[str, dict[str, dict[str, dict[str, str]]]]


@dataclass(frozen=True)
class LineageEdge:
    """One output column's dependency on an upstream ``table.column``."""

    target: str  # "ncbi_gene2pubmed.gene_publication"
    target_column: str  # "pmid"
    source_table: str  # "reporter.publink"
    source_column: str  # "pmid"


def _table_ref(table: exp.Table) -> str | None:
    """``schema.table`` for a real table; ``None`` for a CTE or table function."""
    parts = [p for p in (table.catalog, table.db, table.name) if p]
    return ".".join(parts[-2:]) if len(parts) >= 2 else None


def table_dependencies(model: Model) -> set[str]:
    """Every ``schema.table`` ``model`` reads, excluding itself.

    A bare name (a CTE) or a table-valued function (``read_parquet(...)``) has
    fewer than two parts and is skipped: neither is a lake table.
    """
    tree = sqlglot.parse_one(model.sql, read="duckdb")
    refs = {_table_ref(t) for t in tree.find_all(exp.Table)}
    return {r for r in refs if r and r != model.target}


def catalog_schema(
    con: duckdb.DuckDBPyConnection, tables: Iterable[str], catalog: str = "lake"
) -> Schema:
    """Column types for ``tables`` (``schema.table``) from ``catalog``'s information_schema.

    Tables that don't exist are simply absent from the result.
    """
    wanted = sorted(set(tables))
    if not wanted:
        return {}
    rows = con.execute(
        "SELECT table_schema, table_name, column_name, data_type "
        "FROM information_schema.columns "
        "WHERE table_catalog = ? AND (table_schema || '.' || table_name) IN "
        f"({', '.join('?' for _ in wanted)}) ORDER BY ordinal_position",
        [catalog, *wanted],
    ).fetchall()
    out: Schema = {catalog: {}}
    for schema, table, column, dtype in rows:
        out[catalog].setdefault(schema, {}).setdefault(table, {})[column] = dtype
    return out


def column_lineage(model: Model, schema: Schema | None = None) -> list[LineageEdge]:
    """Best-effort column lineage for every resolvable output column of ``model``."""
    try:
        tree = sqlglot.parse_one(model.sql, read="duckdb")
    except Exception as exc:
        logger.warning("transform lineage: {} failed to parse: {}", model.target, exc)
        return []
    try:
        # Expands SELECT * and qualifies columns when the schema knows the inputs.
        tree = qualify(tree, schema=schema, dialect="duckdb", validate_qualify_columns=False)
    except Exception as exc:
        logger.warning("transform lineage: {} failed to qualify: {}", model.target, exc)
    if not isinstance(tree, exp.Query):
        return []

    edges: list[LineageEdge] = []
    for projection in tree.selects:
        col_name = projection.alias_or_name
        if not col_name or col_name == "*":
            continue
        try:
            root = _sqlglot_lineage(col_name, tree, schema=schema, dialect="duckdb")
        except Exception as exc:
            logger.warning(
                "transform lineage: {}.{} unresolved: {}", model.target, col_name, exc
            )
            continue
        for node in root.walk():
            if node.downstream or not isinstance(node.source, exp.Table):
                continue  # only true leaves backed by a real table are sources
            table = _table_ref(node.source)
            if not table:
                continue
            edge = LineageEdge(model.target, col_name, table, exp.to_column(node.name).name)
            if edge not in edges:
                edges.append(edge)
    return edges
