"""``cdsci.lake.transform.lineage`` — column-level lineage via ``sqlglot`` (ADR-0015 §2).

Best-effort, not a correctness guarantee (ADR-0015 consequences): these models
carry no catalog schema for ``sqlglot`` to expand a ``SELECT *`` against, and
its lineage resolution is less battle-tested than SQLMesh's on gnarly CTEs/
window functions. An output column ``sqlglot`` can't resolve logs a warning
and contributes no edges rather than failing the run — lineage is
observability, not a write-path gate.

Feeds ``lake_ops.lineage`` once ADR-0014's asset/lineage tables exist; until
then this module only *computes* edges, it never writes anywhere.
"""

from __future__ import annotations

from dataclasses import dataclass

import sqlglot
from sqlglot import exp
from sqlglot.lineage import lineage as _sqlglot_lineage

from ..log import logger
from .models import Model


@dataclass(frozen=True)
class LineageEdge:
    """One output column's dependency on an upstream ``table.column``."""

    target: str  # "ref.id_crosswalk"
    target_column: str  # "pmid"
    source_table: str  # "reporter.publink"
    source_column: str  # "pmid"


def model_lineage(model: Model) -> list[LineageEdge]:
    """Best-effort column lineage for every resolvable output column of ``model``."""
    try:
        select = sqlglot.parse_one(model.sql, read="duckdb").find(exp.Select)
    except Exception as exc:
        logger.warning("transform lineage: {} failed to parse: {}", model.target, exc)
        return []
    if select is None:
        return []

    edges: list[LineageEdge] = []
    for projection in select.expressions:
        col_name = projection.alias_or_name
        if not col_name:
            continue
        try:
            root = _sqlglot_lineage(col_name, model.sql, dialect="duckdb")
        except Exception as exc:
            logger.warning(
                "transform lineage: {}.{} unresolved: {}", model.target, col_name, exc
            )
            continue
        for node in root.walk():
            if node.downstream or not isinstance(node.source, exp.Table):
                continue  # only true leaves backed by a real table are sources
            table = ".".join(p for p in (node.source.db, node.source.name) if p)
            if not table:
                continue
            source_column = node.name.rsplit(".", 1)[-1]
            edges.append(LineageEdge(model.target, col_name, table, source_column))
    return edges
