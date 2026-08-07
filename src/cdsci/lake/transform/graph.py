"""``cdsci.lake.transform.graph`` — dependency DAG + topological execution order.

``sqlglot``'s job here is table-reference extraction only, never orchestration:
parse each model's SQL, collect the tables it reads, and keep only the refs
that resolve to *another model in this run*. An unresolved ref (a raw table
this transform layer doesn't own, or a table-valued function like
``read_parquet(...)``) is an external leaf, not an edge — the
``geo_series_with_rnaseq_counts`` case found scoping ADR-0015 is exactly this:
a model reading an external Parquet file has no model dependency to record.
"""

from __future__ import annotations

import sqlglot
from sqlglot import exp

from .models import Model


def model_dependencies(model: Model, known_targets: set[str]) -> set[str]:
    """The subset of ``model``'s table references that are other known models.

    A reference's trailing two dotted parts (``schema.table``, ignoring any
    catalog prefix like ``lake.``) must match a target in ``known_targets`` to
    count — anything else is an input, not a DAG edge.
    """
    deps: set[str] = set()
    for table in sqlglot.parse_one(model.sql, read="duckdb").find_all(exp.Table):
        parts = [p for p in (table.catalog, table.db, table.name) if p]
        if len(parts) < 2:
            continue  # a bare name or a table-valued function call — nothing to match
        candidate = ".".join(parts[-2:])
        if candidate in known_targets and candidate != model.target:
            deps.add(candidate)
    return deps


def build_graph(models: dict[str, Model]) -> dict[str, set[str]]:
    """``{target: {targets it depends on}}`` for every model in ``models``."""
    known = set(models)
    return {target: model_dependencies(m, known) for target, m in models.items()}


def topological_order(graph: dict[str, set[str]]) -> list[str]:
    """Kahn's algorithm: an execution order where every dependency runs first.

    Deterministic (ties broken alphabetically) so runs and tests are
    reproducible. Raises ``ValueError`` on a cycle — a transform DAG must be
    acyclic by construction; there's no valid order to fall back to.
    """
    in_degree = dict.fromkeys(graph, 0)
    children: dict[str, list[str]] = {n: [] for n in graph}
    for target, deps in graph.items():
        for dep in deps:
            children[dep].append(target)
            in_degree[target] += 1

    ready = sorted(n for n, d in in_degree.items() if d == 0)
    order: list[str] = []
    while ready:
        node = ready.pop(0)
        order.append(node)
        for child in children[node]:
            in_degree[child] -= 1
            if in_degree[child] == 0:
                ready.append(child)
        ready.sort()

    if len(order) != len(graph):
        remaining = sorted(set(graph) - set(order))
        raise ValueError(f"cycle detected among transform models: {remaining}")
    return order
