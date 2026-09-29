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

from graphlib import CycleError, TopologicalSorter

from .lineage import table_dependencies
from .models import Model


def _model_dependencies(model: Model, known_targets: set[str]) -> set[str]:
    """The subset of ``model``'s table references that are other known models.

    A reference's trailing two dotted parts (``schema.table``, ignoring any
    catalog prefix like ``lake.``) must match a target in ``known_targets`` to
    count — anything else is an input, not a DAG edge.
    """
    return table_dependencies(model) & known_targets


def build_graph(models: dict[str, Model]) -> dict[str, set[str]]:
    """``{target: {targets it depends on}}`` for every model in ``models``."""
    known = set(models)
    return {target: _model_dependencies(m, known) for target, m in models.items()}


def topological_order(graph: dict[str, set[str]]) -> list[str]:
    """An execution order where every dependency runs first (stdlib ``graphlib``).

    Deterministic (each ready batch sorted alphabetically) so runs and tests are
    reproducible. Raises ``ValueError`` on a cycle -- a transform DAG must be
    acyclic by construction; there's no valid order to fall back to.
    """
    ts = TopologicalSorter(graph)
    try:
        ts.prepare()
    except CycleError as exc:
        raise ValueError(f"cycle detected among transform models: {exc.args[1]}") from exc
    order: list[str] = []
    while ts.is_active():
        ready = sorted(ts.get_ready())
        order.extend(ready)
        ts.done(*ready)
    return order
