"""Sync SQLMesh's own state into `lake_ops` snapshot attribution (ADR-0008
Amendment 2026-09-22; cdsci-lake#89).

Requires the ``[transform]`` extra (SQLMesh) -- import this module only from a
context that already depends on it (a sync CLI run after ``sqlmesh apply``),
never from the base read-client package.

The bracketing/attribution mechanism itself lives in
:func:`cdsci.lake.ops.sync_sqlmesh_snapshot_attribution`, fully offline-testable
against a local DuckLake with no SQLMesh state involved. This module is the thin
adapter that extracts a project's own models from a live SQLMesh
:class:`~sqlmesh.core.context.Context` and calls it once per model.
"""

from __future__ import annotations

import typing as t

from .. import ops

if t.TYPE_CHECKING:
    import duckdb
    from sqlmesh.core.context import Context


def sync_project_attribution(context: Context, con: duckdb.DuckDBPyConnection) -> list[str]:
    """Attribute every model ``context`` itself defines against ``con``.

    ``context.models`` is **not** scoped to this project alone: SQLMesh also
    injects PROD models belonging to *other* projects into it (any state
    snapshot whose ``node.project`` isn't one of the loader's own projects gets
    added to ``self._models`` -- ``sqlmesh/core/context.py`` around
    ``load()``'s uncached-snapshot handling). Skip any model whose own
    :attr:`~sqlmesh.core.node.Model.project` isn't ``context``'s, so this can
    never sync a model belonging to another producer (e.g. omicidx).

    Returns the ``run_id``s of the new runs recorded (skips models with nothing
    new to attribute since their last sync).
    """
    project = context.config.project
    run_ids = []
    for fqn, model in context.models.items():
        if model.project != project:
            continue
        snapshot = context.get_snapshot(fqn)
        if snapshot is None:
            continue
        run_id = ops.sync_sqlmesh_snapshot_attribution(
            con,
            project=project,
            model=model.name,
            target=snapshot.table_name(),
            version=snapshot.version,
        )
        if run_id is not None:
            run_ids.append(run_id)
    return run_ids
