"""``cdsci.lake.transform.sync`` — the transform sync seam (cdsci-lake#85).

The module every later sync piece plugs into: #86 (per-model run rows), #87
(asset rows for SQLMesh models), #88 (asset-level lineage edges). Today it only
wires :func:`cdsci.lake.ops.sync_sqlmesh_snapshot_attribution` (ADR-0008
Amendment 2026-09-22; cdsci-lake#89) for every model this SQLMesh project owns,
via the existing :func:`cdsci.lake.transform.sqlmesh_sync.sync_project_attribution`
adapter — this module adds the report shape (#81's "sync only writes rows for
our own project" decision made countable), nothing else.

Requires the ``[transform]`` extra (SQLMesh) -- import this module only from a
context that already depends on it, same rule as ``sqlmesh_sync``.
"""

from __future__ import annotations

import typing as t
from dataclasses import asdict, dataclass

from . import sqlmesh_sync

if t.TYPE_CHECKING:
    import duckdb
    from sqlmesh.core.context import Context


@dataclass(frozen=True)
class SyncReport:
    """Counts from one :func:`sync` call. A second call against unchanged
    SQLMesh state reports ``runs_written`` and ``snapshots_attributed`` as 0."""

    models_seen: int
    runs_written: int
    snapshots_attributed: int
    skipped_foreign: int

    def to_dict(self) -> dict:
        return asdict(self)


def sync(con: duckdb.DuckDBPyConnection, context: Context) -> SyncReport:
    """Sync every model ``context`` itself owns into ``lake_ops`` snapshot attribution.

    Idempotent: a second call against state unchanged since the first reports
    ``runs_written=0``/``snapshots_attributed=0`` -- the underlying
    :func:`~cdsci.lake.ops.sync_sqlmesh_snapshot_attribution` watermark has
    nothing new to bracket.
    """
    project = context.config.project
    models_seen = sum(1 for model in context.models.values() if model.project == project)
    skipped_foreign = len(context.models) - models_seen

    run_ids = sqlmesh_sync.sync_project_attribution(context, con)

    # ponytail: #86/#87/#88 hook in here -- per-model run rows, asset rows,
    # and lineage edges are not written yet.

    return SyncReport(
        models_seen=models_seen,
        runs_written=len(run_ids),
        snapshots_attributed=_count_attributed(con, run_ids),
        skipped_foreign=skipped_foreign,
    )


def _count_attributed(con: duckdb.DuckDBPyConnection, run_ids: list[str]) -> int:
    if not run_ids:
        return 0
    placeholders = ",".join("?" * len(run_ids))
    return con.execute(
        "SELECT count(*) FROM ops.lake_ops.snapshot_attribution "
        f"WHERE run_id IN ({placeholders})",
        run_ids,
    ).fetchone()[0]
