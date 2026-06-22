"""DuckLake maintenance — reclaim space from accumulated snapshots and files.

Every write adds (a) a snapshot to the catalog and (b) Parquet files to R2; updates
and deletes leave older files behind. Reclaiming space is **two ordered steps**:

1. **expire snapshots** (:func:`expire_snapshots`) — invalidate old snapshots so the
   data files they alone referenced become unreferenced.
2. **delete unused files** (:func:`cleanup_files`) — remove data files no longer
   referenced by any live snapshot.

Optionally **compact** many small files into fewer large ones first
(:func:`compact`). Every step supports ``dry_run`` to preview.

⚠️  These operate on the **whole catalog**, across *all* publishers (omicidx and
every source). Expiring snapshots destroys time-travel for everyone, so on the
shared lake it must be a coordinated, deliberate operation — never a casual run.
Prefer ``dry_run=True`` first, and scope ``older_than`` conservatively.
"""

from __future__ import annotations

import duckdb

from .connect import LAKE


def _bool(value: bool) -> str:
    return "true" if value else "false"


def expire_snapshots(
    con: duckdb.DuckDBPyConnection,
    *,
    older_than: str | None = None,
    dry_run: bool = True,
) -> list[tuple]:
    """Invalidate snapshots older than ``older_than`` (a ``'YYYY-MM-DD[ HH:MM:SS]'``).

    Returns the affected snapshots. ``older_than`` is required for safety — with
    no bound this would target the entire history. Run ``dry_run`` first.
    """
    if not older_than:
        raise ValueError("expire_snapshots requires older_than (no unbounded expiry).")
    return con.execute(
        f"CALL ducklake_expire_snapshots('{LAKE}', "
        f"older_than => TIMESTAMP '{older_than}', dry_run => {_bool(dry_run)})"
    ).fetchall()


def cleanup_files(
    con: duckdb.DuckDBPyConnection,
    *,
    cleanup_all: bool = True,
    dry_run: bool = True,
) -> list[tuple]:
    """Delete data files no longer referenced by any live snapshot.

    ``cleanup_all`` removes every orphaned file (from any prior expiry); this is
    the safe companion to :func:`expire_snapshots` and does not itself drop
    history. Returns the files cleaned (or that would be, under ``dry_run``).
    """
    return con.execute(
        f"CALL ducklake_cleanup_old_files('{LAKE}', "
        f"cleanup_all => {_bool(cleanup_all)}, dry_run => {_bool(dry_run)})"
    ).fetchall()


def compact(con: duckdb.DuckDBPyConnection) -> list[tuple]:
    """Merge adjacent small data files into fewer larger ones (compaction)."""
    return con.execute(f"CALL ducklake_merge_adjacent_files('{LAKE}')").fetchall()


def vacuum(
    con: duckdb.DuckDBPyConnection,
    *,
    older_than: str | None = None,
    compact_first: bool = True,
    dry_run: bool = True,
) -> dict[str, int]:
    """Full maintenance: (compact →) optionally expire → delete unused files.

    Without ``older_than``, no snapshots are expired — only already-orphaned files
    are cleaned (and files compacted), which is non-destructive to history.
    """
    out: dict[str, int] = {}
    if compact_first and not dry_run:
        out["compacted"] = len(compact(con))
    if older_than:
        out["expired"] = len(expire_snapshots(con, older_than=older_than, dry_run=dry_run))
    out["deleted"] = len(cleanup_files(con, cleanup_all=True, dry_run=dry_run))
    return out
