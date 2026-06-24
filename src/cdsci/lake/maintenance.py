"""DuckLake maintenance — reclaim space from accumulated snapshots and files.

Every write adds (a) a snapshot to the catalog and (b) Parquet files to R2; updates
and deletes leave older files behind. Reclaiming space is **two ordered steps**:

1. **expire snapshots** (:func:`expire_snapshots`) — invalidate snapshots so the
   data files they alone referenced become unreferenced.
2. **delete unused files** (:func:`cleanup_files`) — remove data files no longer
   referenced by any live snapshot.

Optionally **compact** many small files into fewer large ones first
(:func:`compact`). Every step supports ``dry_run`` to preview.

⚠️  ``older_than`` expiry operates on the **whole catalog**, across *all* publishers
(omicidx and every source), and destroys time-travel for everyone — on the shared
lake it must be a coordinated, deliberate operation, never a casual run.

To retire **one schema** without touching anyone else's history, use
:func:`purge_schema` (or its parts): it drops the schema, then expires *only* the
snapshots whose every change was that schema (:func:`schema_snapshot_ids`, by
explicit ``versions``), then cleans the now-unreferenced files. That is how the
broken PMC load is rolled back without disturbing omicidx/openalex time-travel.
"""

from __future__ import annotations

import re

import duckdb

from .config import Settings, get_settings
from .connect import LAKE
from .log import logger


def _bool(value: bool) -> str:
    return "true" if value else "false"


# --- Per-schema snapshot resolution (the safe, surgical path) ---


def _attach_metadata(con: duckdb.DuckDBPyConnection, settings: Settings | None, alias: str) -> str:
    """Attach the DuckLake **metadata** Postgres DB read-only as ``alias``; return it.

    The catalog metadata (``ducklake_schema`` / ``ducklake_table`` /
    ``ducklake_snapshot_changes``, in the ``public`` schema) is what maps a change
    log entry back to a schema — the ``ducklake_*`` functions don't expose dropped
    tables or schema names. This is the shared Postgres lake only.
    """
    s = settings or get_settings()
    if s.lake_backend != "postgres":
        raise NotImplementedError("schema-scoped expiry targets the shared postgres lake.")
    con.execute(
        f"ATTACH 'dbname={s.lake_pg_dbname} host={s.lake_pg_host} port={s.lake_pg_port} "
        f"user={s.lake_pg_user}' AS {alias} (TYPE postgres, READ_ONLY);"
    )
    return alias


_SCHEMA_TOK = re.compile(r'(?:created|dropped)_schema:"([^"]+)"')
_TABLE_NAME_TOK = re.compile(r'(?:created|dropped|altered)_table:"([^"]+)"\."[^"]+"')
_TABLE_ID_TOK = re.compile(r"(?:inserted_into|deleted_from|compacted|altered)_table:(\d+)")


def schema_snapshot_ids(
    con: duckdb.DuckDBPyConnection,
    schema: str,
    *,
    settings: Settings | None = None,
) -> list[int]:
    """Snapshot ids whose **every** change belongs to ``schema`` — safe to expire alone.

    A snapshot qualifies only when all of its change-log entries name ``schema``
    (by schema name or by one of the schema's table ids, current *or dropped*). A
    snapshot that also touched another publisher's table is excluded, so expiring
    the returned set can never drop another schema's time-travel. Ids ascending.
    """
    alias = "_md"
    self_attached = alias not in {r[0] for r in con.execute("PRAGMA database_list").fetchall()}
    if self_attached:
        _attach_metadata(con, settings, alias)
    try:
        schema_id = con.execute(
            f"SELECT schema_id FROM {alias}.public.ducklake_schema WHERE schema_name = ?",
            [schema],
        ).fetchone()
        if schema_id is None:
            return []
        table_ids = {
            r[0]
            for r in con.execute(
                f"SELECT table_id FROM {alias}.public.ducklake_table WHERE schema_id = ?",
                [schema_id[0]],
            ).fetchall()
        }
        rows = con.execute(
            f"SELECT snapshot_id, changes_made FROM {alias}.public.ducklake_snapshot_changes "
            "ORDER BY snapshot_id"
        ).fetchall()
    finally:
        if self_attached:
            con.execute(f"DETACH {alias};")

    out: list[int] = []
    for snap, changes in rows:
        text = changes or ""
        schemas = set(_SCHEMA_TOK.findall(text))
        names = set(_TABLE_NAME_TOK.findall(text))
        ids = {int(x) for x in _TABLE_ID_TOK.findall(text)}
        touches = (schema in schemas) or (schema in names) or bool(ids & table_ids)
        if not touches:
            continue
        other = (schemas - {schema}) or (names - {schema}) or (ids - table_ids)
        if other:
            logger.warning(
                "snapshot {} touches {} AND others {} — not expiring", snap, schema, other
            )
            continue
        out.append(snap)
    return out


# --- Primitive operations ---


def drop_schema(con: duckdb.DuckDBPyConnection, schema: str, *, cascade: bool = True) -> None:
    """``DROP SCHEMA lake.<schema>`` — remove it from the live catalog head.

    The data files survive (older snapshots still reference them) until they are
    expired and cleaned — see :func:`purge_schema`.
    """
    cascade_sql = " CASCADE" if cascade else ""
    logger.warning("DROP SCHEMA {}.{}{}", LAKE, schema, cascade_sql)
    con.execute(f"DROP SCHEMA IF EXISTS {LAKE}.{schema}{cascade_sql};")
    logger.info("dropped schema {}.{}", LAKE, schema)


def expire_snapshots(
    con: duckdb.DuckDBPyConnection,
    *,
    older_than: str | None = None,
    versions: list[int] | None = None,
    dry_run: bool = True,
) -> list[tuple]:
    """Invalidate snapshots — by explicit ``versions`` (surgical) or ``older_than``.

    Pass ``versions`` (a list of snapshot ids) to expire exactly those — the safe
    way to retire one schema's snapshots without touching the rest of the catalog.
    ``older_than`` (a ``'YYYY-MM-DD[ HH:MM:SS]'``) expires everything older, across
    *all* publishers — global and destructive. Exactly one is required. Run
    ``dry_run`` first.
    """
    if bool(versions) == bool(older_than):
        raise ValueError("pass exactly one of versions= or older_than=.")
    if versions is not None and not versions:
        logger.info("expire_snapshots: empty versions list — nothing to do")
        return []
    if versions is not None:
        arg = f"versions => [{', '.join(str(int(v)) for v in versions)}]"
        logger.warning(
            "expire_snapshots versions=[{} ids: {}…{}] dry_run={}",
            len(versions), min(versions), max(versions), dry_run,
        )
    else:
        arg = f"older_than => TIMESTAMP '{older_than}'"
        logger.warning("expire_snapshots older_than={!r} (GLOBAL) dry_run={}", older_than, dry_run)
    rows = con.execute(
        f"CALL ducklake_expire_snapshots('{LAKE}', {arg}, dry_run => {_bool(dry_run)})"
    ).fetchall()
    logger.info(
        "expire_snapshots: {} snapshot(s) {}", len(rows), "previewed" if dry_run else "expired"
    )
    return rows


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
    rows = con.execute(
        f"CALL ducklake_cleanup_old_files('{LAKE}', "
        f"cleanup_all => {_bool(cleanup_all)}, dry_run => {_bool(dry_run)})"
    ).fetchall()
    logger.info("cleanup_files: {} file(s) {}", len(rows), "previewed" if dry_run else "deleted")
    return rows


def compact(con: duckdb.DuckDBPyConnection) -> list[tuple]:
    """Merge adjacent small data files into fewer larger ones (compaction)."""
    logger.info("compacting adjacent files in {}", LAKE)
    rows = con.execute(f"CALL ducklake_merge_adjacent_files('{LAKE}')").fetchall()
    logger.info("compact: {} merge result row(s)", len(rows))
    return rows


def purge_schema(
    con: duckdb.DuckDBPyConnection,
    schema: str,
    *,
    settings: Settings | None = None,
    dry_run: bool = True,
) -> dict:
    """Retire one schema fully: drop it, expire **only its** snapshots, clean its files.

    The surgical alternative to a global ``older_than`` expiry. Resolves the
    schema's data-bearing snapshots *before* dropping (:func:`schema_snapshot_ids`),
    drops the schema so its files leave the head, expires exactly those snapshot
    versions, then deletes the now-unreferenced files. Other publishers' snapshots
    are never in the expire set. ``dry_run`` previews the snapshot set and the file
    cleanup without dropping or expiring anything.
    """
    ids = schema_snapshot_ids(con, schema, settings=settings)
    logger.info(
        "purge_schema {}: {} data-bearing snapshot(s) resolved{}",
        schema, len(ids), f" ({min(ids)}…{max(ids)})" if ids else "",
    )
    if dry_run:
        logger.info("purge_schema {}: DRY RUN — not dropping/expiring", schema)
        preview = expire_snapshots(con, versions=ids, dry_run=True) if ids else []
        return {"schema": schema, "snapshots": ids, "expired": 0, "deleted": 0,
                "previewed_expire": len(preview), "dry_run": True}
    drop_schema(con, schema, cascade=True)
    expired = expire_snapshots(con, versions=ids, dry_run=False) if ids else []
    deleted = cleanup_files(con, cleanup_all=True, dry_run=False)
    logger.success(
        "purge_schema {}: dropped, {} snapshot(s) expired, {} file(s) deleted",
        schema, len(expired), len(deleted),
    )
    return {"schema": schema, "snapshots": ids, "expired": len(expired),
            "deleted": len(deleted), "dry_run": False}


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
