import asyncio
import contextlib
import json

from models import LogModel, RunModel, SnapshotModel, SourceModel

from cdsci.lake import get_settings, lake_connect, ops, snapshot_log

# "Table/schema doesn't exist yet" — an empty lake, not an error worth 500-ing on.
_ABSENT = ("does not exist", "Table with name")


def _is_absent(e: Exception) -> bool:
    return any(m in str(e) for m in _ABSENT)


class DuckDBDashboardRepository:
    """Read-only view over the ops ledger + snapshot log, through the public
    ``cdsci.lake`` surface. Config comes from the substrate's own Settings
    (``CU_OPENALEX_*`` env / GSM) — the dashboard is a plain consumer."""

    def __init__(self):
        self._con = None
        self._lock = asyncio.Lock()

    def _get_connection(self):
        if self._con is None:
            # Lake read-only (never mutate the substrate); ops attached read-side.
            self._con = lake_connect(get_settings(), read_only=True, with_ops=True)
        return self._con

    async def _read(self, fn, default):
        async with self._lock:
            def call():
                try:
                    return fn(self._get_connection().cursor())
                except Exception as e:
                    if _is_absent(e):
                        return default
                    raise
            return await asyncio.to_thread(call)

    async def get_sources(self) -> list[SourceModel]:
        rows = await self._read(ops.list_sources, [])
        return [SourceModel(**r) for r in rows]

    async def get_runs(self, limit: int = 50) -> list[RunModel]:
        rows = await self._read(lambda con: ops.list_runs(con, limit=limit), [])
        return [RunModel(**r) for r in rows]

    async def get_snapshots(self, limit: int = 50) -> list[SnapshotModel]:
        rows = await self._read(lambda con: snapshot_log(con, limit=limit), [])
        results = []
        for snapshot_id, ts, author, message, extra_info, changes in rows:
            run_id = None
            if extra_info:
                with contextlib.suppress(Exception):
                    run_id = json.loads(extra_info).get("run_id")
            changes_dict = dict(changes) if changes else None
            results.append(SnapshotModel(
                snapshot_id=snapshot_id, timestamp=ts, author=author,
                commit_message=message, run_id=run_id, changes=changes_dict,
            ))
        return results

    async def get_logs(self, run_id: str) -> list[LogModel]:
        # ponytail: the ledger IS the log source today — synthesize a run's
        # timeline from its row. Swap for real structured logs (design doc §5)
        # only if free-text log search across runs is actually needed.
        run = await self._read(lambda con: ops.get_run(con, run_id), None)
        if not run:
            return []
        return _run_to_logs(run)


def _run_to_logs(r: dict) -> list[LogModel]:
    source, run_id = r["source"], r["run_id"]
    logger_name = f"run:{source}"
    started, finished = r["started_at"], r["finished_at"] or r["started_at"]

    def line(ts, level, message):
        return LogModel(timestamp=ts, level=level, message=message,
                        logger_name=logger_name, run_id=run_id)

    logs = [
        line(started, "INFO",
             f"start → {r['target']} (version={r['version']}, "
             f"snapshot_before={r['snapshot_before']}, run_id={run_id}) on host {r['host']}"),
        line(started, "INFO", "attaching DuckLake substrate connection..."),
        line(started, "INFO", f"connected to backend. running upsert for {r['target']}..."),
    ]
    status, before, after = r["status"], r["snapshot_before"], r["snapshot_after"]
    rows = r["rows_after"]
    if status in ("success", "idempotent"):
        logs.append(line(finished, "SUCCESS",
            f"{status} → {r['target']} (rows={rows}, snapshot {before}→{after}, run_id={run_id})"))
    elif status == "error":
        logs.append(line(finished, "ERROR",
            f"ERROR after {rows} rows (snapshot {before}→{after}, run_id={run_id}): {r['error']}"))
    else:
        logs.append(line(finished, "INFO",
            f"run in progress: upsert target={r['target']} running..."))
    return logs
