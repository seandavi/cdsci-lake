"""``cdsci.lake.ops`` — the operational ledger (ADR-0006).

The questions DuckLake snapshots can't answer — *when did we last load a source,
did the run change/error, which snapshot did it produce, where do incrementals
resume* — are answered here. The ledger is **catalog-adjacent native state**, not
DuckLake data: a second attachment ``ops`` (the Postgres ``lake`` DB in
production, a sibling ``ops.duckdb`` locally) holding plain mutable tables.
:func:`cdsci.lake.connect.lake_connect` attaches it on the write path and calls
:func:`bootstrap`; read-only consumers never see it.

Ingestors don't touch SQL here — they wrap a curate in :func:`run` (a context
manager that records one ``lake_ops.run`` row, bracketing the upsert with the
before/after snapshot ids every ingestor used to hand-roll) and, for
incrementals, read/write a cursor via :func:`get_watermark` / :func:`set_watermark`.

Portability note: ``ops`` may be a real Postgres database reached through DuckDB's
``postgres`` extension, whose DDL surface is narrow. So the tables carry **no**
``SERIAL``/``DEFAULT``/``PRIMARY KEY``/foreign-key constraints — ``run_id`` is a
client-generated UUID, timestamps are written with ``current_timestamp``, the
watermark ``value`` is JSON text, and uniqueness is enforced in code (a registry
refresh and watermark set are delete-then-insert).
"""

from __future__ import annotations

import json
import socket
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any

import duckdb

from .connect import LAKE
from .log import logger

OPS = "ops"  # the ATTACH alias for the ledger database
OPS_SCHEMA = "lake_ops"

# The run currently executing on this context, so :func:`cdsci.lake.connect.upsert`
# can self-attribute its snapshot (ADR-0009) without every curate threading the
# Run through. Set for the duration of a :func:`run` block.
_ACTIVE_RUN: ContextVar[Run | None] = ContextVar("active_run", default=None)


def active_run() -> Run | None:
    """The :class:`Run` for the enclosing :func:`run` block, or None outside one."""
    return _ACTIVE_RUN.get()


def _t(table: str) -> str:
    """Fully-qualified ledger table name, e.g. ``ops.lake_ops.run``."""
    return f"{OPS}.{OPS_SCHEMA}.{table}"


# --- The source registry (declared in code; materialized into lake_ops.source) ---


@dataclass(frozen=True)
class Source:
    """A registered source: its lake schema and how it refreshes."""

    name: str
    lake_schema: str
    description: str
    cadence: str
    distribution: str
    license: str
    watermark_strategy: str | None = None


SOURCES: tuple[Source, ...] = (
    Source("reporter", "reporter", "NIH RePORTER ExPORTER (projects/abstracts/pubs/publink)",
           "per-fiscal-year", "nih-exporter", "us-public-domain"),
    Source("icite", "icite", "iCite article-level metrics (RCR) monthly snapshot",
           "monthly", "figshare", "us-public-domain"),
    Source("ctgov", "ctgov", "ClinicalTrials.gov v2-API full study records + nct↔pmid refs",
           "daily", "ctgov-api", "us-public-domain", watermark_strategy="page_token"),
    Source("scp", "scp", "State Cancer Profiles burden/risk/demographics",
           "monthly", "github-release", "us-public-domain"),
    Source("pmc", "pmc", "BioC-PMC full-text documents + passages",
           "on-rebuild", "biocpmc-bulk", "mixed-oa", watermark_strategy="max_range"),
    Source("openalex", "openalex", "OpenAlex works (Life+Health domains) + edge tables",
           "monthly", "s3-snapshot", "cc0", watermark_strategy="updated_date"),
    Source("census_geo", "ref", "US Census cartographic FIPS + boundaries (ref.geo_*)",
           "annual", "census-cartographic", "us-public-domain"),
    Source("europepmc", "europepmc", "Europe PMC text-mined annotations (PMCID↔term)",
           "monthly", "europepmc-bulk", "europepmc-terms"),
    Source("retractionwatch", "retractionwatch", "Retraction Watch retraction/correction notices",
           "weekday-daily", "crossref-gitlab-csv", "cc0", watermark_strategy="full"),
    # CC BY-NC 4.0: internal non-commercial use only, do NOT redistribute. The
    # license string is the machine-readable carry-forward for consumers.
    Source("reliance", "reliance", "Reliance on Science (Marx): patent↔paper links [NC]",
           "annual", "zenodo", "cc-by-nc-4.0"),
)


# --- Bootstrap (idempotent; run on every write-mode connect) ---


def bootstrap(con: duckdb.DuckDBPyConnection) -> None:
    """Create the ``lake_ops`` schema + tables (if absent) and refresh the registry.

    Idempotent and cheap: ``CREATE … IF NOT EXISTS`` plus a delete-then-insert of
    :data:`SOURCES`. Assumes the ``ops`` database is already attached.
    """
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {OPS}.{OPS_SCHEMA};")
    con.execute(
        f"""CREATE TABLE IF NOT EXISTS {_t("source")} (
            name TEXT, lake_schema TEXT, description TEXT, cadence TEXT,
            distribution TEXT, license TEXT, watermark_strategy TEXT,
            registered_at TIMESTAMPTZ
        );"""
    )
    con.execute(
        f"""CREATE TABLE IF NOT EXISTS {_t("run")} (
            run_id TEXT, source TEXT, target TEXT, version TEXT, status TEXT,
            snapshot_before BIGINT, snapshot_after BIGINT, rows_after BIGINT,
            started_at TIMESTAMPTZ, finished_at TIMESTAMPTZ, error TEXT, host TEXT
        );"""
    )
    con.execute(
        f"""CREATE TABLE IF NOT EXISTS {_t("watermark")} (
            source TEXT, name TEXT, value TEXT, updated_at TIMESTAMPTZ, set_by_run TEXT
        );"""
    )
    con.execute(
        f"""CREATE TABLE IF NOT EXISTS {_t("dataset_contract")} (
            lake_schema TEXT, view_name TEXT, contract_version INTEGER, columns TEXT,
            backing_table TEXT, status TEXT, published_at TIMESTAMPTZ
        );"""
    )
    _seed_sources(con)


def _seed_sources(con: duckdb.DuckDBPyConnection) -> None:
    """Refresh the source registry to match :data:`SOURCES` (delete-then-insert)."""
    for s in SOURCES:
        con.execute(f"DELETE FROM {_t('source')} WHERE name = ?", [s.name])
        con.execute(
            f"INSERT INTO {_t('source')} "
            "(name, lake_schema, description, cadence, distribution, license, "
            " watermark_strategy, registered_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, current_timestamp)",
            [s.name, s.lake_schema, s.description, s.cadence, s.distribution,
             s.license, s.watermark_strategy],
        )


# --- The run ledger ---


@dataclass
class Run:
    """A live run handle. Set :attr:`rows` inside the :func:`run` block."""

    con: duckdb.DuckDBPyConnection
    run_id: str
    source: str
    target: str
    version: str | None
    snapshot_before: int | None
    rows: int | None = None
    snapshot_after: int | None = None
    status: str | None = None
    _txn_depth: int = 0  # re-entrancy guard for nested attribute() blocks

    @property
    def changed(self) -> bool:
        """True when the upsert produced a new snapshot (not idempotent)."""
        return self.snapshot_after != self.snapshot_before

    def summary(self) -> dict:
        """The dict an ``ingest()`` returns — superset of the old hand-rolled one."""
        return {
            "table": self.target,
            "version": self.version,
            "rows": self.rows,
            "changed": self.changed,
            "snapshot": self.snapshot_after,
            "run_id": self.run_id,
            "status": self.status,
        }

    @contextmanager
    def attribute(self, op: str, *, message: str | None = None) -> Iterator[None]:
        """Wrap the enclosed write(s) in one **self-describing** DuckLake snapshot.

        Opens a transaction, stamps the snapshot it will produce with
        ``author='cdsci:<source>'``, a ``commit_message``, and a JSON
        ``commit_extra_info`` (``{writer, source, target, version, run_id, op}``)
        via ``set_commit_message``, runs the block, and commits. So the catalog
        itself attributes the snapshot — no ledger join, no table-id resolution
        (ADR-0007). One snapshot per block; the block rolls back on error.

        ``op`` is the sub-step label (e.g. ``"documents"``, ``"passages.shard2"``).
        Each block is its own transaction, so do **not** wrap an unbounded write in
        one block expecting it to stay small — keep the per-block write bounded
        (the PMC passages shards are sized for exactly this).

        **Re-entrant:** a nested ``attribute`` (e.g. PMC ``curate`` wrapping a
        ``_load`` that itself calls the now-self-attributing :func:`upsert`) joins
        the outer transaction — the outermost block owns the BEGIN/COMMIT and the
        commit message; inner blocks just run. So one snapshot, no nested BEGIN.
        """
        if self._txn_depth > 0:
            self._txn_depth += 1
            try:
                yield
            finally:
                self._txn_depth -= 1
            return

        extra = json.dumps({
            "writer": "cdsci", "source": self.source, "target": self.target,
            "version": self.version, "run_id": self.run_id, "op": op,
        })
        self.con.execute("BEGIN;")
        self.con.execute(
            f"CALL {LAKE}.set_commit_message(?, ?, extra_info => ?);",
            [f"cdsci:{self.source}", message or f"{self.source}: {op}", extra],
        )
        self._txn_depth = 1
        try:
            yield
        except BaseException:
            self.con.execute("ROLLBACK;")
            raise
        else:
            self.con.execute("COMMIT;")
        finally:
            self._txn_depth = 0


def _max_snapshot(con: duckdb.DuckDBPyConnection) -> int | None:
    """Current max DuckLake snapshot id, or None on an empty lake."""
    return con.execute(f"SELECT max(snapshot_id) FROM {LAKE}.snapshots()").fetchone()[0]


@contextmanager
def run(
    con: duckdb.DuckDBPyConnection,
    *,
    source: str,
    target: str,
    version: str | None = None,
    host: str | None = None,
) -> Iterator[Run]:
    """Record one ``lake_ops.run`` row around a curate/upsert.

    On enter: capture ``snapshot_before`` and insert a ``running`` row. Inside the
    block set ``r.rows`` to the upsert's row count. On exit: capture
    ``snapshot_after`` and finalize status — ``error`` if the block raised, else
    ``idempotent`` when no snapshot was added, else ``success``.

        with ops.run(con, source="icite", target=target, version=version) as r:
            r.rows = curate(con, paths, version, target=target, limit=limit)
        return r.summary()
    """
    rid = str(uuid.uuid4())
    host = host or socket.gethostname()
    before = _max_snapshot(con)
    con.execute(
        f"INSERT INTO {_t('run')} "
        "(run_id, source, target, version, status, snapshot_before, started_at, host) "
        "VALUES (?, ?, ?, ?, 'running', ?, current_timestamp, ?)",
        [rid, source, target, version, before, host],
    )
    bound = logger.bind(ctx=f"run:{source}")
    bound.info(
        "start → {} (version={}, snapshot_before={}, run_id={})",
        target, version, before, rid,
    )
    r = Run(con, rid, source, target, version, before)
    token = _ACTIVE_RUN.set(r)
    try:
        try:
            yield r
        except Exception as exc:  # noqa: BLE001 — record then re-raise
            after = _max_snapshot(con)
            con.execute(
                f"UPDATE {_t('run')} SET status='error', snapshot_after=?, rows_after=?, "
                "finished_at=current_timestamp, error=? WHERE run_id=?",
                [after, r.rows, str(exc)[:2000], rid],
            )
            r.snapshot_after, r.status = after, "error"
            bound.error(
                "ERROR after {} rows (snapshot {}→{}, run_id={}): {}",
                r.rows, before, after, rid, exc,
            )
            raise
        else:
            after = _max_snapshot(con)
            status = "idempotent" if after == before else "success"
            con.execute(
                f"UPDATE {_t('run')} SET status=?, snapshot_after=?, rows_after=?, "
                "finished_at=current_timestamp WHERE run_id=?",
                [status, after, r.rows, rid],
            )
            r.snapshot_after, r.status = after, status
            bound.success(
                "{} → {} (rows={}, snapshot {}→{}, run_id={})",
                status, target, r.rows, before, after, rid,
            )
    finally:
        _ACTIVE_RUN.reset(token)


def last_run(
    con: duckdb.DuckDBPyConnection, source: str, *, status: str | None = None
) -> dict | None:
    """The most recent run for ``source`` (optionally filtered to a ``status``)."""
    where = "source = ?"
    params: list[Any] = [source]
    if status is not None:
        where += " AND status = ?"
        params.append(status)
    row = con.execute(
        f"SELECT run_id, source, target, version, status, snapshot_before, "
        f"snapshot_after, rows_after, started_at::VARCHAR, finished_at::VARCHAR, error "
        f"FROM {_t('run')} WHERE {where} ORDER BY started_at DESC LIMIT 1",
        params,
    ).fetchone()
    if row is None:
        return None
    cols = ("run_id", "source", "target", "version", "status", "snapshot_before",
            "snapshot_after", "rows_after", "started_at", "finished_at", "error")
    return dict(zip(cols, row, strict=True))


# --- Watermarks (incremental cursors; in-place) ---


def get_watermark(con: duckdb.DuckDBPyConnection, source: str, name: str) -> Any | None:
    """The cursor value for ``(source, name)``, JSON-decoded, or None if unset."""
    row = con.execute(
        f"SELECT value FROM {_t('watermark')} WHERE source = ? AND name = ?",
        [source, name],
    ).fetchone()
    return json.loads(row[0]) if row else None


def set_watermark(
    con: duckdb.DuckDBPyConnection,
    source: str,
    name: str,
    value: Any,
    *,
    run_id: str | None = None,
) -> None:
    """Set the ``(source, name)`` cursor to ``value`` (JSON-encoded; in-place)."""
    payload = json.dumps(value)
    con.execute(
        f"DELETE FROM {_t('watermark')} WHERE source = ? AND name = ?", [source, name]
    )
    con.execute(
        f"INSERT INTO {_t('watermark')} (source, name, value, updated_at, set_by_run) "
        "VALUES (?, ?, ?, current_timestamp, ?)",
        [source, name, payload, run_id],
    )
