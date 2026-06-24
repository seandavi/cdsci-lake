"""Offline tests for the ``cdsci.lake.ops`` operational ledger (ADR-0006).

Exercise the ledger against the local sibling-file backend: bootstrap +
registry, the :func:`ops.run` context manager (success / idempotent / error),
``last_run``, and watermark round-trips. No network, no Postgres.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from cdsci.lake import Settings, lake_connect, ops, upsert


@pytest.fixture
def lake_settings(tmp_path: Path) -> Settings:
    return Settings(storage_base_uri=f"file://{tmp_path}")


def test_bootstrap_seeds_registry(lake_settings: Settings):
    """A write-mode connect attaches ops, creates tables, and seeds the registry."""
    con = lake_connect(lake_settings)
    try:
        names = {r[0] for r in con.execute("SELECT name FROM ops.lake_ops.source").fetchall()}
        assert names == {s.name for s in ops.SOURCES}
        # All four ledger tables exist.
        tables = {
            r[0]
            for r in con.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_catalog='ops' AND table_schema='lake_ops'"
            ).fetchall()
        }
        assert {"source", "run", "watermark", "dataset_contract"} <= tables
    finally:
        con.close()


def test_bootstrap_idempotent_no_duplicate_sources(lake_settings: Settings):
    """Re-connecting refreshes (not duplicates) the registry rows."""
    lake_connect(lake_settings).close()
    con = lake_connect(lake_settings)
    try:
        n = con.execute("SELECT count(*) FROM ops.lake_ops.source").fetchone()[0]
        assert n == len(ops.SOURCES)
    finally:
        con.close()


def test_read_only_skips_ops(lake_settings: Settings):
    """Read-only consumers don't attach the ledger (a writer concern)."""
    lake_connect(lake_settings).close()  # create the catalog first
    con = lake_connect(lake_settings, read_only=True)
    try:
        rows = con.execute("SELECT database_name FROM duckdb_databases()").fetchall()
        attached = {r[0] for r in rows}
        assert "lake" in attached
        assert "ops" not in attached
    finally:
        con.close()


def test_run_records_success_then_idempotent(lake_settings: Settings):
    """A real upsert → status 'success'; an identical re-run → 'idempotent'."""
    con = lake_connect(lake_settings)
    try:
        src = "SELECT * FROM (VALUES (1,'a'),(2,'b')) v(id,val)"
        with ops.run(con, source="icite", target="lake.main.t", version="2026-05") as r:
            r.rows = upsert(con, "lake.main.t", src, key="id")
        assert r.status == "success"
        assert r.changed is True
        assert r.summary() == {
            "table": "lake.main.t", "version": "2026-05", "rows": 2,
            "changed": True, "snapshot": r.snapshot_after, "run_id": r.run_id,
            "status": "success",
        }

        with ops.run(con, source="icite", target="lake.main.t", version="2026-06") as r2:
            r2.rows = upsert(con, "lake.main.t", src, key="id")  # same data
        assert r2.status == "idempotent"
        assert r2.changed is False
        assert r2.snapshot_after == r2.snapshot_before

        # Two run rows recorded; last_run returns the idempotent one.
        assert con.execute("SELECT count(*) FROM ops.lake_ops.run").fetchone()[0] == 2
        last = ops.last_run(con, "icite")
        assert last["status"] == "idempotent" and last["version"] == "2026-06"
        assert last["finished_at"] is not None
        # last success filter skips the idempotent run.
        assert ops.last_run(con, "icite", status="success")["version"] == "2026-05"
    finally:
        con.close()


def test_run_records_error_and_reraises(lake_settings: Settings):
    """A raise inside the block is recorded as 'error' and propagated."""
    con = lake_connect(lake_settings)
    try:
        with pytest.raises(ValueError, match="boom"), \
                ops.run(con, source="scp", target="lake.scp.incidence"):
            raise ValueError("boom")
        row = ops.last_run(con, "scp")
        assert row["status"] == "error"
        assert "boom" in row["error"]
        assert row["finished_at"] is not None
    finally:
        con.close()


def test_watermark_roundtrip(lake_settings: Settings):
    """Set/get a cursor; overwrite in place; missing returns None; values JSON-typed."""
    con = lake_connect(lake_settings)
    try:
        assert ops.get_watermark(con, "openalex", "updated_date") is None

        ops.set_watermark(con, "openalex", "updated_date", "2026-05-01")
        assert ops.get_watermark(con, "openalex", "updated_date") == "2026-05-01"

        # In-place overwrite — one row per (source, name).
        ops.set_watermark(con, "openalex", "updated_date", "2026-06-01")
        assert ops.get_watermark(con, "openalex", "updated_date") == "2026-06-01"
        assert con.execute(
            "SELECT count(*) FROM ops.lake_ops.watermark "
            "WHERE source='openalex' AND name='updated_date'"
        ).fetchone()[0] == 1

        # Non-scalar cursor round-trips through JSON.
        ops.set_watermark(con, "ctgov", "page_token", {"token": "abc", "page": 7})
        assert ops.get_watermark(con, "ctgov", "page_token") == {"token": "abc", "page": 7}
    finally:
        con.close()
