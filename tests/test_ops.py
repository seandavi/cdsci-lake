"""Offline tests for the ``cdsci.lake.ops`` operational ledger (ADR-0006).

Exercise the ledger against the local sibling-file backend: bootstrap +
registry, the :func:`ops.run` context manager (success / idempotent / error),
``last_run``, and watermark round-trips. No network, no Postgres.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from cdsci.lake import Settings, lake_connect, ops, upsert


@pytest.fixture
def lake_settings(tmp_path: Path) -> Settings:
    return Settings(storage_base_uri=f"file://{tmp_path}")


def test_connect_creates_tables_but_does_not_seed(lake_settings: Settings):
    """bootstrap is schema-only; the substrate never force-seeds on connect (ADR-0011 §4)."""
    con = lake_connect(lake_settings)
    try:
        # All four ledger tables exist...
        tables = {
            r[0]
            for r in con.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_catalog='ops' AND table_schema='lake_ops'"
            ).fetchall()
        }
        assert {"source", "run", "watermark", "dataset_contract"} <= tables
        # ...but the registry is empty — no producer's sources are force-registered.
        assert con.execute("SELECT count(*) FROM ops.lake_ops.source").fetchone()[0] == 0

        ops.bootstrap(con)  # a second bootstrap is still schema-only — must not seed
        assert con.execute("SELECT count(*) FROM ops.lake_ops.source").fetchone()[0] == 0
    finally:
        con.close()


def test_register_sources_populates_writer_and_is_idempotent(lake_settings: Settings):
    """Explicit register_sources seeds the writer column; a re-register refreshes, not dupes."""
    con = lake_connect(lake_settings)
    try:
        ops.register_sources(con, writer="cdsci", sources=ops.SOURCES)
        rows = con.execute("SELECT name, writer FROM ops.lake_ops.source").fetchall()
        assert {name for name, _ in rows} == {s.name for s in ops.SOURCES}
        assert {writer for _, writer in rows} == {"cdsci"}

        ops.register_sources(con, writer="cdsci", sources=ops.SOURCES)
        assert con.execute(
            "SELECT count(*) FROM ops.lake_ops.source"
        ).fetchone()[0] == len(ops.SOURCES)
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
        # icite is NOT pre-registered (connect no longer seeds) — the run must
        # self-register the built-in source so attribution is cdsci:icite.
        assert con.execute(
            "SELECT count(*) FROM ops.lake_ops.source WHERE name='icite'"
        ).fetchone()[0] == 0
        src = "SELECT * FROM (VALUES (1,'a'),(2,'b')) v(id,val)"
        with ops.run(con, source="icite", target="lake.main.t", version="2026-05") as r:
            r.rows = upsert(con, "lake.main.t", src, key="id")
        assert con.execute(
            "SELECT writer FROM ops.lake_ops.source WHERE name='icite'"
        ).fetchone()[0] == "cdsci"
        assert r.status == "success"
        assert r.changed is True
        assert r.summary() == {
            "table": "lake.main.t", "version": "2026-05", "rows": 2,
            "changed": True, "snapshot": r.snapshot_after, "run_id": r.run_id,
            "status": "success",
        }

        # The snapshot upsert produced is self-describing (ADR-0009): authored by
        # the source, op = the table name, and bound back to this run_id.
        author, extra = con.execute(
            "SELECT author, commit_extra_info FROM lake.snapshots() WHERE snapshot_id = ?",
            [r.snapshot_after],
        ).fetchone()
        assert author == "cdsci:icite"
        meta = json.loads(extra)
        assert meta["source"] == "icite" and meta["op"] == "t" and meta["run_id"] == r.run_id

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


def test_run_derives_writer_and_merges_extra(lake_settings: Settings):
    """run() derives writer from the registry and merges extra= into commit_extra_info."""
    con = lake_connect(lake_settings)
    try:
        src = "SELECT * FROM (VALUES (1,'a')) v(id,val)"
        with ops.run(
            con, source="icite", target="lake.main.t2", version="v1",
            extra={"prefect_run_id": "abc-123"},
        ) as r:
            assert r.writer == "cdsci"  # self-registered built-in, then derived
            r.rows = upsert(con, "lake.main.t2", src, key="id")

        author, extra = con.execute(
            "SELECT author, commit_extra_info FROM lake.snapshots() WHERE snapshot_id = ?",
            [r.snapshot_after],
        ).fetchone()
        assert author == "cdsci:icite"
        meta = json.loads(extra)
        assert meta["writer"] == "cdsci"
        # Per-producer key flows through on top of the canonical keys.
        assert meta["prefect_run_id"] == "abc-123"
    finally:
        con.close()


def test_run_foreign_producer_source(lake_settings: Settings):
    """A foreign producer registers its own source; run() attributes it, touches no cdsci rows."""
    con = lake_connect(lake_settings)
    try:
        ops.register_sources(
            con, writer="omicidx",
            sources=(ops.Source("sra", "omicidx", "SRA", "daily", "ncbi", "us-public-domain"),),
        )
        src = "SELECT * FROM (VALUES (1,'a')) v(id,val)"
        with ops.run(con, source="sra", target="lake.omicidx.sra", version="v1") as r:
            assert r.writer == "omicidx"
            r.rows = upsert(con, "lake.omicidx.sra", src, key="id")
        author = con.execute(
            "SELECT author FROM lake.snapshots() WHERE snapshot_id = ?", [r.snapshot_after]
        ).fetchone()[0]
        assert author == "omicidx:sra"
        # "sra" isn't in SOURCES → no self-registration, so no cdsci rows leaked in.
        names = {n for (n,) in con.execute("SELECT name FROM ops.lake_ops.source").fetchall()}
        assert names == {"sra"}
    finally:
        con.close()


def test_run_unregistered_source_falls_back_to_source_name(lake_settings: Settings):
    """A source neither in SOURCES nor registered → warns, writer defaults to its name."""
    con = lake_connect(lake_settings)
    try:
        with ops.run(con, source="not_a_real_source", target="lake.x.y") as r:
            assert r.writer == "not_a_real_source"
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
