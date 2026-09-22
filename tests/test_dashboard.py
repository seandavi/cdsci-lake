import os
import sys

from fastapi.testclient import TestClient

# Ensure backend and src are in python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../backend")))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

from main import app

from cdsci.lake import Settings, lake_connect, ops

client = TestClient(app)

def test_health():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok", "service": "ducklake-dashboard-api"}

def test_get_sources():
    response = client.get("/api/sources")
    assert response.status_code == 200
    assert isinstance(response.json(), list)

def test_get_runs():
    response = client.get("/api/runs")
    assert response.status_code == 200
    assert isinstance(response.json(), list)

def test_get_snapshots():
    response = client.get("/api/snapshots")
    assert response.status_code == 200
    assert isinstance(response.json(), list)

def test_get_logs_nonexistent():
    response = client.get("/api/logs/nonexistent-run-id")
    assert response.status_code == 200
    assert response.json() == []


def test_get_snapshots_shows_run_id_for_a_sqlmesh_synced_snapshot(tmp_path, monkeypatch):
    """A SQLMesh-written snapshot carries no `commit_extra_info` -- the endpoint
    falls back to the `lake_ops.snapshot_attribution` side table a sync step
    populates for it (ADR-0008 Amendment, cdsci-lake#89)."""
    import asyncio

    import repository

    settings = Settings(storage_base_uri=f"file://{tmp_path}")
    con = lake_connect(settings)
    con.execute("CREATE SCHEMA lake.bugsigdb;")
    con.execute("CREATE TABLE lake.bugsigdb.signature AS SELECT 1 AS id")
    run_id = ops.sync_sqlmesh_snapshot_attribution(
        con, project="cdsci_lake", model="bugsigdb.signature",
        target="lake.bugsigdb.signature",
    )
    con.close()
    assert run_id is not None

    monkeypatch.setattr(repository, "get_settings", lambda: settings)
    test_repo = repository.DuckDBDashboardRepository()
    snapshots = asyncio.run(test_repo.get_snapshots())

    matches = [s for s in snapshots if s.run_id == run_id]
    assert matches, f"no snapshot carried run_id={run_id}: {snapshots}"


def test_get_snapshots_prefers_in_catalog_run_id_over_a_conflicting_side_table_row(
    tmp_path, monkeypatch
):
    """A snapshot's own commit_extra_info.run_id must win over a conflicting
    lake_ops.snapshot_attribution row for the same snapshot_id -- the endpoint
    only falls back to the side table when the snapshot carries no run_id of its
    own (ADR-0008 Amendment, cdsci-lake#89)."""
    import asyncio

    import repository

    settings = Settings(storage_base_uri=f"file://{tmp_path}")
    con = lake_connect(settings)
    src = "SELECT 1 AS id"
    with ops.run(con, source="icite", target="lake.main.t", version="v1") as r:
        from cdsci.lake import upsert

        r.rows = upsert(con, "lake.main.t", src, key="id")
    # A conflicting side-table row for the same snapshot -- must never win.
    con.execute(
        "INSERT INTO ops.lake_ops.snapshot_attribution (snapshot_id, run_id, source) "
        "VALUES (?, 'not-the-real-run-id', 'sqlmesh_sync')",
        [r.snapshot_after],
    )
    con.close()

    monkeypatch.setattr(repository, "get_settings", lambda: settings)
    test_repo = repository.DuckDBDashboardRepository()
    snapshots = asyncio.run(test_repo.get_snapshots())

    match = next(s for s in snapshots if s.snapshot_id == r.snapshot_after)
    assert match.run_id == r.run_id


def test_get_snapshots_survives_an_absent_attribution_side_table(tmp_path, monkeypatch):
    """An absent lake_ops.snapshot_attribution table (e.g. an older lake) must
    fail only the attribution lookup, not blank the snapshot rows too -- the two
    reads are independent (P2 finding 7). `lake_connect(with_ops=True)` always
    re-bootstraps the table, so the absent-table path is exercised by making
    the attribution lookup itself raise -- the same failure a real absent table
    produces inside `_read`."""
    import asyncio

    import duckdb
    import repository

    settings = Settings(storage_base_uri=f"file://{tmp_path}")
    con = lake_connect(settings)
    src = "SELECT 1 AS id"
    with ops.run(con, source="icite", target="lake.main.t", version="v1") as r:
        from cdsci.lake import upsert

        r.rows = upsert(con, "lake.main.t", src, key="id")
    con.close()

    def _raise_absent(con, ids):
        raise duckdb.CatalogException("Table with name snapshot_attribution does not exist!")

    monkeypatch.setattr(repository, "get_settings", lambda: settings)
    monkeypatch.setattr(repository.ops, "snapshot_run_ids", _raise_absent)
    test_repo = repository.DuckDBDashboardRepository()
    snapshots = asyncio.run(test_repo.get_snapshots())

    assert any(s.snapshot_id == r.snapshot_after for s in snapshots)
