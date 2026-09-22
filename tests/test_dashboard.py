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
