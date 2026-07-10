import os
import sys
import pytest
from fastapi.testclient import TestClient

# Ensure backend and src are in python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../backend")))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

from main import app

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
