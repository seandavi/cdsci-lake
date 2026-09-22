"""Offline tests for ``cdsci.lake.log`` structured JSON events (design §8.2, ADR-0009).

Exercises :func:`log.configure`'s JSON sink, :func:`log.event`, the redaction guard,
and that ``ops.run``'s lifecycle emits a shared ``run_id``. No network.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

from cdsci.lake import Settings, lake_connect, log, ops, upsert


@pytest.fixture(autouse=True)
def _reset_logger():
    """Loguru's ``logger`` is a process-wide singleton -- leave it sinkless and
    unconfigured after every test so the rest of the suite stays silent (ADR-0009)."""
    yield
    log.logger.remove()
    log._configured = False


def _json_lines(text: str) -> list[dict]:
    return [json.loads(line) for line in text.splitlines() if line.strip()]


def test_json_event_has_required_keys_when_bound(capsys: pytest.CaptureFixture[str]):
    log.configure("INFO", json=True, force=True)
    log.event(
        "publish_completed", run_id="r1", writer="w", job="j",
        asset="release.demo.r1", release="r1", rows=42, status="success",
        duration_ms=12.5,
    )
    lines = _json_lines(capsys.readouterr().err)
    assert len(lines) == 1
    record = lines[0]
    for key in (
        "timestamp", "level", "event", "run_id", "writer", "job",
        "asset", "release", "rows", "status", "duration_ms", "message",
    ):
        assert key in record, key
    assert record["timestamp"].endswith("Z")
    assert record["event"] == "publish_completed"
    assert record["run_id"] == "r1"


def test_json_event_omits_unbound_optional_keys(capsys: pytest.CaptureFixture[str]):
    log.configure("INFO", json=True, force=True)
    log.event("run_started", run_id="r1", writer="w", job="j", status="running")
    record = _json_lines(capsys.readouterr().err)[0]
    assert "asset" not in record
    assert "release" not in record
    assert "rows" not in record


def test_json_redacts_secret_shaped_field(capsys: pytest.CaptureFixture[str]):
    log.configure("INFO", json=True, force=True)
    log.event("run_started", run_id="r1", job="token=abc123", status="running")
    raw = capsys.readouterr().err
    assert "token=abc123" not in raw
    record = _json_lines(raw)[0]
    assert record["job"] == "<redacted>"


def test_ops_run_emits_start_and_success_with_shared_run_id(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
):
    log.configure("INFO", json=True, force=True)
    settings = Settings(storage_base_uri=f"file://{tmp_path}")
    con = lake_connect(settings)
    try:
        src = "SELECT * FROM (VALUES (1,'a'),(2,'b'),(3,'c')) v(id,val)"
        with ops.run(con, source="icite", target="lake.main.t", version="v1") as r:
            r.rows = upsert(con, "lake.main.t", src, key="id")
    finally:
        con.close()
    lines = _json_lines(capsys.readouterr().err)
    events = {rec["event"]: rec for rec in lines if "event" in rec}
    assert "run_started" in events and "run_succeeded" in events
    run_id = events["run_started"]["run_id"]
    assert events["run_succeeded"]["run_id"] == run_id
    assert events["run_succeeded"]["status"] == "success"
    assert events["run_succeeded"]["rows"] == 3
    assert "duration_ms" in events["run_succeeded"]


def test_json_false_output_is_human_readable(capsys: pytest.CaptureFixture[str]):
    log.configure("INFO", json=False, force=True)
    log.logger.bind(ctx="test").info("hello")
    out = capsys.readouterr().err
    assert "hello" in out
    with pytest.raises(json.JSONDecodeError):
        json.loads(out.splitlines()[0])


def test_library_import_emits_nothing():
    out = subprocess.run(
        [sys.executable, "-c", "import cdsci.lake"], capture_output=True, text=True
    )
    assert out.stderr == ""
