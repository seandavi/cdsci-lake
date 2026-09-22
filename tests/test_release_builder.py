"""Offline + local-HTTP acceptance for ``cdsci.lake.publish.builder`` (cdsci-lake#95 M1).

Builds the shared ``tests/fixtures/contracts`` dataset into a ``LocalDirStore``,
then exercises design §11.5/§11.7's acceptance path: determinism, cold
``verify_release``, corruption detection, a local HTTP server (HEAD/GET/Range +
DuckDB ``read_parquet`` over ``http://``), and the ``lake_ops`` receipt round-trip.
"""

from __future__ import annotations

import functools
import hashlib
import http.server
import io
import os
import threading
import time
from pathlib import Path

import duckdb
import httpx
from fixtures.contracts import dataset as fx

from cdsci.lake import Settings, lake_connect, ops
from cdsci.lake.publish.builder import LocalDirStore, build_release, record_release, verify_release
from cdsci.lake.publish.release import ArtifactStatus, ReleaseCandidate, SourceAssetVersion

RUN_ID = "01927c9e-0000-7000-8000-00000000b001"


# ponytail: stdlib http.server has no Range support at all (verified against this
# interpreter) -- this is a test-only shim so §11.7's byte-range check has something
# to exercise locally; a real object store (S3/R2) already serves Range natively.
class _RangeHandler(http.server.SimpleHTTPRequestHandler):
    def send_head(self):
        range_header = self.headers.get("Range")
        path = self.translate_path(self.path)
        if not range_header or not os.path.isfile(path):
            return super().send_head()
        size = os.path.getsize(path)
        start_s, _, end_s = range_header.removeprefix("bytes=").partition("-")
        start = int(start_s) if start_s else 0
        end = min(int(end_s), size - 1) if end_s else size - 1
        with open(path, "rb") as fh:
            fh.seek(start)
            chunk = fh.read(end - start + 1)
        self.send_response(206)
        self.send_header("Content-Type", self.guess_type(path))
        self.send_header("Content-Range", f"bytes {start}-{end}/{size}")
        self.send_header("Content-Length", str(len(chunk)))
        self.end_headers()
        return io.BytesIO(chunk)


def _candidate() -> ReleaseCandidate:
    return ReleaseCandidate(
        dataset="demo-catalog",
        release="R1",
        run_id=RUN_ID,
        built_at="2026-09-22T00:00:00Z",
        destination="local://demo-catalog/R1",
        tables=("demo.events", "demo.entities"),
        status=ArtifactStatus.STAGED,
        source_asset_versions=(
            SourceAssetVersion(ref="lake.demo.events", version="snapshot:1"),
            SourceAssetVersion(ref="lake.demo.entities", version="snapshot:1"),
        ),
    )


def _tables(con: duckdb.DuckDBPyConnection) -> dict[str, duckdb.DuckDBPyRelation]:
    events = con.sql(
        "SELECT * FROM (VALUES "
        "('e1', '2026-01-01T00:00:00Z', 'p1'), "
        "('e2', '2026-01-02T00:00:00Z', 'p2'), "
        "('e3', '2026-01-03T00:00:00Z', NULL::VARCHAR) "
        ") v(event_id, occurred_at, payload)"
    )
    entities = con.sql(
        "SELECT * FROM (VALUES "
        "('e1', 'alpha', 'writer_a', 'R1', NULL::VARCHAR), "
        "('e2', 'beta', 'writer_a', 'R1', NULL::VARCHAR), "
        "('w1', 'zed', 'writer_b', 'R0', NULL::VARCHAR) "
        ") v(entity_id, label, source, valid_from, valid_to)"
    )
    return {"demo.events": events, "demo.entities": entities}


def _events_parquet_path(dataset_root: Path) -> Path:
    return (
        dataset_root / "demo-catalog" / "R1" / "tables" / "demo.events" / "data"
        / "part-00000.parquet"
    )


def test_build_release_is_byte_identical_across_repeat_builds(tmp_path: Path):
    con = duckdb.connect()
    store = LocalDirStore(root=tmp_path)
    candidate = _candidate()

    manifest1 = build_release(candidate, _tables(con), store, contract=fx.DATASET_CONTRACT)
    first = {p: p.read_bytes() for p in tmp_path.rglob("*") if p.is_file()}

    manifest2 = build_release(candidate, _tables(con), store, contract=fx.DATASET_CONTRACT)
    second = {p: p.read_bytes() for p in tmp_path.rglob("*") if p.is_file()}

    assert manifest1 == manifest2
    assert first == second
    assert manifest1.dataset == "demo-catalog" and manifest1.release == "R1"
    assert {t.name for t in manifest1.tables} == {"demo.events", "demo.entities"}


def test_verify_release_passes_on_a_clean_build(tmp_path: Path):
    con = duckdb.connect()
    store = LocalDirStore(root=tmp_path)
    build_release(_candidate(), _tables(con), store, contract=fx.DATASET_CONTRACT)

    report = verify_release(store, "demo-catalog", "R1")
    assert report.passed is True
    assert report.run_id == RUN_ID


def test_verify_release_fails_on_corrupted_file(tmp_path: Path):
    con = duckdb.connect()
    store = LocalDirStore(root=tmp_path)
    build_release(_candidate(), _tables(con), store, contract=fx.DATASET_CONTRACT)

    events_path = _events_parquet_path(tmp_path)
    data = bytearray(events_path.read_bytes())
    data[-1] ^= 0xFF  # flip the last byte -- corrupts content without changing size
    events_path.write_bytes(bytes(data))

    report = verify_release(store, "demo-catalog", "R1")
    assert report.passed is False
    failing = [c.name for c in report.checks if not c.passed]
    assert any("demo.events" in name and "checksum" in name for name in failing)


def test_local_http_acceptance_head_get_range_and_duckdb_read_parquet(tmp_path: Path):
    con = duckdb.connect()
    store = LocalDirStore(root=tmp_path)
    build_release(_candidate(), _tables(con), store, contract=fx.DATASET_CONTRACT)

    handler = functools.partial(_RangeHandler, directory=str(tmp_path))
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    port = server.server_port
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    time.sleep(0.1)
    try:
        base = f"http://127.0.0.1:{port}/demo-catalog/R1/tables/demo.events/data/part-00000.parquet"

        head = httpx.head(base)
        assert head.status_code == 200
        assert int(head.headers["content-length"]) == _events_parquet_path(tmp_path).stat().st_size

        full = httpx.get(base)
        assert full.status_code == 200
        assert hashlib.sha256(full.content).hexdigest() == hashlib.sha256(
            _events_parquet_path(tmp_path).read_bytes()
        ).hexdigest()

        ranged = httpx.get(base, headers={"Range": "bytes=0-9"})
        assert ranged.status_code == 206
        assert len(ranged.content) == 10

        count = duckdb.sql(f"SELECT count(*) FROM read_parquet('{base}')").fetchone()[0]
        assert count == 3
    finally:
        server.shutdown()


def test_record_release_writes_receipt_and_publishes_lineage(tmp_path: Path):
    con_build = duckdb.connect()
    store = LocalDirStore(root=tmp_path / "store")
    candidate = _candidate()
    manifest = build_release(candidate, _tables(con_build), store, contract=fx.DATASET_CONTRACT)
    report = verify_release(store, "demo-catalog", "R1")
    assert report.passed is True

    lake_settings = Settings(storage_base_uri=f"file://{tmp_path / 'lake'}")
    con = lake_connect(lake_settings)
    try:
        receipt = record_release(con, manifest, report)
        assert receipt.status == ArtifactStatus.PUBLISHED
        assert receipt.dataset == "demo-catalog" and receipt.release == "R1"

        got = ops.publication_receipts(con, "R1")
        assert got == [receipt]

        assets = {a["ref"]: a for a in ops.list_assets(con)}
        assert "release.demo-catalog.R1" in assets
        assert assets["release.demo-catalog.R1"]["asset_type"] == "release"

        upstream = ops.lineage_for(con, "release.demo-catalog.R1", direction="upstream")
        assert {e["src_ref"] for e in upstream} == {"lake.demo.events", "lake.demo.entities"}
        assert all(e["edge_type"] == "publishes" for e in upstream)
    finally:
        con.close()
