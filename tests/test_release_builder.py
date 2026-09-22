"""Offline + local-HTTP acceptance for ``cdsci.lake.publish.builder``/``verify`` (cdsci-lake#95 M1).

Builds the shared ``tests/fixtures/contracts`` dataset into a ``LocalDirStore``,
then exercises design §11.5/§11.7's acceptance path: determinism (including tied
sort keys), cold ``verify_release`` (clean, corrupted, and tampered), the
build → verify → finalize → record sequencing, a local HTTP server (HEAD/GET/Range
+ DuckDB ``read_parquet`` over ``http://``), and the ``lake_ops`` receipt round-trip.
"""

from __future__ import annotations

import dataclasses
import functools
import hashlib
import http.server
import io
import os
import threading
import time
from pathlib import Path, PurePosixPath

import duckdb
import httpx
import pytest
from fixtures.contracts import dataset as fx

from cdsci.lake import Settings, lake_connect, ops
from cdsci.lake.contracts import ColumnContract, DatasetContract, TableContract, TemporalModel
from cdsci.lake.publish.builder import (
    LocalDirStore,
    build_release,
    finalize_release,
    record_release,
)
from cdsci.lake.publish.release import ArtifactStatus, ReleaseCandidate, SourceAssetVersion
from cdsci.lake.publish.verify import verify_release

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


def _candidate(release: str = "R1") -> ReleaseCandidate:
    return ReleaseCandidate(
        dataset="demo-catalog",
        release=release,
        run_id=RUN_ID,
        built_at="2026-09-22T00:00:00Z",
        destination=f"local://demo-catalog/{release}",
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


def _events_parquet_path(dataset_root: Path, release: str = "R1") -> Path:
    return (
        dataset_root / "demo-catalog" / release / "tables" / "demo.events" / "data"
        / "part-00000.parquet"
    )


def _schema_path(dataset_root: Path, table: str, release: str = "R1") -> Path:
    return dataset_root / "demo-catalog" / release / "tables" / table / "schema.json"


def _built_and_finalized(con: duckdb.DuckDBPyConnection, store: LocalDirStore):
    manifest = build_release(_candidate(), _tables(con), store, contract=fx.DATASET_CONTRACT)
    report = verify_release(store, "demo-catalog", "R1", manifest=manifest)
    finalize_release(store, manifest, report)
    return manifest


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


def test_build_release_sort_by_ties_broken_by_primary_key_deterministically(tmp_path: Path):
    """B1: ``sort_cols`` must be ``(*sort_by, *primary_key not in sort_by)`` -- a
    ``sort_by`` that doesn't cover the full key still needs one fixed row order among
    ties, not whatever order the input relation happened to arrive in."""
    table = TableContract(
        name="demo.ties",
        description="Tied-sort-key conformance fixture.",
        grain="one row per id",
        primary_key=("id",),
        temporal_model=TemporalModel.APPEND_IMMUTABLE,
        owner="cdsci-lake",
        license="cc0",
        columns=(
            ColumnContract("a", "string", "tie key", nullable=False),
            ColumnContract("id", "string", "business key", nullable=False),
        ),
        sort_by=("a",),
    )
    contract = DatasetContract(
        id="ties-catalog", title="t", description="d", publisher="cdsci-lake",
        tables={"demo.ties": table},
    )
    candidate = ReleaseCandidate(
        dataset="ties-catalog", release="R1", run_id=RUN_ID, built_at="2026-09-22T00:00:00Z",
        destination="local://ties-catalog/R1", tables=("demo.ties",), status=ArtifactStatus.STAGED,
    )
    con = duckdb.connect()
    forward = con.sql("SELECT * FROM (VALUES ('x', '1'), ('x', '2'), ('x', '3')) v(a, id)")
    backward = con.sql("SELECT * FROM (VALUES ('x', '3'), ('x', '2'), ('x', '1')) v(a, id)")

    store1 = LocalDirStore(root=tmp_path / "store1")
    build_release(candidate, {"demo.ties": forward}, store1, contract=contract)
    store2 = LocalDirStore(root=tmp_path / "store2")
    build_release(candidate, {"demo.ties": backward}, store2, contract=contract)

    data1 = (tmp_path / "store1" / "ties-catalog" / "R1" / "tables" / "demo.ties" / "data"
              / "part-00000.parquet").read_bytes()
    data2 = (tmp_path / "store2" / "ties-catalog" / "R1" / "tables" / "demo.ties" / "data"
              / "part-00000.parquet").read_bytes()
    assert data1 == data2


def test_build_release_rejects_partition_by(tmp_path: Path):
    """S7: partitioned output isn't implemented yet -- fail loudly, not silently
    write one file and ignore ``partition_by``."""
    table = dataclasses.replace(fx.EVENTS_TABLE, partition_by=("event_id",))
    contract = dataclasses.replace(
        fx.DATASET_CONTRACT, tables={**fx.DATASET_CONTRACT.tables, "demo.events": table}
    )
    con = duckdb.connect()
    store = LocalDirStore(root=tmp_path)
    with pytest.raises(ValueError, match="partition_by"):
        build_release(_candidate(), _tables(con), store, contract=contract)


def test_put_if_absent_rejects_different_bytes_for_existing_path(tmp_path: Path):
    store = LocalDirStore(root=tmp_path)
    store.put_if_absent(PurePosixPath("x.json"), b"a", content_type="application/json")
    with pytest.raises(FileExistsError):
        store.put_if_absent(PurePosixPath("x.json"), b"b", content_type="application/json")


def test_verify_release_passes_on_a_clean_build(tmp_path: Path):
    con = duckdb.connect()
    store = LocalDirStore(root=tmp_path)
    manifest = build_release(_candidate(), _tables(con), store, contract=fx.DATASET_CONTRACT)

    report = verify_release(store, "demo-catalog", "R1", manifest=manifest)
    assert report.passed is True
    assert report.run_id == RUN_ID

    finalize_release(store, manifest, report)
    reloaded_report = verify_release(store, "demo-catalog", "R1")  # cold reload from store
    assert reloaded_report.passed is True


def test_verify_release_fails_on_corrupted_file(tmp_path: Path):
    con = duckdb.connect()
    store = LocalDirStore(root=tmp_path)
    _built_and_finalized(con, store)

    events_path = _events_parquet_path(tmp_path)
    data = bytearray(events_path.read_bytes())
    data[-1] ^= 0xFF  # flip the last byte -- corrupts content without changing size
    events_path.write_bytes(bytes(data))

    report = verify_release(store, "demo-catalog", "R1")
    assert report.passed is False
    failing = [c.name for c in report.checks if not c.passed]
    assert any("demo.events" in name and "checksum" in name for name in failing)


def test_verify_release_fails_on_rewritten_schema_json(tmp_path: Path):
    con = duckdb.connect()
    store = LocalDirStore(root=tmp_path)
    _built_and_finalized(con, store)

    _schema_path(tmp_path, "demo.events").write_text('{"tampered": true}')

    report = verify_release(store, "demo-catalog", "R1")
    assert report.passed is False
    failing = [c.name for c in report.checks if not c.passed]
    assert any(name == "demo.events.schema_digest_matches" for name in failing)


def test_verify_release_fails_on_deleted_schema_json(tmp_path: Path):
    con = duckdb.connect()
    store = LocalDirStore(root=tmp_path)
    _built_and_finalized(con, store)

    _schema_path(tmp_path, "demo.events").unlink()

    report = verify_release(store, "demo-catalog", "R1")
    assert report.passed is False
    failing = [c.name for c in report.checks if not c.passed]
    assert any(name == "demo.events.schema_readable" for name in failing)


def test_verify_release_fails_on_deleted_provenance(tmp_path: Path):
    con = duckdb.connect()
    store = LocalDirStore(root=tmp_path)
    _built_and_finalized(con, store)

    (tmp_path / "demo-catalog" / "R1" / "provenance.json").unlink()

    report = verify_release(store, "demo-catalog", "R1")
    assert report.passed is False
    failing = [c.name for c in report.checks if not c.passed]
    assert "provenance_readable" in failing


def test_verify_release_fails_on_tampered_row_count(tmp_path: Path):
    con = duckdb.connect()
    store = LocalDirStore(root=tmp_path)
    manifest = build_release(_candidate(), _tables(con), store, contract=fx.DATASET_CONTRACT)
    tampered = dataclasses.replace(
        manifest,
        tables=(dataclasses.replace(manifest.tables[0], row_count=999_999), *manifest.tables[1:]),
    )

    report = verify_release(store, "demo-catalog", "R1", manifest=tampered)
    assert report.passed is False
    failing = [c.name for c in report.checks if not c.passed]
    assert any("row_count_matches" in name for name in failing)


def test_verify_release_fails_on_empty_manifest_tables(tmp_path: Path):
    con = duckdb.connect()
    store = LocalDirStore(root=tmp_path)
    manifest = build_release(_candidate(), _tables(con), store, contract=fx.DATASET_CONTRACT)
    tampered = dataclasses.replace(manifest, tables=())

    report = verify_release(store, "demo-catalog", "R1", manifest=tampered)
    assert report.passed is False
    failing = [c.name for c in report.checks if not c.passed]
    assert "tables_nonempty" in failing


def test_verify_release_fails_when_a_different_releases_manifest_is_served_under_this_prefix(
    tmp_path: Path,
):
    """B2(d): a manifest naming a different release (e.g. R2's, mistakenly served under
    R1's prefix) must fail verification against R1, not silently pass as R1."""
    con = duckdb.connect()
    store = LocalDirStore(root=tmp_path)
    build_release(_candidate("R1"), _tables(con), store, contract=fx.DATASET_CONTRACT)
    r2_manifest = build_release(_candidate("R2"), _tables(con), store, contract=fx.DATASET_CONTRACT)

    report = verify_release(store, "demo-catalog", "R1", manifest=r2_manifest)
    assert report.passed is False
    failing = [c.name for c in report.checks if not c.passed]
    assert "manifest_identity_matches_request" in failing


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
    report = verify_release(store, "demo-catalog", "R1", manifest=manifest)
    assert report.passed is True
    published = finalize_release(store, manifest, report)
    assert published.status == ArtifactStatus.PUBLISHED

    lake_settings = Settings(storage_base_uri=f"file://{tmp_path / 'lake'}")
    con = lake_connect(lake_settings)
    try:
        receipt = record_release(con, published, report)
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


def test_finalize_release_raises_and_writes_nothing_on_a_failed_report(tmp_path: Path):
    """design §11.5 #8: a required adapter failure prevents latest.json/registry
    promotion -- here, ``finalize_release`` must not write ``manifest.json`` at all."""
    con = duckdb.connect()
    store = LocalDirStore(root=tmp_path)
    manifest = build_release(_candidate(), _tables(con), store, contract=fx.DATASET_CONTRACT)
    failing_report = verify_release(
        store, "demo-catalog", "R1", manifest=dataclasses.replace(manifest, tables=())
    )
    assert failing_report.passed is False

    with pytest.raises(ValueError, match="acceptance failed"):
        finalize_release(store, manifest, failing_report)
    assert not (tmp_path / "demo-catalog" / "R1" / "manifest.json").exists()
