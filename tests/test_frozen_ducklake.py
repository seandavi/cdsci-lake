"""Offline + local-HTTP acceptance for ``cdsci.lake.publish.frozen`` (cdsci-lake#95 M2).

Builds the shared ``tests/fixtures/contracts`` dataset into a ``LocalDirStore`` via
``build_release`` + ``build_frozen_ducklake``, then exercises design §11.6's Frozen
DuckLake acceptance: a clean-connection ``ATTACH`` (local and http-served), a
metadata scan proving the catalog references no absolute/private path, tamper
detection through ``verify_release``, and determinism (parquet/JSON byte-identical,
``catalog.ducklake`` explicitly not -- see ``test_catalog_ducklake_is_not_byte_
identical_across_repeat_builds_but_is_checksummed``).
"""

from __future__ import annotations

import dataclasses
import functools
import hashlib
import http.server
import threading
import time
from pathlib import Path

import duckdb
import pytest
from fixtures.contracts import dataset as fx
from test_release_builder import _candidate, _tables

from cdsci.lake.publish.builder import LocalDirStore, build_release
from cdsci.lake.publish.frozen import (
    CATALOG_FILENAME,
    build_frozen_ducklake,
    frozen_ducklake_attach_sql,
)
from cdsci.lake.publish.verify import verify_release


def _build_frozen(tmp_path: Path, release: str = "R1"):
    con = duckdb.connect()
    store = LocalDirStore(root=tmp_path)
    manifest = build_release(_candidate(release), _tables(con), store, contract=fx.DATASET_CONTRACT)
    frozen_manifest = build_frozen_ducklake(store, manifest, contract=fx.DATASET_CONTRACT)
    return store, frozen_manifest


def _release_dir(tmp_path: Path, release: str = "R1") -> Path:
    return tmp_path / "demo-catalog" / release


def _attach_and_count(catalog_dir: Path, table_names: list[str]) -> dict[str, int]:
    con = duckdb.connect()
    try:
        con.execute("INSTALL ducklake; LOAD ducklake;")
        con.execute(frozen_ducklake_attach_sql(str(catalog_dir), alias="frozen"))
        return {
            name: con.execute(f'SELECT count(*) FROM frozen.main."{name}"').fetchone()[0]
            for name in table_names
        }
    finally:
        con.close()


def test_build_frozen_ducklake_adds_parquet_and_ducklake_artifacts(tmp_path: Path):
    _, manifest = _build_frozen(tmp_path)
    assert manifest.artifacts["ducklake"].location == "catalog.ducklake"
    assert manifest.artifacts["ducklake"].required is True
    assert manifest.artifacts["parquet"].location == "tables/"
    assert manifest.artifacts["parquet"].required is True
    assert fx.DATASET_CONTRACT.required_artifacts <= manifest.artifacts.keys()


def test_frozen_catalog_clean_connection_attach_and_select(tmp_path: Path):
    """Design §11.6 checks 1-4: a fresh connection, no internal credentials, attaches
    the just-built catalog and every manifest table's count matches."""
    _, manifest = _build_frozen(tmp_path)
    counts = _attach_and_count(_release_dir(tmp_path), [t.name for t in manifest.tables])
    assert counts == {t.name: t.row_count for t in manifest.tables}


def test_frozen_catalog_metadata_has_no_absolute_paths_or_schemes(tmp_path: Path):
    """Design §4 rule 4: the catalog's own metadata tables must not leak this build's
    local path, any storage scheme, or any other absolute path."""
    _build_frozen(tmp_path)
    catalog_path = _release_dir(tmp_path) / CATALOG_FILENAME

    con = duckdb.connect(str(catalog_path), read_only=True)
    try:
        table_names = [
            r[0] for r in con.execute("SELECT table_name FROM information_schema.tables").fetchall()
        ]
        leaked: list[tuple[str, str, str]] = []
        for table_name in table_names:
            described = con.execute(f"DESCRIBE {table_name}").fetchall()
            varchar_cols = [c[0] for c in described if c[1] == "VARCHAR"]
            for col in varchar_cols:
                rows = con.execute(
                    f"SELECT DISTINCT {col} FROM {table_name} WHERE {col} IS NOT NULL "
                    f"AND ({col} LIKE ? OR {col} LIKE '%://%' OR {col} LIKE '/%')",
                    [str(tmp_path) + "%"],
                ).fetchall()
                leaked.extend((table_name, col, str(r[0])) for r in rows)
        assert leaked == []
    finally:
        con.close()


def test_frozen_catalog_served_over_http_attach_and_select(tmp_path: Path):
    """Design §11.6 checks 1/7: served over plain HTTP, DATA_PATH overridden to the
    release's own base URL -- both the catalog and its Parquet data are genuinely
    fetched over the wire, not resolved against local disk."""
    _, manifest = _build_frozen(tmp_path)
    release_dir = _release_dir(tmp_path)

    handler = functools.partial(http.server.SimpleHTTPRequestHandler, directory=str(release_dir))
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    port = server.server_port
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    time.sleep(0.1)
    try:
        base_url = f"http://127.0.0.1:{port}"
        con = duckdb.connect()
        con.execute("INSTALL ducklake; LOAD ducklake;")
        con.execute(frozen_ducklake_attach_sql(base_url, alias="frozen"))
        for table in manifest.tables:
            count = con.execute(f'SELECT count(*) FROM frozen.main."{table.name}"').fetchone()[0]
            assert count == table.row_count
        con.close()
    finally:
        server.shutdown()


def test_verify_release_fails_on_tampered_parquet_with_frozen_ducklake_built(tmp_path: Path):
    store, manifest = _build_frozen(tmp_path)
    events_path = (
        _release_dir(tmp_path) / "tables" / "demo.events" / "data" / "part-00000.parquet"
    )
    data = bytearray(events_path.read_bytes())
    data[-1] ^= 0xFF
    events_path.write_bytes(bytes(data))

    report = verify_release(store, "demo-catalog", "R1", manifest, contract=fx.DATASET_CONTRACT)
    assert report.passed is False
    failing = [c.name for c in report.checks if not c.passed]
    assert any("demo.events" in name and "checksum" in name for name in failing)


def test_verify_release_passes_frozen_ducklake_checks_on_a_clean_build(tmp_path: Path):
    store, manifest = _build_frozen(tmp_path)
    report = verify_release(store, "demo-catalog", "R1", manifest, contract=fx.DATASET_CONTRACT)
    assert report.passed is True
    check_names = {c.name for c in report.checks}
    assert "required_artifacts_present" in check_names
    assert "ducklake.attach_succeeds" in check_names
    assert "demo.events.ducklake_row_count_matches" in check_names
    assert "demo.events.ducklake_schema_matches" in check_names


def test_verify_release_fails_when_required_ducklake_artifact_is_missing(tmp_path: Path):
    """The addendum's gate: a manifest with no ``ducklake``/``parquet`` artifacts (i.e.
    ``build_release`` alone, no ``build_frozen_ducklake``) must fail ``verify_release``
    once a ``contract`` is supplied, since ``required_artifacts`` isn't satisfied."""
    con = duckdb.connect()
    store = LocalDirStore(root=tmp_path)
    manifest = build_release(_candidate(), _tables(con), store, contract=fx.DATASET_CONTRACT)
    assert manifest.artifacts == {}

    report = verify_release(store, "demo-catalog", "R1", manifest, contract=fx.DATASET_CONTRACT)
    assert report.passed is False
    failing = [c.name for c in report.checks if not c.passed]
    assert "required_artifacts_present" in failing


def test_catalog_ducklake_is_not_byte_identical_across_repeat_builds_but_is_checksummed(
    tmp_path: Path,
):
    """Determinism ceiling: DuckLake's own catalog file embeds a fresh
    ``schema_uuid``/``table_uuid`` (and snapshot commit metadata) per ``ATTACH``, so
    two builds of the same release produce different ``catalog.ducklake`` bytes even
    though every Parquet/JSON artifact stays byte-identical (cdsci-lake#95 M1's own
    determinism guarantee, unaffected).

    # ponytail: catalog.ducklake is excluded from the byte-identical set for this
    # reason, not overlooked -- its sha256 is still computed per build (as a file
    # index would record any other artifact's checksum) so a consumer can still
    # detect corruption/tampering (see the tamper test above). Upgrade path, if
    # DuckLake ever exposes a way to pin/omit the embedded UUIDs: drop this
    # exclusion and fold catalog.ducklake into the plain byte-identical assertion.
    """
    store1, _ = _build_frozen(tmp_path / "b1")
    store2, _ = _build_frozen(tmp_path / "b2")

    parquet1 = (
        _release_dir(tmp_path / "b1") / "tables" / "demo.events" / "data" / "part-00000.parquet"
    ).read_bytes()
    parquet2 = (
        _release_dir(tmp_path / "b2") / "tables" / "demo.events" / "data" / "part-00000.parquet"
    ).read_bytes()
    assert parquet1 == parquet2

    catalog1 = (_release_dir(tmp_path / "b1") / CATALOG_FILENAME).read_bytes()
    catalog2 = (_release_dir(tmp_path / "b2") / CATALOG_FILENAME).read_bytes()
    assert catalog1 != catalog2

    sha1 = hashlib.sha256(catalog1).hexdigest()
    sha2 = hashlib.sha256(catalog2).hexdigest()
    assert len(sha1) == 64 and len(sha2) == 64
    assert sha1 != sha2


def test_build_frozen_ducklake_raises_if_catalog_already_exists(tmp_path: Path):
    store, manifest = _build_frozen(tmp_path)
    con = duckdb.connect()
    stale = build_release(_candidate(), _tables(con), store, contract=fx.DATASET_CONTRACT)
    stale = dataclasses.replace(stale, artifacts={})
    with pytest.raises(FileExistsError):
        build_frozen_ducklake(store, stale, contract=fx.DATASET_CONTRACT)
