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
from test_release_builder import _candidate, _RangeHandler, _tables

from cdsci.lake.publish import frozen
from cdsci.lake.publish.builder import LocalDirStore, ObjectStore, build_release, finalize_release
from cdsci.lake.publish.frozen import (
    CATALOG_FILENAME,
    build_frozen_ducklake,
    frozen_ducklake_attach_sql,
)
from cdsci.lake.publish.release import ArtifactStatus
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

    handler = functools.partial(_RangeHandler, directory=str(release_dir))
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
    """The addendum's gate: a manifest with only the ``parquet`` artifact ``build_release``
    itself stamps (no ``build_frozen_ducklake``) must fail ``verify_release`` once a
    ``contract`` is supplied, since ``required_artifacts`` (``parquet`` + ``ducklake``)
    isn't satisfied."""
    con = duckdb.connect()
    store = LocalDirStore(root=tmp_path)
    manifest = build_release(_candidate(), _tables(con), store, contract=fx.DATASET_CONTRACT)
    assert manifest.artifacts.keys() == {"parquet"}

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
    store1, manifest1 = _build_frozen(tmp_path / "b1")
    store2, manifest2 = _build_frozen(tmp_path / "b2")

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

    # The claim in this test's own docstring, actually backed by the manifest now --
    # not just computed independently here (finding #11: ArtifactEntry.sha256/size).
    assert manifest1.artifacts["ducklake"].sha256 == sha1
    assert manifest1.artifacts["ducklake"].size == len(catalog1)
    assert manifest2.artifacts["ducklake"].sha256 == sha2
    assert sha1 != sha2


def test_build_frozen_ducklake_raises_if_catalog_already_exists(tmp_path: Path):
    store, manifest = _build_frozen(tmp_path)
    con = duckdb.connect()
    stale = build_release(_candidate(), _tables(con), store, contract=fx.DATASET_CONTRACT)
    stale = dataclasses.replace(stale, artifacts={})
    with pytest.raises(FileExistsError):
        build_frozen_ducklake(store, stale, contract=fx.DATASET_CONTRACT)


# --- Review findings (cdsci-lake#95 M2 review) -----------------------------------


def test_build_frozen_ducklake_leaves_no_freed_page_trace_of_the_build_path(tmp_path: Path):
    """P0 #1: a plain ``UPDATE`` against the in-place catalog leaves this build's
    absolute temp path recoverable from the file's freed pages even after every
    ``path`` column reads relative -- ``build_frozen_ducklake`` must materialize the
    release catalog via ``COPY FROM DATABASE`` into a fresh file instead."""
    _, _ = _build_frozen(tmp_path)
    catalog = (_release_dir(tmp_path) / CATALOG_FILENAME).read_bytes()
    assert str(tmp_path).encode() not in catalog


def test_verify_release_ducklake_sample_query_fails_when_parquet_deleted(tmp_path: Path):
    """P0 #2: ``count(*)`` is answered from DuckLake's own catalog stats and doesn't
    open the Parquet file -- the bounded sample query must, and must fail when it's
    gone."""
    store, manifest = _build_frozen(tmp_path)
    events_path = (
        _release_dir(tmp_path) / "tables" / "demo.events" / "data" / "part-00000.parquet"
    )
    events_path.unlink()

    report = verify_release(store, "demo-catalog", "R1", manifest, contract=fx.DATASET_CONTRACT)
    assert report.passed is False
    failing = {c.name for c in report.checks if not c.passed}
    assert "demo.events.ducklake_sample_readable" in failing


def test_frozen_catalog_http_sample_query_fetches_parquet_bytes(tmp_path: Path):
    """P0 #2 companion: over HTTP, a bounded sample query genuinely fetches the
    Parquet bytes across the wire -- proven by the server's own access log recording
    a ``.parquet`` request, not just a request for ``catalog.ducklake``."""
    _, manifest = _build_frozen(tmp_path)
    release_dir = _release_dir(tmp_path)
    requested_paths: list[str] = []

    class _LoggingHandler(_RangeHandler):
        def log_message(self, format, *args):  # noqa: A002
            requested_paths.append(self.path)

    handler = functools.partial(_LoggingHandler, directory=str(release_dir))
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    port = server.server_port
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    time.sleep(0.1)
    try:
        con = duckdb.connect()
        con.execute("INSTALL ducklake; LOAD ducklake;")
        con.execute(frozen_ducklake_attach_sql(f"http://127.0.0.1:{port}", alias="frozen"))
        con.execute('SELECT * FROM frozen.main."demo.events" LIMIT 5').fetchall()
        con.close()
    finally:
        server.shutdown()

    assert any(p.split("?")[0].endswith(".parquet") for p in requested_paths)


def test_verify_release_reports_no_private_paths_and_single_snapshot_checks(tmp_path: Path):
    """P1 #3/#7: the private-path scan and the single-snapshot collapse are both
    ``verify_release``-visible required checks, not just build-time side effects."""
    store, manifest = _build_frozen(tmp_path)
    report = verify_release(store, "demo-catalog", "R1", manifest, contract=fx.DATASET_CONTRACT)
    by_name = {c.name: c for c in report.checks}
    assert by_name["ducklake.no_private_paths"].passed is True
    assert by_name["ducklake.no_private_paths"].required is True
    assert by_name["ducklake.single_snapshot"].passed is True
    assert by_name["ducklake.single_snapshot"].detail == "snapshot_count=1"


def test_relativize_catalog_raises_on_unexpected_metadata_version(tmp_path: Path):
    """P1 #4: fail closed -- refuse to relativize a catalog whose format version this
    module's raw metadata-table SQL hasn't been validated against."""
    catalog_path = tmp_path / CATALOG_FILENAME
    con = duckdb.connect()
    con.execute("INSTALL ducklake; LOAD ducklake;")
    con.execute(f"ATTACH {'ducklake:' + str(catalog_path)!r} AS cat (DATA_PATH '.')")
    con.execute("DETACH cat")
    con.close()

    con = duckdb.connect(str(catalog_path))
    con.execute("UPDATE ducklake_metadata SET value = '0.9' WHERE key = 'version'")
    con.close()

    with pytest.raises(ValueError, match="ducklake_metadata.version"):
        frozen._relativize_catalog(catalog_path, tmp_path)


def test_relativize_catalog_derives_relative_path_from_recorded_absolute_path(tmp_path: Path):
    """P1 #5: the relative URI is derived by stripping the release prefix from the
    absolute path DuckLake recorded, not reconstructed as ``part-00000.parquet`` --
    proven with two registered files for one table (``build_frozen_ducklake`` itself
    only ever writes/registers one; this registers a second manually)."""
    release_prefix = tmp_path / "demo-catalog" / "R1"
    data_dir = release_prefix / "tables" / "demo.events" / "data"
    data_dir.mkdir(parents=True)
    first = data_dir / "part-00000.parquet"
    second = data_dir / "extra-file.parquet"
    duckdb.execute(f"COPY (SELECT 1 AS a) TO {str(first)!r} (FORMAT PARQUET)")
    duckdb.execute(f"COPY (SELECT 2 AS a) TO {str(second)!r} (FORMAT PARQUET)")

    catalog_path = tmp_path / "tmp-build" / CATALOG_FILENAME
    catalog_path.parent.mkdir(parents=True)
    con = duckdb.connect()
    con.execute("INSTALL ducklake; LOAD ducklake;")
    con.execute(f"ATTACH {'ducklake:' + str(catalog_path)!r} AS cat (DATA_PATH '.')")
    con.execute('CREATE TABLE cat."demo.events" (a INTEGER)')
    con.execute(f"CALL ducklake_add_data_files('cat', 'demo.events', {str(first)!r})")
    con.execute(f"CALL ducklake_add_data_files('cat', 'demo.events', {str(second)!r})")
    con.execute("DETACH cat")
    con.close()

    frozen._relativize_catalog(catalog_path, release_prefix)

    con = duckdb.connect(str(catalog_path))
    paths = sorted(r[0] for r in con.execute("SELECT path FROM ducklake_data_file").fetchall())
    con.close()
    assert paths == [
        "tables/demo.events/data/extra-file.parquet",
        "tables/demo.events/data/part-00000.parquet",
    ]


def test_verify_release_attach_failure_returns_failed_report_not_public_path_error(
    tmp_path: Path,
):
    """P1 #6: a corrupt catalog's raw DuckDB exception text can carry this build's
    local path -- the attach-failure check must record only ``type(exc).__name__``,
    never that text, or constructing the ``AcceptanceCheck`` itself would raise
    ``PublicPathError`` instead of returning a failed report."""
    store, manifest = _build_frozen(tmp_path)
    catalog_path = _release_dir(tmp_path) / CATALOG_FILENAME
    catalog_path.write_bytes(b"not a valid duckdb database file")

    report = verify_release(store, "demo-catalog", "R1", manifest, contract=fx.DATASET_CONTRACT)
    assert report.passed is False
    by_name = {c.name: c for c in report.checks}
    assert by_name["ducklake.attach_succeeds"].passed is False
    assert str(tmp_path) not in by_name["ducklake.attach_succeeds"].detail
    assert by_name["ducklake.attach_succeeds"].detail  # type(exc).__name__, non-empty


def test_verify_release_checks_read_only_mutation_fails(tmp_path: Path):
    """P1 #8: mutation under the consumer's ``READ_ONLY`` attach must fail -- exercised
    against the release dir's own catalog (safe: a rejected mutation changes nothing).
    The non-read-only mutation path is deliberately not tested here since it would
    mutate this immutable release directory."""
    store, manifest = _build_frozen(tmp_path)
    report = verify_release(store, "demo-catalog", "R1", manifest, contract=fx.DATASET_CONTRACT)
    by_name = {c.name: c for c in report.checks}
    assert by_name["ducklake.read_only_enforced"].passed is True
    assert by_name["ducklake.read_only_enforced"].required is True


def test_build_frozen_ducklake_stages_and_finalize_release_verifies_artifacts(tmp_path: Path):
    """P1 #9: ``build_frozen_ducklake`` stamps ``STAGED``; only ``finalize_release``,
    once acceptance has passed, promotes to ``VERIFIED``."""
    store, manifest = _build_frozen(tmp_path)
    assert manifest.artifacts["ducklake"].status == ArtifactStatus.STAGED
    assert manifest.artifacts["parquet"].status == ArtifactStatus.STAGED

    report = verify_release(store, "demo-catalog", "R1", manifest, contract=fx.DATASET_CONTRACT)
    assert report.passed is True
    published = finalize_release(store, manifest, report, contract=fx.DATASET_CONTRACT)
    assert published.artifacts["ducklake"].status == ArtifactStatus.VERIFIED
    assert published.artifacts["parquet"].status == ArtifactStatus.VERIFIED


class _UnsupportedStore:
    """A minimal in-memory :class:`ObjectStore` -- not :class:`LocalDirStore` -- to
    prove P1 #10: Frozen DuckLake acceptance is never silently skipped for a store
    this module doesn't know how to ``ATTACH`` against."""

    def __init__(self) -> None:
        self._objects: dict[str, bytes] = {}

    def put_if_absent(self, path, body: bytes, *, content_type: str) -> None:
        self._objects[str(path)] = body

    def get(self, path) -> bytes:
        return self._objects[str(path)]


def test_verify_release_fails_closed_for_unsupported_store_with_ducklake_artifact(
    tmp_path: Path,
):
    """P1 #10: a manifest that declares a ``ducklake`` artifact but is verified
    against a store this module can't ``ATTACH`` against must fail closed, not skip
    the check silently."""
    local_store, manifest = _build_frozen(tmp_path)
    fake_store: ObjectStore = _UnsupportedStore()

    from pathlib import PurePosixPath

    prefix = PurePosixPath(manifest.dataset) / manifest.release
    for table in manifest.tables:
        fake_store.put_if_absent(
            prefix / table.schema_path,
            local_store.get(prefix / table.schema_path),
            content_type="application/json",
        )
        fake_store.put_if_absent(
            prefix / table.files_path,
            local_store.get(prefix / table.files_path),
            content_type="application/json",
        )
    fake_store.put_if_absent(
        prefix / manifest.provenance,
        local_store.get(prefix / manifest.provenance),
        content_type="application/json",
    )
    fake_store.put_if_absent(
        prefix / manifest.lineage,
        local_store.get(prefix / manifest.lineage),
        content_type="application/json",
    )

    report = verify_release(
        fake_store, "demo-catalog", "R1", manifest, contract=fx.DATASET_CONTRACT
    )
    by_name = {c.name: c for c in report.checks}
    assert "ducklake.acceptance_supported" in by_name
    assert by_name["ducklake.acceptance_supported"].passed is False
    assert by_name["ducklake.acceptance_supported"].required is True
    assert report.passed is False
