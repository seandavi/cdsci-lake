"""``cdsci.lake.publish.builder`` — format-neutral release builder (design §6.5-§6.7, §11.5).

:func:`build_release` writes one release's Parquet + schema/file-index/provenance/lineage
JSON deterministically to an :class:`ObjectStore`, using only DuckDB's own
``write_parquet`` (``pyarrow`` stays dev-only, per AGENTS.md), and returns the release's
:class:`~cdsci.lake.publish.release.ReleaseManifest` **without writing it**. Layout is the
design §4 public tree, minus the ``datasets/``/``releases/`` wrapping segments M1 doesn't
need yet::

    <dataset_id>/<release_id>/manifest.json, provenance.json, lineage.json,
        tables/<name>/{schema.json, files.json, data/part-00000.parquet}

``verify_release`` (:mod:`cdsci.lake.publish.verify`) is the cold-path counterpart --
given the in-memory manifest :func:`build_release` returned (or reloading one from
``store`` when called with no manifest) it re-runs the public-path/asset-ref allowlists
and re-checks every table's schema digest, row count, and per-file size/checksum. It
makes no ``ops`` call and needs no credentials -- it is the seed of DuckDock's ``verify``,
split into its own module for that eventual extraction.

:func:`finalize_release` is the only function here that writes ``manifest.json``, and
only once ``report.passed`` -- so a release that fails acceptance leaves no
``manifest.json`` behind for a registry/``latest.json`` promotion to pick up (design
§11.5 #8).

:func:`record_release` is the one function here that touches ``ops``: writes the
release's :class:`~cdsci.lake.publish.release.PublicationReceipt` and registers the
release + its lineage edges (design §13 M1's "record receipts in lake_ops").
"""

from __future__ import annotations

import dataclasses
import functools
import hashlib
import json
import os
import shutil
import tempfile
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import BinaryIO, Protocol

import duckdb

from ..contracts import DatasetContract, TableContract, _split_canonical
from ..contracts_render import render_dataset_markdown, render_table_markdown
from ..log import event
from .release import (
    AcceptanceReport,
    ArtifactEntry,
    ArtifactStatus,
    FileEntry,
    ManifestTable,
    PublicationReceipt,
    ReleaseCandidate,
    ReleaseManifest,
    TableFileIndex,
    check_required_artifacts,
)

_PARQUET_CONTENT_TYPE = "application/vnd.apache.parquet"
_JSON_CONTENT_TYPE = "application/json"
_MARKDOWN_CONTENT_TYPE = "text/markdown"
# DuckDB's own default -- pinned explicitly rather than left implicit, so a future
# DuckDB version changing its default can't silently change release byte content.
_ROW_GROUP_SIZE = 122_880

# Canonical arrow-type string (contracts.ColumnContract.arrow_type) -> the DuckDB
# DESCRIBE type name it must round-trip through. Parallels contracts._parse_arrow_type's
# pyarrow mapping, but targets DuckDB's own type vocabulary so schema verification
# never needs pyarrow.
_DUCKDB_TYPE_BY_CANONICAL = {
    "string": "VARCHAR", "bool": "BOOLEAN", "int8": "TINYINT", "int16": "SMALLINT",
    "int32": "INTEGER", "int64": "BIGINT", "uint8": "UTINYINT", "uint16": "USMALLINT",
    "uint32": "UINTEGER", "uint64": "UBIGINT", "float": "FLOAT", "double": "DOUBLE",
    "date32": "DATE", "binary": "BLOB",
}


def _expected_duckdb_type(canonical: str) -> str:
    kind, args = _split_canonical(canonical)
    if kind == "list":
        return f"{_expected_duckdb_type(args[0])}[]"
    if kind == "struct":
        return "STRUCT(" + ", ".join(f'"{n}" {_expected_duckdb_type(t)}' for n, t in args) + ")"
    text = args[0]
    if text in _DUCKDB_TYPE_BY_CANONICAL:
        return _DUCKDB_TYPE_BY_CANONICAL[text]
    return "TIMESTAMPTZ" if "tz=" in text else "TIMESTAMP"


@functools.cache
def _normalize_duckdb_type(type_sql: str) -> str:
    """DuckDB's own rendering of ``type_sql`` -- so comparisons never depend on how a
    type was spelled (struct field quoting, ``LIST`` vs ``[]``)."""
    return str(duckdb.sql(f"SELECT CAST(NULL AS {type_sql}) AS x").types[0])


def _check_parquet_matches_contract(contract: TableContract, parquet_path: Path) -> None:
    """Read the just-written file's schema via DuckDB and compare column names/order/types
    to ``contract``."""
    rel = duckdb.read_parquet(str(parquet_path))
    actual = dict(zip(rel.columns, rel.types, strict=True))
    expected_names = [c.name for c in contract.columns]
    if list(actual) != expected_names:
        raise ValueError(
            f"{contract.name}: parquet columns {list(actual)} != contract {expected_names}"
        )
    for c in contract.columns:
        expected_type = _normalize_duckdb_type(_expected_duckdb_type(c.arrow_type))
        if str(actual[c.name]) != expected_type:
            raise ValueError(
                f"{contract.name}.{c.name}: parquet type {str(actual[c.name])!r} != "
                f"contract-expected {expected_type!r}"
            )


def _file_sha256(path: Path) -> str:
    with path.open("rb") as f:
        return hashlib.file_digest(f, "sha256").hexdigest()


class ObjectStore(Protocol):
    """Design §6.7. Small objects go in/out as ``bytes``; data files stream by path
    (``put_file_if_absent``) or handle (``open``) so a release never holds a table in memory."""

    def put_if_absent(self, path: PurePosixPath, body: bytes, *, content_type: str) -> None: ...

    def put_file_if_absent(self, path: PurePosixPath, src: Path, *, content_type: str) -> None: ...

    def get(self, path: PurePosixPath) -> bytes: ...

    def open(self, path: PurePosixPath) -> BinaryIO: ...

    def replace(self, path: PurePosixPath, body: bytes, *, content_type: str) -> None: ...

    def delete_tree(self, prefix: PurePosixPath) -> None: ...


@dataclass(frozen=True)
class LocalDirStore:
    """The one :class:`ObjectStore` adapter -- a local directory tree (no S3/R2 adapter
    yet). Enough for the local-HTTP-server acceptance path (§11.7)."""

    root: Path

    def _abs(self, path: PurePosixPath) -> Path:
        """Resolve ``path`` under ``root``; reject anything that escapes it (absolute
        paths, ``..`` segments, symlinks out of the tree) or names ``root`` itself."""
        dest = (self.root / Path(*path.parts)).resolve()
        root = self.root.resolve()
        if dest == root or not dest.is_relative_to(root):
            raise ValueError(f"store path escapes the store root: {str(path)!r}")
        return dest

    def put_if_absent(self, path: PurePosixPath, body: bytes, *, content_type: str) -> None:
        dest = self._abs(path)
        if dest.exists() and dest.read_bytes() != body:
            raise FileExistsError(f"release object already exists with different content: {path}")
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(body)

    def put_file_if_absent(self, path: PurePosixPath, src: Path, *, content_type: str) -> None:
        dest = self._abs(path)
        if dest.exists():
            if _file_sha256(dest) != _file_sha256(src):
                raise FileExistsError(
                    f"release object already exists with different content: {path}"
                )
            return
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(src, dest)

    def get(self, path: PurePosixPath) -> bytes:
        dest = self._abs(path)
        if not dest.is_file():
            raise FileNotFoundError(str(path))
        return dest.read_bytes()

    def open(self, path: PurePosixPath) -> BinaryIO:
        dest = self._abs(path)
        if not dest.is_file():
            raise FileNotFoundError(str(path))
        return dest.open("rb")

    def replace(self, path: PurePosixPath, body: bytes, *, content_type: str) -> None:
        """Atomically overwrite ``path`` -- used only for dataset pointer files."""
        dest = self._abs(path)
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp = dest.with_name(dest.name + ".tmp")
        tmp.write_bytes(body)
        os.replace(tmp, dest)

    def delete_tree(self, prefix: PurePosixPath) -> None:
        dest = self._abs(prefix)
        if dest.exists():
            shutil.rmtree(dest)


def build_release(
    candidate: ReleaseCandidate,
    tables: Mapping[str, duckdb.DuckDBPyRelation],
    out: ObjectStore,
    *,
    contract: DatasetContract,
) -> ReleaseManifest:
    """Write one release's Parquet + schema/file-index/provenance/lineage JSON to
    ``out`` and return the release's :class:`ReleaseManifest` -- **not yet written**;
    call :func:`finalize_release` once :func:`verify_release` has passed it.

    ``tables`` maps each name in ``candidate.tables`` to a DuckDB relation holding its
    rows (order-independent -- sorted here per ``contract``). ``contract`` supplies the
    per-table ``TableContract`` (columns, primary key, sort order, temporal model) that
    ``ReleaseCandidate`` itself doesn't carry.

    Deterministic: sorted by ``sort_by`` plus any remaining ``primary_key`` columns (so
    a ``sort_by`` that doesn't cover the full key still produces one row order, not an
    arbitrary one among ties), written in one fixed-size row group via DuckDB's own
    Parquet writer.

    # ponytail: determinism is scoped to one DuckDB version -- the Parquet footer's
    # ``created_by`` (and possibly row-group/encoding choices) can change across a
    # DuckDB upgrade. Rebuilding an already-published release after a DuckDB upgrade
    # must cut a new release_id, not silently overwrite the old bytes.
    """
    prefix = PurePosixPath(candidate.dataset) / candidate.release
    manifest_tables: list[ManifestTable] = []

    with tempfile.TemporaryDirectory() as tmp:
        for name in candidate.tables:
            table_contract = contract.tables[name]
            if table_contract.partition_by:
                # ponytail: single file per table; honour partition_by when a table needs it
                raise ValueError(
                    f"{table_contract.name}: partition_by is not supported by this release "
                    f"builder yet: {table_contract.partition_by}"
                )
            sort_cols = (
                *table_contract.sort_by,
                *(k for k in table_contract.primary_key if k not in table_contract.sort_by),
            )
            ordered = tables[name].order(", ".join(sort_cols))

            tmp_path = Path(tmp) / f"{name}.parquet"
            ordered.write_parquet(
                str(tmp_path), row_group_size=_ROW_GROUP_SIZE, compression="zstd"
            )
            _check_parquet_matches_contract(table_contract, tmp_path)
            row_count = duckdb.execute(
                "SELECT count(*) FROM read_parquet(?)", [str(tmp_path)]
            ).fetchone()[0]
            size = tmp_path.stat().st_size
            sha256 = _file_sha256(tmp_path)

            table_dir = prefix / "tables" / name
            out.put_file_if_absent(
                table_dir / "data" / "part-00000.parquet",
                tmp_path,
                content_type=_PARQUET_CONTENT_TYPE,
            )

            file_index = TableFileIndex(
                table=name,
                release=candidate.release,
                files=(
                    FileEntry(
                        uri="data/part-00000.parquet",
                        size=size,
                        sha256=sha256,
                        content_type=_PARQUET_CONTENT_TYPE,
                        rows=row_count,
                    ),
                ),
            )
            out.put_if_absent(
                table_dir / "files.json",
                file_index.to_json().encode(),
                content_type=_JSON_CONTENT_TYPE,
            )

            schema_dict = table_contract.to_schema_dict()
            schema_bytes = json.dumps(schema_dict, indent=2).encode()
            schema_digest = "sha256:" + hashlib.sha256(schema_bytes).hexdigest()
            out.put_if_absent(
                table_dir / "schema.json", schema_bytes, content_type=_JSON_CONTENT_TYPE
            )
            # Docs, not data -- deliberately not named in files.json/the manifest, so
            # verify_release's file-index walk never expects or checks them.
            out.put_if_absent(
                table_dir / "README.md",
                render_table_markdown(table_contract).encode(),
                content_type=_MARKDOWN_CONTENT_TYPE,
            )

            manifest_tables.append(
                ManifestTable.from_contract(
                    table_contract,
                    schema_path=str(PurePosixPath("tables") / name / "schema.json"),
                    files_path=str(PurePosixPath("tables") / name / "files.json"),
                    schema_digest=schema_digest,
                    row_count=row_count,
                )
            )

    manifest = ReleaseManifest(
        dataset=candidate.dataset,
        release=candidate.release,
        status=ArtifactStatus.STAGED,
        run_id=candidate.run_id,
        tables=tuple(manifest_tables),
        source_asset_versions=candidate.source_asset_versions,
        artifacts={
            "parquet": ArtifactEntry(
                status=ArtifactStatus.STAGED, required=True, location="tables/"
            )
        },
    )
    out.put_if_absent(
        prefix / "provenance.json", _provenance_bytes(manifest), content_type=_JSON_CONTENT_TYPE
    )
    out.put_if_absent(prefix / "lineage.json", b'{"edges": []}', content_type=_JSON_CONTENT_TYPE)
    # Docs, not data -- deliberately not named in files.json/the manifest (see the
    # per-table README.md above).
    out.put_if_absent(
        prefix / "README.md", render_dataset_markdown(contract).encode(),
        content_type=_MARKDOWN_CONTENT_TYPE,
    )
    event(
        "build_release_completed",
        run_id=manifest.run_id,
        asset=f"release.{manifest.dataset}.{manifest.release}",
        release=manifest.release,
        rows=sum(t.row_count for t in manifest_tables if t.row_count is not None),
        status=manifest.status.value,
    )
    return manifest


def _provenance_bytes(manifest: ReleaseManifest) -> bytes:
    # ponytail: this exists only so manifest.provenance's link resolves; a real
    # provenance document (source retrieval timestamps/checksums, design §6.5's
    # SourceArtifact) is M4 territory.
    return json.dumps(
        {"source_asset_versions": [v.to_dict() for v in manifest.source_asset_versions]}, indent=2
    ).encode()


def finalize_release(
    store: ObjectStore,
    manifest: ReleaseManifest,
    report: AcceptanceReport,
    contract: DatasetContract,
) -> ReleaseManifest:
    """Write ``manifest.json`` with status ``published`` -- but only once ``report``
    has passed every required check (design §11.5 #8: "a required adapter failure
    prevents latest.json and registry promotion") and ``manifest.artifacts`` satisfies
    ``contract.required_artifacts`` (:func:`~cdsci.lake.publish.release.check_required_artifacts`).
    ``contract`` is required here, not optional as on ``verify_release`` -- a caller
    that skipped ``verify_release``'s own optional ``required_artifacts_present``
    check (or never passed it ``contract``) must not be able to publish a release
    missing a required artifact by omission. Raises and writes nothing on either
    failure. Also promotes every ``STAGED`` artifact (e.g. the ``ducklake`` catalog
    :func:`~cdsci.lake.publish.frozen.build_frozen_ducklake` stamped staged, not yet
    acceptance-checked) to ``VERIFIED``.
    """
    check_required_artifacts(manifest, contract)
    if not report.passed:
        failed = [c.name for c in report.checks if c.required and not c.passed]
        raise ValueError(
            f"{manifest.dataset} {manifest.release}: acceptance failed, refusing to publish "
            f"manifest.json (failed required checks: {failed})"
        )
    verified_artifacts = {
        name: dataclasses.replace(entry, status=ArtifactStatus.VERIFIED)
        if entry.status == ArtifactStatus.STAGED
        else entry
        for name, entry in manifest.artifacts.items()
    }
    published = dataclasses.replace(
        manifest,
        status=ArtifactStatus.PUBLISHED,
        published_at=datetime.now(UTC).isoformat(),
        artifacts=verified_artifacts,
    )
    prefix = PurePosixPath(published.dataset) / published.release
    store.put_if_absent(
        prefix / "manifest.json", published.to_json().encode(), content_type=_JSON_CONTENT_TYPE
    )
    event(
        "release_published",
        run_id=published.run_id,
        asset=f"release.{published.dataset}.{published.release}",
        release=published.release,
        status=published.status.value,
    )
    return published


def record_release(
    con: duckdb.DuckDBPyConnection, manifest: ReleaseManifest, report: AcceptanceReport
) -> PublicationReceipt:
    """Record ``manifest``'s :class:`PublicationReceipt` in ``lake_ops``; register the
    release as an asset ref ``release.<dataset>.<release>`` (deliberately not the
    internal lake grammar -- ADR-0014 Amendment 2026-09-22 -- since ``dataset``/
    ``release`` are producer-chosen strings, e.g. ``demo-catalog``/``R1``); write
    ``publishes`` lineage edges from each source lake table.
    """
    from .. import ops  # local: builder is a downstream consumer of ops, not the reverse

    status = ArtifactStatus.PUBLISHED if report.passed else ArtifactStatus.FAILED
    receipt = PublicationReceipt(
        dataset=manifest.dataset,
        release=manifest.release,
        format="parquet",
        destination=f"{manifest.dataset}/{manifest.release}",
        schema_digest=hashlib.sha256(
            "".join(t.schema_digest for t in manifest.tables).encode()
        ).hexdigest(),
        run_id=manifest.run_id,
        status=status,
        row_counts={t.name: t.row_count for t in manifest.tables if t.row_count is not None},
        checksums={t.name: t.schema_digest for t in manifest.tables},
        details={"checks": [c.to_dict() for c in report.checks]},
    )
    ops.record_publication_receipt(con, receipt)
    event(
        "release_receipt_recorded",
        run_id=manifest.run_id,
        asset=f"release.{manifest.dataset}.{manifest.release}",
        release=manifest.release,
        status=receipt.status.value,
    )

    release_ref = f"release.{manifest.dataset}.{manifest.release}"
    ops.register_asset(
        con, ref=release_ref, writer="cdsci", asset_type="release",
        name=f"{manifest.dataset} {manifest.release}", current_version=manifest.run_id,
    )
    for source in manifest.source_asset_versions:
        ops.record_lineage(con, src_ref=source.ref, dst_ref=release_ref, edge_type="publishes")
    return receipt
