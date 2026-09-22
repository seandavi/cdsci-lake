"""``cdsci.lake.publish.builder`` — format-neutral release builder (design §6.5-§6.7, §11.5).

:func:`build_release` writes one release's Parquet + manifest/schema/file-index JSON
deterministically to an :class:`ObjectStore`, using only DuckDB's own ``write_parquet``
(``pyarrow`` stays dev-only, per AGENTS.md). Layout is the design §4 public tree, minus
the ``datasets/``/``releases/`` wrapping segments M1 doesn't need yet::

    <dataset_id>/<release_id>/manifest.json, provenance.json, lineage.json,
        tables/<name>/{schema.json, files.json, data/part-00000.parquet}

:func:`verify_release` is the cold-path counterpart -- reloads the manifest through
:class:`~cdsci.lake.publish.release.ReleaseManifest`/``TableFileIndex`` (whose
``__post_init__`` already runs the public-path/asset-ref allowlists) and re-checks
size + sha256 per file. No ``cdsci.lake.ops``/lake import -- it is the seed of
DuckDock's ``verify``, which must run with no private credentials.

:func:`record_release` is the one function here that touches ``ops``: writes the
release's :class:`~cdsci.lake.publish.release.PublicationReceipt` and registers the
release + its lineage edges (design §13 M1's "record receipts in lake_ops").
"""

from __future__ import annotations

import hashlib
import json
import tempfile
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Protocol

import duckdb

from ..contracts import DatasetContract, Materialization, TableContract, TemporalModel
from .release import (
    SPEC_VERSION,
    AcceptanceCheck,
    AcceptanceReport,
    ArtifactStatus,
    FileEntry,
    ManifestTable,
    PublicationReceipt,
    ReleaseCandidate,
    ReleaseManifest,
    TableFileIndex,
)

_PARQUET_CONTENT_TYPE = "application/vnd.apache.parquet"
_JSON_CONTENT_TYPE = "application/json"
# DuckDB's own default -- pinned explicitly rather than left implicit, so a future
# DuckDB version changing its default can't silently change release byte content.
_ROW_GROUP_SIZE = 122_880

# The single canonical materialization per temporal model -- the "cross-check deferred
# from #96" the M1 builder must enforce (a TableFileIndex.materialization that
# disagrees with its table's temporal_model fails verify_release). scd2_release ->
# release_snapshot per design §5.3's own annotation.gene example; scd2_bitemporal ->
# history, since collapsing it to one interval loses the bitemporal dimension.
# ponytail: one fixed mapping, not a per-producer choice; extend if scd2_release-as-
# full-history publication is ever needed.
_MATERIALIZATION_FOR_TEMPORAL_MODEL: dict[TemporalModel, Materialization] = {
    TemporalModel.APPEND_IMMUTABLE: Materialization.APPEND_IMMUTABLE,
    TemporalModel.UPSERT_LATEST_SNAPSHOT: Materialization.RELEASE_SNAPSHOT,
    TemporalModel.SCD2_RELEASE: Materialization.RELEASE_SNAPSHOT,
    TemporalModel.SCD2_BITEMPORAL: Materialization.HISTORY,
}

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
    if canonical in _DUCKDB_TYPE_BY_CANONICAL:
        return _DUCKDB_TYPE_BY_CANONICAL[canonical]
    if canonical.startswith("timestamp["):
        return "TIMESTAMPTZ" if "tz=" in canonical else "TIMESTAMP"
    raise ValueError(f"unsupported canonical arrow type string: {canonical!r}")


def _check_parquet_matches_contract(contract: TableContract, parquet_path: Path) -> None:
    """``DESCRIBE`` the just-written file and compare column names/order/types to ``contract``."""
    described = duckdb.execute(
        "DESCRIBE SELECT * FROM read_parquet(?)", [str(parquet_path)]
    ).fetchall()
    actual = {name: col_type for name, col_type, *_ in described}
    expected_names = [c.name for c in contract.columns]
    if list(actual) != expected_names:
        raise ValueError(
            f"{contract.name}: parquet columns {list(actual)} != contract {expected_names}"
        )
    for c in contract.columns:
        expected_type = _expected_duckdb_type(c.arrow_type)
        if actual[c.name] != expected_type:
            raise ValueError(
                f"{contract.name}.{c.name}: parquet type {actual[c.name]!r} != "
                f"contract-expected {expected_type!r}"
            )


@dataclass(frozen=True)
class ObjectMetadata:
    size: int
    content_type: str


class ObjectStore(Protocol):
    """Design §6.7, simplified to plain ``bytes`` in/out (not ``BinaryIO``) -- every M1
    object is small enough to hold in memory; add streaming when a table needs it."""

    def put_if_absent(self, path: PurePosixPath, body: bytes, *, content_type: str) -> None: ...

    def head(self, path: PurePosixPath) -> ObjectMetadata: ...

    def get(self, path: PurePosixPath) -> bytes: ...

    def copy_pointer(self, path: PurePosixPath, document: bytes) -> None: ...


_CONTENT_TYPE_BY_SUFFIX = {".parquet": _PARQUET_CONTENT_TYPE, ".json": _JSON_CONTENT_TYPE}


@dataclass(frozen=True)
class LocalDirStore:
    """The one M1 :class:`ObjectStore` adapter -- a local directory tree (no S3/R2
    adapter yet; that's M4). Enough for the local-HTTP-server acceptance path (§11.7)."""

    root: Path

    def _abs(self, path: PurePosixPath) -> Path:
        return self.root / Path(*path.parts)

    def put_if_absent(self, path: PurePosixPath, body: bytes, *, content_type: str) -> None:
        dest = self._abs(path)
        if dest.exists() and dest.read_bytes() != body:
            raise FileExistsError(f"release object already exists with different content: {path}")
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(body)

    def head(self, path: PurePosixPath) -> ObjectMetadata:
        dest = self._abs(path)
        if not dest.is_file():
            raise FileNotFoundError(str(path))
        content_type = _CONTENT_TYPE_BY_SUFFIX.get(dest.suffix, "application/octet-stream")
        return ObjectMetadata(size=dest.stat().st_size, content_type=content_type)

    def get(self, path: PurePosixPath) -> bytes:
        dest = self._abs(path)
        if not dest.is_file():
            raise FileNotFoundError(str(path))
        return dest.read_bytes()

    def copy_pointer(self, path: PurePosixPath, document: bytes) -> None:
        """Pointer paths (``latest.json``) are the one thing a release domain may replace."""
        dest = self._abs(path)
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(document)


def build_release(
    candidate: ReleaseCandidate,
    tables: Mapping[str, duckdb.DuckDBPyRelation],
    out: ObjectStore,
    *,
    contract: DatasetContract,
) -> ReleaseManifest:
    """Write one release's Parquet + schema/file-index/manifest JSON to ``out``.

    ``tables`` maps each name in ``candidate.tables`` to a DuckDB relation holding its
    rows (order-independent -- sorted here per ``contract``). ``contract`` supplies the
    per-table ``TableContract`` (columns, primary key, sort order, temporal model) that
    ``ReleaseCandidate`` itself doesn't carry.

    Deterministic: sorted by ``sort_by`` (falling back to ``primary_key``), written in
    one fixed-size row group via DuckDB's own Parquet writer -- verified byte-identical
    across repeat runs and separate processes (design §11.5 #1).
    """
    prefix = PurePosixPath(candidate.dataset) / candidate.release
    manifest_tables: list[ManifestTable] = []

    with tempfile.TemporaryDirectory() as tmp:
        for name in candidate.tables:
            table_contract = contract.tables[name]
            sort_cols = table_contract.sort_by or table_contract.primary_key
            ordered = tables[name].order(", ".join(sort_cols))
            row_count = ordered.aggregate("count(*) AS n").fetchone()[0]

            tmp_path = Path(tmp) / f"{name}.parquet"
            ordered.write_parquet(
                str(tmp_path), row_group_size=_ROW_GROUP_SIZE, compression="zstd"
            )
            _check_parquet_matches_contract(table_contract, tmp_path)
            data = tmp_path.read_bytes()
            sha256 = hashlib.sha256(data).hexdigest()

            table_dir = prefix / "tables" / name
            out.put_if_absent(
                table_dir / "data" / "part-00000.parquet", data, content_type=_PARQUET_CONTENT_TYPE
            )

            materialization = _MATERIALIZATION_FOR_TEMPORAL_MODEL[table_contract.temporal_model]
            file_index = TableFileIndex(
                table=name,
                release=candidate.release,
                materialization=materialization,
                files=(
                    FileEntry(
                        uri="data/part-00000.parquet",
                        size=len(data),
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
    )
    out.put_if_absent(
        prefix / "provenance.json", _provenance_bytes(manifest), content_type=_JSON_CONTENT_TYPE
    )
    out.put_if_absent(prefix / "lineage.json", b'{"edges": []}', content_type=_JSON_CONTENT_TYPE)
    out.put_if_absent(
        prefix / "manifest.json", manifest.to_json().encode(), content_type=_JSON_CONTENT_TYPE
    )
    return manifest


def _provenance_bytes(manifest: ReleaseManifest) -> bytes:
    # ponytail: this exists only so manifest.provenance's link resolves; a real
    # provenance document (source retrieval timestamps/checksums, design §6.5's
    # SourceArtifact) is M4 territory.
    return json.dumps(
        {"source_asset_versions": [v.to_dict() for v in manifest.source_asset_versions]}, indent=2
    ).encode()


def verify_release(store: ObjectStore, dataset_id: str, release_id: str) -> AcceptanceReport:
    """Cold-path acceptance (design §11.5/§11.7): reload the manifest + file indexes
    (re-running their public-path/asset-ref allowlists) and re-check size + sha256 per
    file. No ``ops``/lake import -- the seed of DuckDock's ``verify``.
    """
    prefix = PurePosixPath(dataset_id) / release_id
    checks: list[AcceptanceCheck] = []
    run_id = "unknown"

    try:
        manifest = ReleaseManifest.from_json(store.get(prefix / "manifest.json").decode())
    except Exception as exc:  # noqa: BLE001 -- surfaced as a failed check, not a crash
        checks.append(
            AcceptanceCheck("manifest_loads", passed=False, required=True, detail=str(exc)[:500])
        )
        return AcceptanceReport(
            dataset=dataset_id, release=release_id, run_id=run_id,
            checked_at=datetime.now(UTC).isoformat(), checks=tuple(checks),
        )
    checks.append(AcceptanceCheck("manifest_loads", passed=True, required=True))
    run_id = manifest.run_id
    checks.append(
        AcceptanceCheck(
            "spec_version", passed=manifest.spec_version == SPEC_VERSION, required=True,
            detail=manifest.spec_version,
        )
    )

    for table in manifest.tables:
        try:
            file_index = TableFileIndex.from_json(store.get(prefix / table.files_path).decode())
        except Exception as exc:  # noqa: BLE001
            checks.append(
                AcceptanceCheck(
                    f"{table.name}.file_index_loads", passed=False, required=True,
                    detail=str(exc)[:500],
                )
            )
            continue
        checks.append(AcceptanceCheck(f"{table.name}.file_index_loads", passed=True, required=True))

        expected = _MATERIALIZATION_FOR_TEMPORAL_MODEL[table.temporal_model]
        checks.append(
            AcceptanceCheck(
                f"{table.name}.materialization_matches_temporal_model",
                passed=file_index.materialization == expected,
                required=True,
                detail=f"{file_index.materialization.value} vs expected {expected.value}",
            )
        )

        for entry in file_index.files:
            file_path = prefix / "tables" / table.name / entry.uri
            try:
                body = store.get(file_path)
            except Exception as exc:  # noqa: BLE001
                checks.append(
                    AcceptanceCheck(
                        f"{table.name}.{entry.uri}.readable", passed=False, required=True,
                        detail=str(exc)[:200],
                    )
                )
                continue
            size_ok = len(body) == entry.size
            sha_ok = hashlib.sha256(body).hexdigest() == entry.sha256
            checks.append(
                AcceptanceCheck(
                    f"{table.name}.{entry.uri}.size_and_checksum",
                    passed=size_ok and sha_ok, required=True,
                    detail=f"size={len(body)} (expected {entry.size}), sha256_ok={sha_ok}",
                )
            )

    return AcceptanceReport(
        dataset=dataset_id, release=release_id, run_id=run_id,
        checked_at=datetime.now(UTC).isoformat(), checks=tuple(checks),
    )


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

    release_ref = f"release.{manifest.dataset}.{manifest.release}"
    ops.register_asset(
        con, ref=release_ref, writer="cdsci", asset_type="release",
        name=f"{manifest.dataset} {manifest.release}", current_version=manifest.run_id,
    )
    for source in manifest.source_asset_versions:
        ops.record_lineage(con, src_ref=source.ref, dst_ref=release_ref, edge_type="publishes")
    return receipt
