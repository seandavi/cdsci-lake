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


class ObjectStore(Protocol):
    """Design §6.7, simplified to plain ``bytes`` in/out (not ``BinaryIO``) -- every M1
    object is small enough to hold in memory; add streaming when a table needs it."""

    def put_if_absent(self, path: PurePosixPath, body: bytes, *, content_type: str) -> None: ...

    def get(self, path: PurePosixPath) -> bytes: ...


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

    def get(self, path: PurePosixPath) -> bytes:
        dest = self._abs(path)
        if not dest.is_file():
            raise FileNotFoundError(str(path))
        return dest.read_bytes()


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
    return manifest


def _provenance_bytes(manifest: ReleaseManifest) -> bytes:
    # ponytail: this exists only so manifest.provenance's link resolves; a real
    # provenance document (source retrieval timestamps/checksums, design §6.5's
    # SourceArtifact) is M4 territory.
    return json.dumps(
        {"source_asset_versions": [v.to_dict() for v in manifest.source_asset_versions]}, indent=2
    ).encode()


def finalize_release(
    store: ObjectStore, manifest: ReleaseManifest, report: AcceptanceReport
) -> ReleaseManifest:
    """Write ``manifest.json`` with status ``published`` -- but only once ``report``
    has passed every required check (design §11.5 #8: "a required adapter failure
    prevents latest.json and registry promotion"). Raises and writes nothing on a
    failed report.
    """
    if not report.passed:
        failed = [c.name for c in report.checks if c.required and not c.passed]
        raise ValueError(
            f"{manifest.dataset} {manifest.release}: acceptance failed, refusing to publish "
            f"manifest.json (failed required checks: {failed})"
        )
    published = dataclasses.replace(
        manifest, status=ArtifactStatus.PUBLISHED, published_at=datetime.now(UTC).isoformat()
    )
    prefix = PurePosixPath(published.dataset) / published.release
    store.put_if_absent(
        prefix / "manifest.json", published.to_json().encode(), content_type=_JSON_CONTENT_TYPE
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

    release_ref = f"release.{manifest.dataset}.{manifest.release}"
    ops.register_asset(
        con, ref=release_ref, writer="cdsci", asset_type="release",
        name=f"{manifest.dataset} {manifest.release}", current_version=manifest.run_id,
    )
    for source in manifest.source_asset_versions:
        ops.record_lineage(con, src_ref=source.ref, dst_ref=release_ref, edge_type="publishes")
    return receipt
