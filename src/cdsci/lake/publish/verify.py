"""``cdsci.lake.publish.verify`` — cold-path release acceptance (design §6.5-§6.7, §11.5).

:func:`verify_release` is the seed of DuckDock's ``verify``: given the in-memory
manifest :func:`cdsci.lake.publish.builder.build_release` returned (or reloading one
from ``store`` when called with no manifest) it re-runs the public-path/asset-ref
allowlists on load and re-checks every table's schema digest, declared row count, and
per-file size/checksum. It makes no ``ops`` call and needs no credentials -- kept in
its own module, separate from the write-side :mod:`cdsci.lake.publish.builder`, for
that eventual extraction.
"""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from pathlib import PurePosixPath

from .builder import _MATERIALIZATION_FOR_TEMPORAL_MODEL, ObjectStore
from .release import (
    SPEC_VERSION,
    AcceptanceCheck,
    AcceptanceReport,
    ReleaseManifest,
    TableFileIndex,
)


def _get_or_record_failure(
    store: ObjectStore, path: PurePosixPath, checks: list[AcceptanceCheck], check_name: str
) -> bytes | None:
    """``store.get`` that records a failed required check instead of raising."""
    try:
        return store.get(path)
    except Exception as exc:  # noqa: BLE001
        checks.append(
            AcceptanceCheck(check_name, passed=False, required=True, detail=str(exc)[:200])
        )
        return None


def verify_release(
    store: ObjectStore,
    dataset_id: str,
    release_id: str,
    manifest: ReleaseManifest | None = None,
) -> AcceptanceReport:
    """Cold-path acceptance (design §11.5/§11.7): given ``manifest`` (typically the
    in-memory result of ``build_release``, not yet written) -- or, when ``manifest``
    is ``None``, reloaded from ``store``'s ``manifest.json`` -- re-run the public-path/
    asset-ref allowlists and re-check every table's schema digest, declared row count,
    and per-file size/checksum.
    """
    prefix = PurePosixPath(dataset_id) / release_id
    checks: list[AcceptanceCheck] = []
    run_id = "unknown"

    if manifest is None:
        try:
            manifest = ReleaseManifest.from_json(store.get(prefix / "manifest.json").decode())
        except Exception as exc:  # noqa: BLE001 -- surfaced as a failed check, not a crash
            checks.append(
                AcceptanceCheck(
                    "manifest_loads", passed=False, required=True, detail=str(exc)[:500]
                )
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
    checks.append(
        AcceptanceCheck(
            "manifest_identity_matches_request",
            passed=(manifest.dataset == dataset_id and manifest.release == release_id),
            required=True,
            detail=f"manifest names {manifest.dataset}/{manifest.release}, "
            f"requested {dataset_id}/{release_id}",
        )
    )
    checks.append(
        AcceptanceCheck("tables_nonempty", passed=len(manifest.tables) > 0, required=True)
    )
    for doc_name, doc_path in (("provenance", manifest.provenance), ("lineage", manifest.lineage)):
        check_name = f"{doc_name}_readable"
        if _get_or_record_failure(store, prefix / doc_path, checks, check_name) is not None:
            checks.append(AcceptanceCheck(check_name, passed=True, required=True))

    for table in manifest.tables:
        schema_bytes = _get_or_record_failure(
            store, prefix / table.schema_path, checks, f"{table.name}.schema_readable"
        )
        if schema_bytes is not None:
            schema_digest = "sha256:" + hashlib.sha256(schema_bytes).hexdigest()
            checks.append(
                AcceptanceCheck(
                    f"{table.name}.schema_digest_matches",
                    passed=schema_digest == table.schema_digest,
                    required=True,
                    detail=f"{schema_digest} vs manifest {table.schema_digest}",
                )
            )

        file_index_bytes = _get_or_record_failure(
            store, prefix / table.files_path, checks, f"{table.name}.file_index_loads"
        )
        if file_index_bytes is None:
            continue
        try:
            file_index = TableFileIndex.from_json(file_index_bytes.decode())
        except Exception as exc:  # noqa: BLE001
            checks.append(
                AcceptanceCheck(
                    f"{table.name}.file_index_loads", passed=False, required=True,
                    detail=str(exc)[:500],
                )
            )
            continue
        checks.append(AcceptanceCheck(f"{table.name}.file_index_loads", passed=True, required=True))
        checks.append(
            AcceptanceCheck(
                f"{table.name}.file_index_identity_matches",
                passed=(file_index.table == table.name and file_index.release == release_id),
                required=True,
                detail=f"file index names {file_index.table}/{file_index.release}, "
                f"expected {table.name}/{release_id}",
            )
        )

        expected = _MATERIALIZATION_FOR_TEMPORAL_MODEL[table.temporal_model]
        checks.append(
            AcceptanceCheck(
                f"{table.name}.materialization_matches_temporal_model",
                passed=file_index.materialization == expected,
                required=True,
                detail=f"{file_index.materialization.value} vs expected {expected.value}",
            )
        )

        total_rows = 0
        for entry in file_index.files:
            file_path = prefix / "tables" / table.name / entry.uri
            check_name = f"{table.name}.{entry.uri}.readable"
            body = _get_or_record_failure(store, file_path, checks, check_name)
            if body is None:
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
            total_rows += entry.rows or 0
        checks.append(
            AcceptanceCheck(
                f"{table.name}.row_count_matches",
                passed=(table.row_count is not None and total_rows == table.row_count),
                required=True,
                detail=f"files sum rows={total_rows}, manifest row_count={table.row_count}",
            )
        )

    return AcceptanceReport(
        dataset=dataset_id, release=release_id, run_id=run_id,
        checked_at=datetime.now(UTC).isoformat(), checks=tuple(checks),
    )
