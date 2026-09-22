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
import json
import re
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath

import duckdb

from ..contracts import DatasetContract
from .builder import (
    _MATERIALIZATION_FOR_TEMPORAL_MODEL,
    LocalDirStore,
    ObjectStore,
    _expected_duckdb_type,
)
from .frozen import CATALOG_FILENAME, frozen_ducklake_attach_sql
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


_PRIVATE_PATH_RAW_MARKERS = (rb"/(home|tmp|var|mnt)/", rb"://", rb"[A-Za-z]:[\\/]")
_PRIVATE_PATH_RAW_PATTERN = re.compile(b"|".join(_PRIVATE_PATH_RAW_MARKERS))


def _inspect_catalog_metadata(catalog_path: Path) -> tuple[list[str], int]:
    """Open ``catalog_path`` as a plain (non-DuckLake) database and return
    ``(leak_reasons, snapshot_count)``: ``leak_reasons`` names every place (raw file
    bytes, or a ``table.column``) that still looks like an absolute path or URL
    scheme -- design §4 rule 4 -- and ``snapshot_count`` is the released catalog's own
    ``ducklake_snapshot`` row count (a release is one published state -- cdsci-lake#95
    M2 review finding #7's single-snapshot collapse). Reasons never include the leaked
    value itself, only its location -- an ``AcceptanceCheck.detail`` is itself a
    public-artifact field (design §4), so it must not carry the very private path
    this check exists to catch.
    """
    reasons: list[str] = []
    if _PRIVATE_PATH_RAW_PATTERN.search(catalog_path.read_bytes()):
        reasons.append("raw catalog bytes contain an absolute-path or URL-scheme marker")

    con = duckdb.connect(str(catalog_path), read_only=True)
    try:
        table_names = [
            r[0] for r in con.execute("SELECT table_name FROM information_schema.tables").fetchall()
        ]
        for table_name in table_names:
            for col_name, col_type, *_ in con.execute(f"DESCRIBE {table_name}").fetchall():
                if col_type != "VARCHAR":
                    continue
                hit = con.execute(
                    f'SELECT count(*) FROM {table_name} WHERE "{col_name}" IS NOT NULL '
                    f'AND ("{col_name}" LIKE \'/%\' OR "{col_name}" LIKE \'%://%\')'
                ).fetchone()[0]
                if hit:
                    reasons.append(f"{table_name}.{col_name}")
        snapshot_count = con.execute("SELECT count(*) FROM ducklake_snapshot").fetchone()[0]
    finally:
        con.close()
    return reasons, snapshot_count


def _check_frozen_ducklake(
    store: LocalDirStore, prefix: PurePosixPath, manifest: ReleaseManifest
) -> list[AcceptanceCheck]:
    """Design §11.6: a fresh, credential-free ``ATTACH`` of this release's own
    ``catalog.ducklake`` (local -- the http-served variant is the same statement
    against a served base URL, exercised in acceptance, not here) -- every manifest
    table is discoverable, its ``count(*)`` matches the manifest row count, a bounded
    sample query genuinely opens its Parquet file(s) (check 3), and its columns match
    ``schema.json``. Also checks the catalog carries no private path (design §4 rule
    4), collapses to one published snapshot (review finding #7), the ``ducklake``
    artifact's recorded checksum matches the file on disk, and mutation under
    ``READ_ONLY`` fails (check 10).
    """
    checks: list[AcceptanceCheck] = []
    catalog_dir = store.root / prefix
    catalog_path = catalog_dir / CATALOG_FILENAME
    if not catalog_path.is_file():
        checks.append(
            AcceptanceCheck("ducklake.catalog_readable", passed=False, required=True)
        )
        return checks
    checks.append(AcceptanceCheck("ducklake.catalog_readable", passed=True, required=True))

    artifact = manifest.artifacts.get("ducklake")
    if artifact is not None and artifact.sha256 is not None:
        actual_bytes = catalog_path.read_bytes()
        actual_size = len(actual_bytes)
        actual_sha256 = hashlib.sha256(actual_bytes).hexdigest()
        checks.append(
            AcceptanceCheck(
                "ducklake.artifact_checksum_matches",
                passed=(actual_size == artifact.size and actual_sha256 == artifact.sha256),
                required=True,
                detail=f"size={actual_size} vs manifest {artifact.size}, "
                f"sha256_ok={actual_sha256 == artifact.sha256}",
            )
        )

    try:
        leak_reasons, snapshot_count = _inspect_catalog_metadata(catalog_path)
    except Exception as exc:  # noqa: BLE001 -- e.g. a corrupt catalog file; type name
        # only, same reasoning as the attach_succeeds except-clause below.
        checks.append(
            AcceptanceCheck(
                "ducklake.no_private_paths",
                passed=False,
                required=True,
                detail=type(exc).__name__,
            )
        )
        checks.append(
            AcceptanceCheck(
                "ducklake.single_snapshot", passed=False, required=True, detail=type(exc).__name__
            )
        )
    else:
        checks.append(
            AcceptanceCheck(
                "ducklake.no_private_paths",
                passed=not leak_reasons,
                required=True,
                detail="; ".join(leak_reasons)[:500],
            )
        )
        checks.append(
            AcceptanceCheck(
                "ducklake.single_snapshot",
                passed=snapshot_count == 1,
                required=True,
                detail=f"snapshot_count={snapshot_count}",
            )
        )

    con = duckdb.connect()
    try:
        con.execute("INSTALL ducklake; LOAD ducklake;")
        con.execute(frozen_ducklake_attach_sql(str(catalog_dir), alias="frozen"))
    except Exception as exc:  # noqa: BLE001 -- never the raw message, which can carry
        # this build's local path (design §4 rule 4) and would itself then fail the
        # public-path allowlist AcceptanceCheck.__post_init__ runs on `detail`.
        checks.append(
            AcceptanceCheck(
                "ducklake.attach_succeeds",
                passed=False,
                required=True,
                detail=type(exc).__name__,
            )
        )
        return checks
    checks.append(AcceptanceCheck("ducklake.attach_succeeds", passed=True, required=True))

    try:
        if manifest.tables:
            first_table = manifest.tables[0].name
            try:
                con.execute(f'DELETE FROM frozen.main."{first_table}" WHERE 1=0')
            except Exception:  # noqa: BLE001 -- expected: READ_ONLY must reject this
                checks.append(
                    AcceptanceCheck("ducklake.read_only_enforced", passed=True, required=True)
                )
            else:
                checks.append(
                    AcceptanceCheck(
                        "ducklake.read_only_enforced",
                        passed=False,
                        required=True,
                        detail="mutation under READ_ONLY unexpectedly succeeded",
                    )
                )

        table_names = {
            r[0] for r in con.execute(
                "SELECT table_name FROM duckdb_tables() WHERE database_name = 'frozen'"
            ).fetchall()
        }
        for table in manifest.tables:
            check_name = f"{table.name}.ducklake_table_exists"
            if table.name not in table_names:
                checks.append(AcceptanceCheck(check_name, passed=False, required=True))
                continue
            checks.append(AcceptanceCheck(check_name, passed=True, required=True))

            qualified = f'frozen.main."{table.name}"'
            count = con.execute(f"SELECT count(*) FROM {qualified}").fetchone()[0]
            checks.append(
                AcceptanceCheck(
                    f"{table.name}.ducklake_row_count_matches",
                    passed=(table.row_count is not None and count == table.row_count),
                    required=True,
                    detail=f"ducklake count={count}, manifest row_count={table.row_count}",
                )
            )

            try:
                con.execute(f"SELECT * FROM {qualified} LIMIT 5").fetchall()
            except Exception as exc:  # noqa: BLE001 -- e.g. a deleted/corrupt Parquet
                # file behind a catalog whose count(*) is answered from catalog stats
                # alone (design §11.6 check 3) -- type name only, see attach_succeeds.
                checks.append(
                    AcceptanceCheck(
                        f"{table.name}.ducklake_sample_readable",
                        passed=False,
                        required=True,
                        detail=type(exc).__name__,
                    )
                )
            else:
                checks.append(
                    AcceptanceCheck(
                        f"{table.name}.ducklake_sample_readable", passed=True, required=True
                    )
                )

            schema_bytes = _get_or_record_failure(
                store, prefix / table.schema_path, checks, f"{table.name}.ducklake_schema_readable"
            )
            if schema_bytes is None:
                continue
            expected_columns = [
                (c["name"], _expected_duckdb_type(c["arrow_type"]), c["nullable"])
                for c in json.loads(schema_bytes)["columns"]
            ]
            described = con.execute(f"DESCRIBE {qualified}").fetchall()
            actual_columns = [
                (name, col_type, null == "YES") for name, col_type, null, *_ in described
            ]
            checks.append(
                AcceptanceCheck(
                    f"{table.name}.ducklake_schema_matches",
                    passed=actual_columns == expected_columns,
                    required=True,
                    detail=f"ducklake columns {actual_columns} vs schema.json {expected_columns}",
                )
            )
    finally:
        con.close()
    return checks


def verify_release(
    store: ObjectStore,
    dataset_id: str,
    release_id: str,
    manifest: ReleaseManifest | None = None,
    *,
    contract: DatasetContract | None = None,
) -> AcceptanceReport:
    """Cold-path acceptance (design §11.5/§11.7): given ``manifest`` (typically the
    in-memory result of ``build_release``, not yet written) -- or, when ``manifest``
    is ``None``, reloaded from ``store``'s ``manifest.json`` -- re-run the public-path/
    asset-ref allowlists and re-check every table's schema digest, declared row count,
    and per-file size/checksum.

    ``contract``, when given, adds a ``required_artifacts_present`` check (design
    §11.5 #8's ``required_artifacts`` gate) -- omitted, not skipped-as-passed, when
    ``contract`` is ``None``, since a caller with no contract has no basis to know
    what's required. When ``store`` is a :class:`~cdsci.lake.publish.builder.LocalDirStore`
    and the manifest declares a ``ducklake`` artifact, this also runs design §11.6's
    Frozen DuckLake acceptance: a fresh, credential-free ``ATTACH`` of the release's
    own ``catalog.ducklake``, checking every manifest table is discoverable, its
    ``count(*)`` matches the manifest row count, and its columns match ``schema.json``.
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
    if contract is not None:
        missing_artifacts = contract.required_artifacts - manifest.artifacts.keys()
        checks.append(
            AcceptanceCheck(
                "required_artifacts_present",
                passed=not missing_artifacts,
                required=True,
                detail=f"missing: {sorted(missing_artifacts)}" if missing_artifacts else "",
            )
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

    if "ducklake" in manifest.artifacts:
        if isinstance(store, LocalDirStore):
            checks.extend(_check_frozen_ducklake(store, prefix, manifest))
        else:
            checks.append(
                AcceptanceCheck(
                    "ducklake.acceptance_supported",
                    passed=False,
                    required=True,
                    detail=f"Frozen DuckLake acceptance not implemented for {type(store).__name__}",
                )
            )

    return AcceptanceReport(
        dataset=dataset_id, release=release_id, run_id=run_id,
        checked_at=datetime.now(UTC).isoformat(), checks=tuple(checks),
    )
