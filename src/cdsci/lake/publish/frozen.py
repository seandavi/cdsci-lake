"""``cdsci.lake.publish.frozen`` — Frozen DuckLake adapter (design §6.6, §11.6, §13 M2).

:func:`build_frozen_ducklake` runs after :func:`~cdsci.lake.publish.builder.build_release`
and before :func:`~cdsci.lake.publish.builder.finalize_release`: it writes one
``catalog.ducklake`` file into the release directory that *references* the Parquet
files :func:`build_release` already wrote -- via ``ducklake_add_data_files``, so no
bytes are copied -- and returns ``manifest`` with a ``ducklake``
:class:`~cdsci.lake.publish.release.ArtifactEntry` added to ``artifacts`` (``parquet``
is already there -- :func:`build_release` stamps it), status ``STAGED`` (design
§11.5's `required_artifacts` gate; promoted to ``VERIFIED`` by
:func:`~cdsci.lake.publish.builder.finalize_release` once acceptance passes).

DuckLake's own ``ducklake_add_data_files`` records whatever path it is given
verbatim and marks it ``path_is_relative = false`` (confirmed against DuckLake
1.5/duckdb 1.5.4) -- so a naive call leaves this build's absolute local path sitting
in the catalog's own metadata tables, which is exactly what design §4 rule 4
forbids. Worse, a plain SQL ``UPDATE`` against that catalog file doesn't erase the
old bytes -- DuckDB leaves them in the file's freed pages, still recoverable with
``strings``. So the catalog is instead built at a *temporary* path, relativized
there, and only then materialized into the release directory via ``COPY FROM
DATABASE`` into a brand-new file (see :func:`_materialize_catalog`) -- a fresh
database file has no freed pages to leak into. :func:`_relativize_catalog` does the
in-place rewrite at the temp path: every table/schema/file path becomes a
`catalog.ducklake`-relative Parquet URI (derived from the absolute path DuckLake
recorded, by stripping the release prefix -- never reconstructed positionally) with
``path_is_relative = true``, the shape DuckLake's own writes produce. Only then does
a consumer's ``DATA_PATH`` (local or ``https://``, overridden at ``ATTACH`` time --
see :func:`frozen_ducklake_attach_sql`) resolve correctly.

Local-store-only for M2 (design §13: R2/S3 is M4) -- :func:`build_frozen_ducklake`
takes the one existing :class:`~cdsci.lake.publish.builder.LocalDirStore` adapter,
not the abstract ``ObjectStore`` protocol, since DuckDB's ``ATTACH``/
``ducklake_add_data_files`` need a real filesystem path, not ``bytes``.

Browser CORS acceptance (design §11.6 check 8) needs a real HTTP server with CORS
headers configured -- deferred to M4 serving, not part of this module's offline
``http.server``-based acceptance tests.
"""

from __future__ import annotations

import dataclasses
import hashlib
import tempfile
from pathlib import Path

import duckdb

from ..contracts import DatasetContract, TableContract
from .builder import LocalDirStore, _expected_duckdb_type
from .release import ArtifactEntry, ArtifactStatus, ReleaseManifest

CATALOG_FILENAME = "catalog.ducklake"
_CATALOG_CONTENT_TYPE = "application/octet-stream"

# Pinned to the ``ducklake_metadata`` catalog format version this module's SQL
# (column/table names, UPDATE targets) has been validated against -- a DuckLake
# upgrade that bumps this needs conscious re-validation, not a silent format drift
# through _relativize_catalog's raw metadata-table UPDATEs.
_EXPECTED_DUCKLAKE_METADATA_VERSION = "1.0"


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def frozen_ducklake_attach_sql(
    base_url: str, *, alias: str = "published", read_only: bool = True
) -> str:
    """The ``ATTACH`` statement a consumer runs against one release's public base URL
    (local path or ``https://`` prefix, no trailing slash) -- design §11.6's "ATTACH
    instruction". The catalog stores a build-time-relative ``DATA_PATH`` placeholder
    (``./``, never this build's local path, per :func:`_relativize_catalog`); without
    ``OVERRIDE_DATA_PATH``, DuckLake resolves that ``./`` against the *attaching
    process's current working directory*, not the catalog file's own location
    (verified against DuckLake 1.5/duckdb 1.5.4) -- so ``OVERRIDE_DATA_PATH`` is used
    here to relocate ``DATA_PATH`` explicitly to ``base_url``, not because the
    catalog itself demands it. ``READ_ONLY`` (default) is the consumer's own
    responsibility -- this module never attaches for write.
    """
    base = base_url.rstrip("/")
    ro = ", READ_ONLY" if read_only else ""
    return (
        f"ATTACH {_sql_literal(f'{base}/{CATALOG_FILENAME}')} AS {alias} "
        f"(TYPE DUCKLAKE, DATA_PATH {_sql_literal(base + '/')}, OVERRIDE_DATA_PATH{ro})"
    )


def _create_table(con: duckdb.DuckDBPyConnection, alias: str, table: TableContract) -> None:
    columns_sql = ", ".join(
        f"{_quote_ident(c.name)} {_expected_duckdb_type(c.arrow_type)}"
        + ("" if c.nullable else " NOT NULL")
        for c in table.columns
    )
    con.execute(f"CREATE TABLE {alias}.{_quote_ident(table.name)} ({columns_sql})")


def _relativize_catalog(catalog_path: Path, release_prefix: Path) -> None:
    """Rewrite ``catalog_path``'s own metadata tables in place: every schema/table/file
    path becomes release-relative and ``path_is_relative = true``, replacing whatever
    this build's local absolute path ``ducklake_add_data_files`` recorded. See module
    docstring. Fails closed: raises if the catalog isn't the format version this SQL
    was validated against, and raises if any ``ducklake_data_file`` row is still
    absolute afterward, rather than silently publishing a half-relativized catalog.

    ``release_prefix`` is the absolute release directory every ``ducklake_add_data_files``
    call was pointed under (:func:`build_frozen_ducklake`'s ``prefix``) -- each data
    file's release-relative URI is derived by stripping this prefix from the absolute
    path DuckLake recorded, never reconstructed positionally (a table can have more
    than one registered file; see :func:`~cdsci.lake.publish.builder.build_release`'s
    own one-file-per-table limit, which this function doesn't assume).
    """
    con = duckdb.connect(str(catalog_path))
    try:
        version = con.execute(
            "SELECT value FROM ducklake_metadata WHERE key = 'version'"
        ).fetchone()
        if version is None or version[0] != _EXPECTED_DUCKLAKE_METADATA_VERSION:
            raise ValueError(
                f"{catalog_path}: ducklake_metadata.version is "
                f"{version[0] if version else None!r}, expected "
                f"{_EXPECTED_DUCKLAKE_METADATA_VERSION!r} -- refusing to relativize an "
                "unvalidated catalog format"
            )
        con.execute("UPDATE ducklake_schema SET path = '', path_is_relative = TRUE")
        con.execute("UPDATE ducklake_table SET path = '', path_is_relative = TRUE")
        con.execute("UPDATE ducklake_metadata SET value = './' WHERE key = 'data_path'")
        for data_file_id, abs_path in con.execute(
            "SELECT data_file_id, path FROM ducklake_data_file"
        ).fetchall():
            relative_uri = Path(abs_path).relative_to(release_prefix).as_posix()
            con.execute(
                "UPDATE ducklake_data_file SET path = ?, path_is_relative = TRUE "
                "WHERE data_file_id = ?",
                [relative_uri, data_file_id],
            )
        still_absolute = con.execute(
            "SELECT count(*) FROM ducklake_data_file WHERE NOT path_is_relative"
        ).fetchone()[0]
        if still_absolute:
            raise ValueError(
                f"{catalog_path}: {still_absolute} ducklake_data_file row(s) still "
                "absolute after relativization"
            )
    finally:
        con.close()


def _collapse_to_single_snapshot(con: duckdb.DuckDBPyConnection, alias: str) -> None:
    """Expire every ``ducklake_snapshot`` row but the final one: a released catalog
    exposes only the one published state, not this build's intermediate per-table
    commits (cdsci-lake#95 M2 review finding #7). Requires ``alias`` still attached;
    run before ``DETACH``.
    """
    snapshot_ids = [
        r[0]
        for r in con.execute(f"SELECT snapshot_id FROM ducklake_snapshots('{alias}')").fetchall()
    ]
    expire_ids = [i for i in snapshot_ids if i != max(snapshot_ids)]
    if expire_ids:
        ids_sql = "[" + ", ".join(str(i) for i in expire_ids) + "]"
        con.execute(f"CALL ducklake_expire_snapshots('{alias}', versions => {ids_sql})")


def _materialize_catalog(tmp_catalog_path: Path, catalog_path: Path) -> None:
    """Copy the relativized temp catalog into a brand-new database file at
    ``catalog_path``. A plain file copy (or an in-place ``UPDATE``) would leave this
    build's absolute temp path sitting in the source file's freed pages, still
    recoverable with ``strings`` -- ``COPY FROM DATABASE`` into a fresh file has no
    freed pages to leak into.
    """
    con = duckdb.connect()
    try:
        con.execute(f"ATTACH {_sql_literal(str(tmp_catalog_path))} AS src (READ_ONLY)")
        con.execute(f"ATTACH {_sql_literal(str(catalog_path))} AS dst")
        con.execute("COPY FROM DATABASE src TO dst")
    finally:
        con.close()


def build_frozen_ducklake(
    store: LocalDirStore, manifest: ReleaseManifest, *, contract: DatasetContract
) -> ReleaseManifest:
    """Write ``<dataset>/<release>/catalog.ducklake``, registering the Parquet files
    :func:`~cdsci.lake.publish.builder.build_release` already wrote under ``store``
    (no bytes copied), and return ``manifest`` with a ``ducklake``
    :class:`~cdsci.lake.publish.release.ArtifactEntry` added to ``artifacts`` (``parquet``
    is already there, stamped by ``build_release``), status ``STAGED`` --
    :func:`~cdsci.lake.publish.builder.finalize_release` promotes both to ``VERIFIED``
    once acceptance passes.

    The catalog is built and relativized at a temporary path, then materialized into
    the release directory as a fresh database file (see :func:`_materialize_catalog`);
    the release directory itself never holds an unrelativized or freed-page-leaking
    catalog file at any point.
    """
    prefix = store.root / manifest.dataset / manifest.release
    catalog_path = prefix / CATALOG_FILENAME
    if catalog_path.exists():
        raise FileExistsError(f"frozen ducklake catalog already exists: {catalog_path}")

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_catalog_path = Path(tmp_dir) / CATALOG_FILENAME
        con = duckdb.connect()
        try:
            # ponytail: relies on a prior `INSTALL ducklake` having cached the
            # extension locally -- offline/air-gapped runs need that done ahead of time.
            con.execute("INSTALL ducklake; LOAD ducklake;")
            # `SET TimeZone='UTC'` here does *not* make ducklake_snapshot.snapshot_time
            # UTC -- DuckLake records it from the OS's local timezone regardless of the
            # session's TimeZone setting (verified against DuckLake 1.5/duckdb 1.5.4).
            # Left unset since it would be a no-op; noted so it isn't tried again blind.
            con.execute(
                f"ATTACH {_sql_literal(f'ducklake:{tmp_catalog_path}')} AS cat (DATA_PATH '.')"
            )
            con.execute("BEGIN TRANSACTION")
            for table in manifest.tables:
                table_contract = contract.tables[table.name]
                _create_table(con, "cat", table_contract)
                data_file = prefix / "tables" / table.name / "data" / "part-00000.parquet"
                con.execute(
                    f"CALL ducklake_add_data_files('cat', {_sql_literal(table.name)}, "
                    f"{_sql_literal(str(data_file))})"
                )
            con.execute("COMMIT")
            _collapse_to_single_snapshot(con, "cat")
            con.execute("DETACH cat")
        finally:
            con.close()

        _relativize_catalog(tmp_catalog_path, prefix)
        _materialize_catalog(tmp_catalog_path, catalog_path)

    catalog_bytes = catalog_path.read_bytes()

    return dataclasses.replace(
        manifest,
        artifacts={
            # `build_release` already stamped "parquet" (staged, location "tables/") --
            # carried forward as-is, not reconstructed here.
            **manifest.artifacts,
            "ducklake": ArtifactEntry(
                status=ArtifactStatus.STAGED,
                required=True,
                location=CATALOG_FILENAME,
                size=len(catalog_bytes),
                sha256=hashlib.sha256(catalog_bytes).hexdigest(),
                content_type=_CATALOG_CONTENT_TYPE,
            ),
        },
    )
