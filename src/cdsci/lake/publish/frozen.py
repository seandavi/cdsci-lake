"""``cdsci.lake.publish.frozen`` — Frozen DuckLake adapter (design §6.6, §11.6, §13 M2).

:func:`build_frozen_ducklake` runs after :func:`~cdsci.lake.publish.builder.build_release`
and before :func:`~cdsci.lake.publish.builder.finalize_release`: it writes one
``catalog.ducklake`` file into the release directory that *references* the Parquet
files :func:`build_release` already wrote -- via ``ducklake_add_data_files``, so no
bytes are copied -- and returns ``manifest`` with ``parquet``/``ducklake``
:class:`~cdsci.lake.publish.release.ArtifactEntry` entries added to ``artifacts``
(design §11.5's `required_artifacts` gate, satisfied by :mod:`cdsci.lake.publish.verify`).

DuckLake's own ``ducklake_add_data_files`` records whatever path it is given
verbatim and marks it ``path_is_relative = false`` (confirmed against DuckLake
1.5/duckdb 1.5.4) -- so a naive call leaves this build's absolute local path sitting
in the catalog's own metadata tables, which is exactly what design §4 rule 4
forbids. :func:`_relativize_catalog` runs a plain SQL ``UPDATE`` against the just
-written catalog file (which is itself a DuckDB database) directly afterward,
rewriting every table/schema/file path to a `catalog.ducklake`-relative Parquet
URI and setting ``path_is_relative = true`` -- the same shape DuckLake's own writes
produce. Only then does a consumer's ``DATA_PATH`` (local or ``https://``,
overridden at ``ATTACH`` time -- see :func:`frozen_ducklake_attach_sql`) resolve
correctly, and only then is the catalog metadata itself free of this build's
local path.

Local-store-only for M2 (design §13: R2/S3 is M4) -- :func:`build_frozen_ducklake`
takes the one existing :class:`~cdsci.lake.publish.builder.LocalDirStore` adapter,
not the abstract ``ObjectStore`` protocol, since DuckDB's ``ATTACH``/
``ducklake_add_data_files`` need a real filesystem path, not ``bytes``.
"""

from __future__ import annotations

import dataclasses
from pathlib import Path

import duckdb

from ..contracts import DatasetContract, TableContract
from .builder import LocalDirStore, _expected_duckdb_type
from .release import ArtifactEntry, ArtifactStatus, ReleaseManifest

CATALOG_FILENAME = "catalog.ducklake"


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def frozen_ducklake_attach_sql(
    base_url: str, *, alias: str = "published", read_only: bool = True
) -> str:
    """The ``ATTACH`` statement a consumer runs against one release's public base URL
    (local path or ``https://`` prefix, no trailing slash) -- design §11.6's "ATTACH
    instruction". ``OVERRIDE_DATA_PATH`` is required: the catalog always stores a
    build-time-relative ``DATA_PATH`` placeholder (never this build's local path,
    per :func:`_relativize_catalog`), so every consumer -- local or remote -- must
    point ``DATA_PATH`` at wherever it actually fetches the release from.
    """
    base = base_url.rstrip("/")
    ro = ", READ_ONLY" if read_only else ""
    return (
        f"ATTACH {_sql_literal(f'{base}/{CATALOG_FILENAME}')} AS {alias} "
        f"(TYPE DUCKLAKE, DATA_PATH {_sql_literal(base + '/')}, OVERRIDE_DATA_PATH{ro})"
    )


def _create_table(con: duckdb.DuckDBPyConnection, alias: str, table: TableContract) -> None:
    columns_sql = ", ".join(
        f"{_quote_ident(c.name)} {_expected_duckdb_type(c.arrow_type)}" for c in table.columns
    )
    con.execute(f"CREATE TABLE {alias}.{_quote_ident(table.name)} ({columns_sql})")


def _relativize_catalog(catalog_path: Path, table_names: tuple[str, ...]) -> None:
    """Rewrite ``catalog_path``'s own metadata tables in place: every schema/table/file
    path becomes release-relative and ``path_is_relative = true``, replacing whatever
    this build's local absolute path ``ducklake_add_data_files`` recorded. See module
    docstring.
    """
    con = duckdb.connect(str(catalog_path))
    try:
        con.execute("UPDATE ducklake_schema SET path = '', path_is_relative = TRUE")
        con.execute("UPDATE ducklake_table SET path = '', path_is_relative = TRUE")
        con.execute("UPDATE ducklake_metadata SET value = './' WHERE key = 'data_path'")
        for name in table_names:
            relative_uri = f"tables/{name}/data/part-00000.parquet"
            con.execute(
                "UPDATE ducklake_data_file SET path = ?, path_is_relative = TRUE "
                "WHERE table_id = (SELECT table_id FROM ducklake_table WHERE table_name = ?)",
                [relative_uri, name],
            )
    finally:
        con.close()


def build_frozen_ducklake(
    store: LocalDirStore, manifest: ReleaseManifest, *, contract: DatasetContract
) -> ReleaseManifest:
    """Write ``<dataset>/<release>/catalog.ducklake``, registering the Parquet files
    :func:`~cdsci.lake.publish.builder.build_release` already wrote under ``store``
    (no bytes copied), and return ``manifest`` with ``parquet``/``ducklake``
    :class:`~cdsci.lake.publish.release.ArtifactEntry` entries added to ``artifacts``.
    """
    prefix = store.root / manifest.dataset / manifest.release
    catalog_path = prefix / CATALOG_FILENAME
    if catalog_path.exists():
        raise FileExistsError(f"frozen ducklake catalog already exists: {catalog_path}")

    con = duckdb.connect()
    try:
        con.execute("INSTALL ducklake; LOAD ducklake;")
        con.execute(f"ATTACH {_sql_literal(f'ducklake:{catalog_path}')} AS cat (DATA_PATH '.')")
        for table in manifest.tables:
            table_contract = contract.tables[table.name]
            _create_table(con, "cat", table_contract)
            data_file = prefix / "tables" / table.name / "data" / "part-00000.parquet"
            con.execute(
                f"CALL ducklake_add_data_files('cat', {_sql_literal(table.name)}, "
                f"{_sql_literal(str(data_file))})"
            )
        con.execute("DETACH cat")
    finally:
        con.close()

    _relativize_catalog(catalog_path, tuple(t.name for t in manifest.tables))

    return dataclasses.replace(
        manifest,
        artifacts={
            **manifest.artifacts,
            "parquet": ArtifactEntry(
                status=ArtifactStatus.VERIFIED, required=True, location="tables/"
            ),
            "ducklake": ArtifactEntry(
                status=ArtifactStatus.VERIFIED, required=True, location=CATALOG_FILENAME
            ),
        },
    )
