"""The DuckLake substrate — one connection seam for every source and project.

A source ingestor calls :func:`lake_connect` and writes a table; a consumer
(dashboard / API / "ask" portal) calls it ``read_only=True`` and queries. The
catalog is a **single local DuckDB file** (ADR-0022) — no server to run — while
the table data is Parquet under the shared storage seam (local today, R2 later,
exactly like the OpenAlex pipeline's landing pad, ADR-0003).

Because DuckLake stores its data as plain Parquet, the lake **degrades to "just
Parquet"**: drop the catalog and the files are still readable. That keeps the
lock-in risk of a young format low.
"""

from __future__ import annotations

import os
from pathlib import Path
from urllib.parse import urlparse

import duckdb

from .config import Settings, get_settings
from .secrets import get_secret

LAKE = "lake"  # the ATTACH alias every query uses: ``lake.<table>``


def _auto_memory_limit() -> str:
    """~70% of system RAM as a DuckDB ``memory_limit`` string (OS headroom kept)."""
    try:
        total = os.sysconf("SC_PHYS_PAGES") * os.sysconf("SC_PAGE_SIZE")
        return f"{max(4, int(total * 0.7 / (1024**3)))}GB"
    except (ValueError, OSError, AttributeError):
        return "8GB"


def _local_root(settings: Settings) -> Path:
    """Absolute local ``./data`` root (the catalog and local data live under it)."""
    base = settings.storage_base_uri.rstrip("/")
    if base.startswith("file://"):
        raw = base[len("file://") :]
        return Path(os.path.abspath(os.path.expanduser(raw)))
    # Remote landing pad → catalog still local (ADR-0003); keep under ./data.
    return Path(os.path.abspath("./data"))


def catalog_path(settings: Settings | None = None) -> Path:
    """Local filesystem path to the single-file DuckLake catalog."""
    s = settings or get_settings()
    cat = Path(s.lake_catalog)
    path = cat if cat.is_absolute() else _local_root(s) / cat
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def data_path(settings: Settings | None = None) -> str:
    """Directory/URI under the storage seam where the lake's Parquet lives.

    Local bases return an absolute path (created on demand); ``s3://`` bases
    return the joined URI with a trailing slash (DuckLake wants a directory).
    """
    s = settings or get_settings()
    base = s.storage_base_uri.rstrip("/")
    prefix = s.lake_data_prefix.strip("/")
    if base.startswith("s3://"):
        return f"{base}/{prefix}/"
    target = _local_root(s) / prefix
    target.mkdir(parents=True, exist_ok=True)
    return str(target)


def raw_dir(source: str, settings: Settings | None = None) -> Path:
    """Local directory for a source's downloaded *raw* files (bronze layer).

    Bulk dumps land here verbatim; curated lake tables are built from them, so a
    re-curate needs no re-download (the medallion contract, ADR-0012).
    """
    s = settings or get_settings()
    path = _local_root(s) / "lake" / "raw" / source
    path.mkdir(parents=True, exist_ok=True)
    return path


def lake_connect(
    settings: Settings | None = None, *, read_only: bool = False
) -> duckdb.DuckDBPyConnection:
    """Open an in-memory DuckDB that ``ATTACH``es the lake as ``lake``.

    Two backends (``settings.lake_backend``):

    * ``"local"`` — a single-file DuckDB catalog + Parquet under the storage seam
      (dev/test/prototype; no server).
    * ``"postgres"`` — the shared platform lake: Postgres ``lake`` catalog + R2
      data, all credentials from Google Secret Manager (ADR-0024).

    ``httpfs`` + ``ducklake`` are always loaded; ``read_only=True`` attaches the
    lake read-only — the right mode for serving (a dashboard/API/project must
    never mutate the shared substrate).
    """
    s = settings or get_settings()
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("INSTALL ducklake; LOAD ducklake;")
    _apply_limits(con, s)

    if s.lake_backend == "postgres":
        _attach_postgres(con, s, read_only=read_only)
    elif s.lake_backend == "local":
        _attach_local(con, s, read_only=read_only)
    else:
        raise ValueError(f"Unknown lake_backend: {s.lake_backend!r}")
    return con


def _apply_limits(con: duckdb.DuckDBPyConnection, s: Settings) -> None:
    """Bound memory/threads and spill to a local temp dir rather than OOM."""
    tmp_dir = _local_root(s) / "lake" / "duckdb_tmp"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    con.execute(f"SET memory_limit = '{s.duckdb_memory_limit or _auto_memory_limit()}';")
    con.execute(f"SET threads = {s.duckdb_threads};")
    con.execute(f"SET temp_directory = '{tmp_dir}';")
    con.execute("SET preserve_insertion_order = false;")


def _attach_local(con: duckdb.DuckDBPyConnection, s: Settings, *, read_only: bool) -> None:
    """Attach a single-file DuckLake catalog with an explicit local/R2 data path."""
    if s.writes_to_r2 and s.r2_endpoint_url and s.r2_access_key_id:
        endpoint = urlparse(s.r2_endpoint_url).netloc or s.r2_endpoint_url
        con.execute(
            """
            CREATE OR REPLACE SECRET r2_lake (
                TYPE s3, KEY_ID ?, SECRET ?, ENDPOINT ?, REGION ?,
                URL_STYLE 'path', USE_SSL true
            );
            """,
            [s.r2_access_key_id, s.r2_secret_access_key, endpoint, s.r2_region],
        )
    ro = ", READ_ONLY" if read_only else ""
    con.execute(
        f"ATTACH 'ducklake:{catalog_path(s)}' AS {LAKE} "
        f"(DATA_PATH '{data_path(s)}'{ro});"
    )


def _attach_postgres(con: duckdb.DuckDBPyConnection, s: Settings, *, read_only: bool) -> None:
    """Attach the shared Postgres-catalog DuckLake; secrets come from GSM.

    The data path is inherited from the catalog (not re-specified). Postgres auth
    goes through ``PGPASSWORD`` so the password never lands in the ATTACH string
    or DuckDB error text; the R2 credentials become a DuckDB ``r2`` secret.
    """
    con.execute("INSTALL postgres; LOAD postgres;")
    con.execute(
        "CREATE OR REPLACE SECRET r2_lake (TYPE r2, KEY_ID ?, SECRET ?, ACCOUNT_ID ?);",
        [
            get_secret(s.r2_access_key_secret, s.gsm_project),
            get_secret(s.r2_secret_key_secret, s.gsm_project),
            get_secret(s.r2_account_id_secret, s.gsm_project),
        ],
    )
    os.environ["PGPASSWORD"] = get_secret(s.lake_pg_password_secret, s.gsm_project)
    opts = " (READ_ONLY)" if read_only else ""
    con.execute(
        f"ATTACH 'ducklake:postgres:dbname={s.lake_pg_dbname} host={s.lake_pg_host} "
        f"port={s.lake_pg_port} user={s.lake_pg_user}' AS {LAKE}{opts};"
    )


def csv_source(paths: list[Path] | str) -> str:
    """Render a ``read_csv`` source argument from a glob string or list of paths."""
    if isinstance(paths, str):
        return "'" + paths.replace("'", "''") + "'"
    quoted = ", ".join("'" + str(p).replace("'", "''") + "'" for p in paths)
    return f"[{quoted}]"


def table_exists(con: duckdb.DuckDBPyConnection, table: str) -> bool:
    """True if ``lake.<table>`` exists in the attached catalog."""
    rows = con.execute(
        "SELECT 1 FROM information_schema.tables "
        "WHERE table_catalog = ? AND table_name = ?",
        [LAKE, table],
    ).fetchall()
    return bool(rows)


def snapshots(con: duckdb.DuckDBPyConnection) -> list[tuple]:
    """List the lake's snapshots (id, time, schema_version) — the version log."""
    return con.execute(
        f"SELECT snapshot_id, snapshot_time, schema_version "
        f"FROM {LAKE}.snapshots() ORDER BY snapshot_id"
    ).fetchall()
