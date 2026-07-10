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
import tempfile
from contextlib import nullcontext
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
    # Isolate DuckDB's secret store to an app-owned dir (ADR-0011 §6). The flow only
    # creates TEMPORARY secrets, so this stays empty — which sidesteps a stale
    # persistent `pg_main` secret in the default `~/.duckdb/stored_secrets` causing
    # ATTACH failures: "Ambiguity detected for secret name 'pg_main'" (temp +
    # persistent) and "Unknown secret storage found: 'local_file'".
    secret_dir = os.path.join(tempfile.gettempdir(), "cdsci-lake-duckdb-secrets")
    os.makedirs(secret_dir, exist_ok=True)
    con = duckdb.connect(config={"secret_directory": secret_dir})
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("INSTALL ducklake; LOAD ducklake;")
    _apply_limits(con, s)

    if s.lake_backend == "postgres":
        _attach_postgres(con, s, read_only=read_only)
    elif s.lake_backend == "local":
        _attach_local(con, s, read_only=read_only)
    else:
        raise ValueError(f"Unknown lake_backend: {s.lake_backend!r}")

    # The operational ledger is a writer concern (ADR-0006): attach it only when
    # the lake is writable, never for read-only consumers.
    if not read_only:
        _attach_ops(con, s)
    return con


def _apply_limits(con: duckdb.DuckDBPyConnection, s: Settings) -> None:
    """Bound memory/threads and spill to a local temp dir rather than OOM."""
    tmp_dir = (
        Path(s.duckdb_temp_directory)
        if s.duckdb_temp_directory
        else _local_root(s) / "lake" / "duckdb_tmp"
    )
    tmp_dir.mkdir(parents=True, exist_ok=True)
    con.execute(f"SET memory_limit = '{s.duckdb_memory_limit or _auto_memory_limit()}';")
    con.execute(f"SET threads = {s.duckdb_threads};")
    con.execute(f"SET temp_directory = '{tmp_dir}';")
    con.execute("SET preserve_insertion_order = false;")
    # Resilient R2 reads: large parquet GETs over the link exceed the 30s default,
    # and transient timeouts shouldn't abort a multi-hour load.
    con.execute("SET http_timeout = 600000;")  # ms (per request)
    con.execute("SET http_retries = 8;")
    con.execute("SET http_retry_backoff = 2;")


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

    Credentials come from GSM (default) or the env-backed settings when
    ``cred_source == "env"`` (ADR-0011 §6 — omicidx's Prefect workers have no
    gcloud). The GSM path is byte-for-byte unchanged.
    """
    con.execute("INSTALL postgres; LOAD postgres;")
    if s.cred_source == "env":
        r2_key, r2_secret, r2_account, pg_password = (
            s.r2_access_key_id, s.r2_secret_access_key, s.r2_account_id, s.lake_pg_password,
        )
        missing = [
            n for n, v in (
                ("r2_access_key_id", r2_key), ("r2_secret_access_key", r2_secret),
                ("r2_account_id", r2_account), ("lake_pg_password", pg_password),
            ) if not v
        ]
        if missing:
            raise ValueError(f"cred_source='env' but these settings are unset: {missing}")
    else:
        r2_key = get_secret(s.r2_access_key_secret, s.gsm_project)
        r2_secret = get_secret(s.r2_secret_key_secret, s.gsm_project)
        r2_account = get_secret(s.r2_account_id_secret, s.gsm_project)
        pg_password = get_secret(s.lake_pg_password_secret, s.gsm_project)
    con.execute(
        "CREATE OR REPLACE SECRET r2_lake (TYPE r2, KEY_ID ?, SECRET ?, ACCOUNT_ID ?);",
        [r2_key, r2_secret, r2_account],
    )
    os.environ["PGPASSWORD"] = pg_password
    opts = " (READ_ONLY)" if read_only else ""
    con.execute(
        f"ATTACH 'ducklake:postgres:dbname={s.lake_pg_dbname} host={s.lake_pg_host} "
        f"port={s.lake_pg_port} user={s.lake_pg_user}' AS {LAKE}{opts};"
    )


def ops_db_path(settings: Settings | None = None) -> Path:
    """Local filesystem path to the operational-ledger DuckDB file (``ops``).

    A sibling of the catalog file — deliberately a *separate* DuckDB file, not the
    ``.ducklake`` catalog, so the ledger isn't a read-write double-attach of the
    catalog (ADR-0006).
    """
    s = settings or get_settings()
    return catalog_path(s).parent / "ops.duckdb"


def _attach_ops(con: duckdb.DuckDBPyConnection, s: Settings) -> None:
    """Attach the operational ledger as ``ops`` and ensure its schema (ADR-0006).

    Postgres backend: the same ``lake`` Postgres DB, reached through the ``postgres``
    extension (already loaded, ``PGPASSWORD`` already set by ``_attach_postgres``),
    in its own ``lake_ops`` schema. Local backend: a sibling ``ops.duckdb`` file.
    """
    from . import ops  # local import avoids a connect<->ops circular dependency

    if s.lake_backend == "postgres":
        con.execute(
            f"ATTACH 'dbname={s.lake_pg_dbname} host={s.lake_pg_host} "
            f"port={s.lake_pg_port} user={s.lake_pg_user}' AS {ops.OPS} (TYPE postgres);"
        )
    else:
        con.execute(f"ATTACH '{ops_db_path(s)}' AS {ops.OPS};")
    ops.bootstrap(con)


def csv_source(paths: list[Path] | str) -> str:
    """Render a ``read_csv`` source argument from a glob string or list of paths."""
    if isinstance(paths, str):
        return "'" + paths.replace("'", "''") + "'"
    quoted = ", ".join("'" + str(p).replace("'", "''") + "'" for p in paths)
    return f"[{quoted}]"


def upsert(
    con: duckdb.DuckDBPyConnection,
    target: str,
    source_sql: str,
    key: str | list[str],
    *,
    exclude_change_cols: list[str] | None = None,
) -> int:
    """MERGE the rows of ``source_sql`` into ``target`` on ``key``; return row count.

    ``target`` is a fully-qualified ``catalog.schema.table``. The schema and an
    empty target are created on first run; thereafter each call MERGEs — a row
    INSERTs when new and UPDATEs **only when a non-key column actually differs**
    (``IS DISTINCT FROM``). So a re-run of unchanged data writes nothing and adds
    no snapshot, and every DuckLake snapshot records just the real deltas — the
    point of time-travel. A bulk ``CREATE OR REPLACE`` would instead make every
    snapshot a full rewrite.

    ``exclude_change_cols`` are columns set on update (via ``UPDATE SET *``) but
    **ignored** in the change predicate — for per-load stamps like
    ``snapshot_version`` that differ on every load. Without this, such a column
    would mark every row "changed" each load and force a full rewrite; with it,
    only rows whose *real* data moved are rewritten, and the stamp then records
    the snapshot in which a row last actually changed.

    When called inside an :func:`cdsci.lake.ops.run` block, the MERGE is wrapped
    in that run's :meth:`~cdsci.lake.ops.Run.attribute` so the snapshot it produces
    is self-describing (author/source/run_id, ADR-0009). An idempotent MERGE makes
    no snapshot even when wrapped, so the no-op-is-free contract holds; outside a
    run (e.g. a direct test call) the write is unattributed.
    """
    from . import ops  # local import avoids a connect<->ops circular dependency

    parts = target.split(".")
    if len(parts) != 3:
        raise ValueError(f"target must be catalog.schema.table, got {target!r}")
    catalog, schema, table = parts
    keys = [key] if isinstance(key, str) else list(key)
    ignore = set(exclude_change_cols or [])

    run = ops.active_run()
    attribution = run.attribute(table) if run is not None else nullcontext()
    with attribution:
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema};")
        con.execute(f"CREATE OR REPLACE TEMP TABLE _cri_stage AS {source_sql};")
        con.execute(
            f"CREATE TABLE IF NOT EXISTS {target} AS SELECT * FROM _cri_stage WHERE false;"
        )

        cols = [c[0] for c in con.execute("DESCRIBE _cri_stage").fetchall()]
        compare = [c for c in cols if c not in keys and c not in ignore]
        on = " AND ".join(f"t.{k} = s.{k}" for k in keys)
        matched = ""
        if compare:
            changed = " OR ".join(f"t.{c} IS DISTINCT FROM s.{c}" for c in compare)
            matched = f"WHEN MATCHED AND ({changed}) THEN UPDATE SET * "
        con.execute(
            f"MERGE INTO {target} AS t USING _cri_stage AS s ON {on} "
            f"{matched}WHEN NOT MATCHED THEN INSERT *;"
        )
    return con.execute(f"SELECT count(*) FROM {target}").fetchone()[0]


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
        f"SELECT snapshot_id, snapshot_time::VARCHAR AS snapshot_time, schema_version "
        f"FROM {LAKE}.snapshots() ORDER BY snapshot_id"
    ).fetchall()
