"""Print a DuckDB `-init` script that ATTACHes the prod lake, for bare-SQL
ad-hoc use via the `duckdb` CLI (see scripts/lake_sql.sh).

Deliberately reuses cdsci.lake.config / cdsci.lake.secrets rather than
duplicating which GSM secrets to fetch or how the ATTACH string is shaped —
this is the one place that logic lives; connect.py's _attach_postgres is the
other caller. If they drift apart, this is wrong, not a second design.

Two invocation modes:
  uv run python scripts/lake_sql_init.py              -> prints the init SQL
  uv run python scripts/lake_sql_init.py --pgpassword  -> prints only the PG
    password, so it can go straight into $PGPASSWORD and never appear in the
    init SQL file on disk (matches connect.py's own reasoning for using
    PGPASSWORD instead of embedding the password in the ATTACH string).
"""

from __future__ import annotations

import sys

from cdsci.lake.config import get_settings
from cdsci.lake.secrets import get_secret


def main() -> None:
    s = get_settings()
    if s.lake_backend != "postgres":
        print(
            "CU_OPENALEX_LAKE_BACKEND=postgres is not set -- refusing to "
            "generate a local-backend init script (that's just `duckdb "
            f"{s.storage_base_uri}` directly, no secrets needed).",
            file=sys.stderr,
        )
        raise SystemExit(1)

    if "--pgpassword" in sys.argv:
        print(get_secret(s.lake_pg_password_secret, s.gsm_project), end="")
        return

    r2_key = get_secret(s.r2_access_key_secret, s.gsm_project)
    r2_secret = get_secret(s.r2_secret_key_secret, s.gsm_project)
    r2_account = get_secret(s.r2_account_id_secret, s.gsm_project)

    # Isolate from this box's persisted secrets (a persisted `r2`/`pg_main`/
    # `lake` secret already exists in the default secret_directory) -- same
    # trap connect.py's own comment documents for the Python path.
    print("SET secret_directory = '/tmp/cdsci-lake-sql-secrets';")
    print("INSTALL httpfs; LOAD httpfs;")
    print("INSTALL ducklake; LOAD ducklake;")
    print("INSTALL postgres; LOAD postgres;")
    print(
        "CREATE OR REPLACE TEMPORARY SECRET r2_lake "
        f"(TYPE r2, KEY_ID '{r2_key}', SECRET '{r2_secret}', "
        f"ACCOUNT_ID '{r2_account}');"
    )
    # No password here on purpose -- PGPASSWORD env var, set by
    # lake_sql.sh from the --pgpassword mode above, same as connect.py.
    print(
        f"ATTACH 'ducklake:postgres:dbname={s.lake_pg_dbname} "
        f"host={s.lake_pg_host} port={s.lake_pg_port} "
        f"user={s.lake_pg_user}' AS lake;"
    )
    print(
        f"ATTACH 'dbname={s.lake_pg_dbname} host={s.lake_pg_host} "
        f"port={s.lake_pg_port} user={s.lake_pg_user}' AS ops (TYPE postgres);"
    )
    print(".timer on")


if __name__ == "__main__":
    main()
