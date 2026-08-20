"""SQLMesh config for cdsci-lake's transform layer (ADR-0019).

Reuses ``cdsci.lake``'s own ``Settings`` + credential resolution, so the same
GSM secrets that drive `lake_connect` drive SQLMesh — no second credential path
to keep in sync. The shared DuckLake attaches as ``lake``; SQLMesh state lives
in that same Postgres under the ``sqlmesh`` schema, shared with omicidx
(ADR-0019 decision 3).

**This project never targets `prod`.** `default_target_environment` pins plans
to `cdsci_lake`'s own environment: a producer that only plans its own
environment cannot damage another's virtual layer, and models reach `prod` only
by deliberate promotion once another producer depends on them (ADR-0019
decision 6). Targeting prod requires typing it explicitly.
"""

from __future__ import annotations

import os

from sqlmesh.core.config import Config, GatewayConfig, ModelDefaultsConfig
from sqlmesh.core.config.connection import (
    DuckDBAttachOptions,
    DuckDBConnectionConfig,
    PostgresConnectionConfig,
)

from cdsci.lake.config import get_settings
from cdsci.lake.connect import resolve_lake_credentials

_s = get_settings()
if _s.lake_backend != "postgres":
    raise RuntimeError(
        "The transform layer targets the shared postgres lake. "
        "Set CU_OPENALEX_LAKE_BACKEND=postgres."
    )

_r2_key, _r2_secret, _r2_account, _pg_password = resolve_lake_credentials(_s)

# PGPASSWORD is set for any libpq consumer in-process, but DuckLake's own attach
# does not pick it up (verified: "fe_sendauth: no password supplied"), so the
# password is also inlined into the DSN below — local process only, same
# tradeoff omicidx's SQLMesh config already accepts. Unlike
# connect._attach_postgres, this means the password can appear in DuckDB error
# text; keep SQLMesh output out of shared logs.
os.environ["PGPASSWORD"] = _pg_password

# data_path is deliberately omitted: the catalog's stored path governs existing
# tables, and passing a mismatched one makes DuckLake refuse the attach outright.
connection = DuckDBConnectionConfig(
    extensions=["httpfs", "postgres", "ducklake"],
    secrets=[
        {
            "type": "r2",
            "key_id": _r2_key,
            "secret": _r2_secret,
            "account_id": _r2_account,
        }
    ],
    catalogs={
        "lake": DuckDBAttachOptions(
            type="ducklake",
            path=(
                f"postgres:dbname={_s.lake_pg_dbname} host={_s.lake_pg_host} "
                f"port={_s.lake_pg_port} user={_s.lake_pg_user} "
                f"password={_pg_password}"
            ),
        )
    },
)

state_connection = PostgresConnectionConfig(
    host=_s.lake_pg_host,
    port=int(_s.lake_pg_port),
    user=_s.lake_pg_user,
    password=_pg_password,
    database=_s.lake_pg_dbname,
)

config = Config(
    # Load-bearing: an unset project name is "" (falsy), which disables SQLMesh's
    # preserve-other-projects guard for EVERY project sharing this state
    # (core/context.py:692,702). See tests/test_transform_config.py.
    project="cdsci_lake",
    gateways={
        "lake": GatewayConfig(
            connection=connection,
            state_connection=state_connection,
            state_schema="sqlmesh",
        )
    },
    default_gateway="lake",
    default_target_environment="cdsci_lake",
    model_defaults=ModelDefaultsConfig(dialect="duckdb", start="2026-08-01"),
)
