"""Offline test for the SQLMesh-state reader (ADR-0008 Amendment; cdsci-lake#89).

A real (local, ephemeral) SQLMesh project applies one model, and
`sync_project_attribution` is exercised against that same run's own DuckDB
connection -- confirming the thin adapter wires `model.name` /
`snapshot.table_name()` / `snapshot.version` into
`ops.sync_sqlmesh_snapshot_attribution` correctly. (A second, independent
`cdsci.lake` connection can't attach the same local DuckLake catalog file
concurrently -- verified: DuckLake raises "Unique file handle conflict" -- so
this reuses SQLMesh's own connection, manually attaching the `ops` sidecar onto
it exactly as `cdsci.lake.connect._attach_ops` would.) The bracketing/
attribution mechanism itself is covered without SQLMesh in `tests/test_ops.py`.
"""

from __future__ import annotations

from pathlib import Path

import pytest

pytest.importorskip("sqlmesh")

from cdsci.lake import Settings, ops  # noqa: E402
from cdsci.lake.connect import ops_db_path  # noqa: E402
from cdsci.lake.transform.sqlmesh_sync import sync_project_attribution  # noqa: E402


def test_sync_project_attribution_attributes_a_real_sqlmesh_apply(tmp_path: Path):
    from sqlmesh.core.config import Config, GatewayConfig, ModelDefaultsConfig
    from sqlmesh.core.config.connection import DuckDBAttachOptions, DuckDBConnectionConfig
    from sqlmesh.core.context import Context

    lake_settings = Settings(storage_base_uri=f"file://{tmp_path / 'lake_root'}")
    catalog_path = tmp_path / "lake_root" / "catalog.ducklake"
    data_path = tmp_path / "lake_root" / "data"
    catalog_path.parent.mkdir(parents=True, exist_ok=True)
    data_path.mkdir(parents=True, exist_ok=True)

    project_dir = tmp_path / "project"
    (project_dir / "models").mkdir(parents=True)
    (project_dir / "models" / "widgets.sql").write_text(
        "MODEL (\n  name demo.widgets,\n  kind FULL\n);\n\nSELECT 1 AS id, 'a' AS name\n"
    )
    config = Config(
        project="cdsci_lake",
        gateways={
            "lake": GatewayConfig(
                connection=DuckDBConnectionConfig(
                    extensions=["ducklake"],
                    catalogs={
                        "lake": DuckDBAttachOptions(
                            type="ducklake",
                            path=f"ducklake:{catalog_path}",
                            data_path=str(data_path),
                        )
                    },
                )
            )
        },
        default_gateway="lake",
        default_target_environment="cdsci_lake",
        model_defaults=ModelDefaultsConfig(dialect="duckdb", start="2026-01-01"),
    )
    context = Context(paths=str(project_dir), config=config)
    context.plan(auto_apply=True, no_prompts=True)

    con = context.engine_adapter.connection
    con.execute(f"ATTACH '{ops_db_path(lake_settings)}' AS {ops.OPS};")
    ops.bootstrap(con)

    run_ids = sync_project_attribution(context, con)
    assert run_ids, "expected at least one new run from the SQLMesh apply"

    run = ops.get_run(con, run_ids[0])
    assert run["source"] == "demo.widgets"
    assert run["target"].startswith("lake.sqlmesh__demo.")

    attributed_ids = con.execute(
        "SELECT snapshot_id FROM ops.lake_ops.snapshot_attribution WHERE run_id = ?",
        [run_ids[0]],
    ).fetchall()
    assert attributed_ids
    assert ops.snapshot_run_ids(con, [attributed_ids[0][0]]) == {
        attributed_ids[0][0]: run_ids[0]
    }

    # Idempotent: re-syncing the same, unchanged apply finds nothing new.
    assert sync_project_attribution(context, con) == []
