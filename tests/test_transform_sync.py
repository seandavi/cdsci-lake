"""Offline tests for the transform sync seam (cdsci-lake#85).

``sync()`` itself is exercised against the same real-SQLMesh-apply fixture
``tests/test_sqlmesh_sync.py`` uses (a local DuckLake, one model, one apply).
The CLI is exercised via typer's ``CliRunner`` with the SQLMesh ``Context``
monkeypatched to that same fixture context -- avoiding a second, concurrent
connection to the same local-file DuckLake catalog (verified elsewhere:
DuckLake raises "Unique file handle conflict" for that), so ``lake_connect``
is monkeypatched alongside it to hand back the fixture's own already-attached
connection.
"""

from __future__ import annotations

from pathlib import Path

import pytest

pytest.importorskip("sqlmesh")

from cdsci.lake import Settings, ops  # noqa: E402
from cdsci.lake.connect import ops_db_path  # noqa: E402
from cdsci.lake.transform import __main__ as transform_main  # noqa: E402
from cdsci.lake.transform import sync as sync_mod  # noqa: E402


def _build_fixture_context(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """A real, applied one-model SQLMesh context on a local DuckLake -- same
    shape as ``tests/test_sqlmesh_sync.py``'s fixture. Returns ``(context, con)``
    with ``ops`` already attached and bootstrapped on ``con``."""
    from sqlmesh.core.analytics import disable_analytics
    from sqlmesh.core.config import Config, GatewayConfig, ModelDefaultsConfig
    from sqlmesh.core.config.connection import DuckDBAttachOptions, DuckDBConnectionConfig
    from sqlmesh.core.context import Context

    disable_analytics()
    monkeypatch.setenv("SQLMESH_HOME", str(tmp_path))

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
    return context, con


def test_sync_is_idempotent(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    context, con = _build_fixture_context(tmp_path, monkeypatch)

    first = sync_mod.sync(con, context)
    assert first.models_seen == 1
    assert first.skipped_foreign == 0
    assert first.runs_written == 1
    assert first.snapshots_attributed >= 1

    second = sync_mod.sync(con, context)
    assert second.models_seen == 1
    assert second.runs_written == 0
    assert second.snapshots_attributed == 0


def test_cli_sync_prints_report_and_exits_zero(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    import json

    from typer.testing import CliRunner

    context, con = _build_fixture_context(tmp_path, monkeypatch)
    monkeypatch.setattr("sqlmesh.core.context.Context", lambda *a, **kw: context)
    monkeypatch.setattr(transform_main, "lake_connect", lambda *a, **kw: con)

    result = CliRunner().invoke(transform_main.app, ["sync"])
    assert result.exit_code == 0, result.output
    report = json.loads(result.output)
    assert report == {
        "models_seen": 1,
        "runs_written": 1,
        "snapshots_attributed": report["snapshots_attributed"],
        "skipped_foreign": 0,
    }
    assert report["snapshots_attributed"] >= 1


def test_cli_sync_exits_nonzero_on_exception(monkeypatch: pytest.MonkeyPatch):
    from typer.testing import CliRunner

    class _FakeCon:
        def close(self) -> None:
            pass

    monkeypatch.setattr("sqlmesh.core.context.Context", lambda *a, **kw: object())
    monkeypatch.setattr(transform_main, "lake_connect", lambda *a, **kw: _FakeCon())

    def _raise(con, context):
        raise RuntimeError("boom")

    monkeypatch.setattr(sync_mod, "sync", _raise)

    result = CliRunner().invoke(transform_main.app, ["sync"])
    assert result.exit_code != 0
