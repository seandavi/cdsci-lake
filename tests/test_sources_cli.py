"""Offline tests for the shared source CLI driver (``cdsci.lake.sources._cli``, issue #52).

Exercises the two claims the issue's fix rests on: (1) the generated ``run``
command is a thin shell over a source's own ``ingest()`` -- it doesn't
re-implement the ``ops.run`` bracket, it just calls through, so any ledger row
comes from ``ingest()`` itself, same as every hand-written CLI before it; and
(2) a name absent from ``ops.SOURCES`` cannot get a CLI at all -- the
structural fix that makes the ``ontology`` drift (no ``ops.SOURCES`` entry, no
ledger row) impossible to repeat. No network.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from typer.testing import CliRunner

from cdsci.lake import Settings, lake_connect, ops, upsert
from cdsci.lake.sources._cli import build_app

runner = CliRunner()


@pytest.fixture
def lake_settings(tmp_path: Path) -> Settings:
    return Settings(storage_base_uri=f"file://{tmp_path}")


def _stub_ingest(lake_settings: Settings):
    """A minimal ``ingest(**kwargs) -> dict`` shaped exactly like a real source's:
    self-connects, self-brackets in ``ops.run``, returns ``r.summary()``."""

    def ingest(*, schema: str = "bioregistry", limit: int | None = None) -> dict:
        con = lake_connect(lake_settings)
        try:
            target = f"lake.{schema}.stub"
            with ops.run(con, source="bioregistry", target=target, version="v1") as r:
                src = f"SELECT * FROM (VALUES (1)) v(id) {'LIMIT ' + str(limit) if limit else ''}"
                r.rows = upsert(con, target, src, key="id")
            return r.summary()
        finally:
            con.close()

    return ingest


def test_build_app_rejects_a_name_not_in_sources():
    """A source absent from ops.SOURCES can't get a CLI -- the structural fix."""
    with pytest.raises(ValueError, match="not registered in ops.SOURCES"):
        build_app("not_a_real_source", lambda **kwargs: {}, help="x")


def test_run_options_are_generated_from_ingest_signature(lake_settings: Settings):
    """--help lists exactly the stub's own kwargs (not a hand-written guess)."""
    app = build_app("bioregistry", _stub_ingest(lake_settings), help="x")
    result = runner.invoke(app, ["run", "--help"])
    assert result.exit_code == 0
    assert "--schema" in result.output
    assert "--limit" in result.output
    # `settings` is driver-injected, never a CLI option.
    assert "--settings" not in result.output


def test_run_command_calls_ingest_and_produces_a_ledger_row(lake_settings: Settings):
    """The generated `run` command is a pass-through: ingest()'s own ops.run bracket
    is what lands the ledger row -- the driver doesn't (and shouldn't) re-implement it."""
    app = build_app("bioregistry", _stub_ingest(lake_settings), help="x")
    result = runner.invoke(app, ["run", "--schema", "bioregistry"])
    assert result.exit_code == 0, result.output
    assert "changed" in result.output

    con = lake_connect(lake_settings)
    try:
        last = ops.last_run(con, "bioregistry")
        assert last is not None
        assert last["status"] == "success"
        assert last["target"] == "lake.bioregistry.stub"
    finally:
        con.close()


def test_run_command_reports_idempotent_rerun(lake_settings: Settings):
    """A second identical run echoes 'no change' -- the generic echo reads ingest()'s
    own summary dict (`changed`/`status`), not a source-specific format string."""
    app = build_app("bioregistry", _stub_ingest(lake_settings), help="x")
    runner.invoke(app, ["run"])
    result = runner.invoke(app, ["run"])
    assert result.exit_code == 0, result.output
    assert "no change (idempotent)" in result.output
