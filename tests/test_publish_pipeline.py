"""``publish_release`` one-call pipeline and nested column types (versioned datasets)."""

from __future__ import annotations

import dataclasses
import json
from datetime import date
from pathlib import Path

import duckdb
import pytest
from fixtures.contracts import dataset as fx
from test_release_builder import _tables

from cdsci.lake.contracts import ColumnContract, DatasetContract, TableContract, TemporalModel
from cdsci.lake.publish.builder import (
    LocalDirStore,
    _check_parquet_matches_contract,
    _expected_duckdb_type,
    _normalize_duckdb_type,
)
from cdsci.lake.publish.frozen import frozen_ducklake_attach_sql
from cdsci.lake.publish.index import load_index
from cdsci.lake.publish.pipeline import publish_release
from cdsci.lake.publish.release import SourceAssetVersion
from cdsci.lake.publish.verify import verify_release

SOURCES = (SourceAssetVersion(ref="lake.demo.events", version="snapshot:1"),)
DATASET = "demo-catalog"
TODAY = date(2026, 10, 2)


def _publish(store: LocalDirStore, tables, today: date = TODAY, contract=fx.DATASET_CONTRACT):
    return publish_release(
        store, contract=contract, tables=tables, source_asset_versions=SOURCES,
        run_id="run-1", today=today,
    )


def test_publish_release_builds_verifies_promotes(tmp_path: Path):
    store = LocalDirStore(tmp_path)
    manifest = _publish(store, _tables(duckdb.connect()))
    assert manifest.release == "2026-10-02" and manifest.status.value == "published"
    assert (tmp_path / DATASET / "2026-10-02" / "manifest.json").exists()
    assert (tmp_path / DATASET / "2026-10-02" / "catalog.ducklake").exists()
    assert verify_release(store, DATASET, "2026-10-02", contract=fx.DATASET_CONTRACT).passed
    assert load_index(store, DATASET).latest == "2026-10-02"


def test_two_publishes_same_day_give_dot_two_and_latest_follows(tmp_path: Path):
    store = LocalDirStore(tmp_path)
    con = duckdb.connect()
    assert _publish(store, _tables(con)).release == "2026-10-02"
    assert _publish(store, _tables(con)).release == "2026-10-02.2"
    assert [r.release for r in load_index(store, DATASET).releases] == [
        "2026-10-02", "2026-10-02.2",
    ]
    latest = json.loads((tmp_path / DATASET / "latest.json").read_text())
    assert latest["release"] == "2026-10-02.2"
    assert latest["manifest"] == "2026-10-02.2/manifest.json"


def test_failed_run_leaves_no_manifest_and_index_unchanged_and_id_is_reused(tmp_path: Path):
    store = LocalDirStore(tmp_path)
    con = duckdb.connect()
    _publish(store, _tables(con))
    index_before = (tmp_path / DATASET / "releases.json").read_bytes()
    latest_before = (tmp_path / DATASET / "latest.json").read_bytes()

    bad = _tables(con)
    bad["demo.events"] = con.sql("SELECT 1::BIGINT AS event_id, 'x' AS occurred_at, 'p' AS payload")
    with pytest.raises(ValueError, match="parquet type"):
        _publish(store, bad)
    assert not (tmp_path / DATASET / "2026-10-02.2" / "manifest.json").exists()
    assert (tmp_path / DATASET / "releases.json").read_bytes() == index_before
    assert (tmp_path / DATASET / "latest.json").read_bytes() == latest_before

    # plant a stale file in the leftover directory: the retry must delete the tree
    leftover = tmp_path / DATASET / "2026-10-02.2"
    leftover.mkdir(parents=True, exist_ok=True)
    (leftover / "stale.txt").write_text("stale")
    manifest = _publish(store, _tables(con))
    assert manifest.release == "2026-10-02.2"
    assert not (leftover / "stale.txt").exists()
    assert (leftover / "manifest.json").exists()
    assert load_index(store, DATASET).latest == "2026-10-02.2"


def test_acceptance_failure_leaves_no_manifest(tmp_path: Path, monkeypatch):
    from cdsci.lake.publish import pipeline

    real = pipeline.verify_release

    def tampering_verify(store, dataset, release, **kwargs):
        table_dir = tmp_path / dataset / release / "tables" / "demo.events"
        part = table_dir / "data" / "part-00000.parquet"
        part.write_bytes(part.read_bytes() + b"x")
        return real(store, dataset, release, **kwargs)

    monkeypatch.setattr(pipeline, "verify_release", tampering_verify)
    store = LocalDirStore(tmp_path)
    with pytest.raises(ValueError, match="acceptance failed"):
        _publish(store, _tables(duckdb.connect()))
    assert not (tmp_path / DATASET / "2026-10-02" / "manifest.json").exists()
    assert not (tmp_path / DATASET / "releases.json").exists()


def test_publish_release_prunes_per_keep_last(tmp_path: Path):
    store = LocalDirStore(tmp_path)
    con = duckdb.connect()
    contract = dataclasses.replace(
        fx.DATASET_CONTRACT, keep_last=2, required_artifacts=frozenset({"parquet"})
    )
    for day in (1, 2, 3):
        _publish(store, _tables(con), today=date(2026, 10, day), contract=contract)
    assert [r.release for r in load_index(store, DATASET).releases] == [
        "2026-10-02", "2026-10-03",
    ]
    assert not (tmp_path / DATASET / "2026-10-01").exists()


# --- nested column types ----------------------------------------------------

NESTED = "list<struct<a: string, b: int64>>"
NESTED_TABLE = TableContract(
    name="nested.items",
    description="Nested-type fixture.",
    grain="one row per id",
    primary_key=("id",),
    temporal_model=TemporalModel.UPSERT_LATEST_SNAPSHOT,
    owner="cdsci-lake",
    license="cc0",
    columns=(
        ColumnContract("id", "string", "Key.", nullable=False),
        ColumnContract("items", NESTED, "Nested items.", nullable=True, null_meaning="No items."),
    ),
)
NESTED_CONTRACT = DatasetContract(
    id="nested-catalog", title="t", description="d", publisher="cdsci-lake",
    tables={"nested.items": NESTED_TABLE},
)


def test_expected_duckdb_type_recurses():
    assert _expected_duckdb_type(NESTED) == 'STRUCT("a" VARCHAR, "b" BIGINT)[]'
    assert _expected_duckdb_type("struct<x: list<int32>>") == 'STRUCT("x" INTEGER[])'
    assert _normalize_duckdb_type(_expected_duckdb_type(NESTED)) == (
        str(duckdb.sql(f"SELECT CAST(NULL AS {_expected_duckdb_type(NESTED)}) AS x").types[0])
    )


def test_nested_column_builds_passes_frozen_check_and_round_trips_attach(tmp_path: Path):
    con = duckdb.connect()
    rows = con.sql(
        "SELECT * FROM (VALUES "
        "('k1', [{'a': 'x', 'b': 1::BIGINT}, {'a': 'y', 'b': 2::BIGINT}]), "
        "('k2', []::STRUCT(a VARCHAR, b BIGINT)[]), "
        "('k3', NULL::STRUCT(a VARCHAR, b BIGINT)[]) "
        ") v(id, items)"
    )
    store = LocalDirStore(tmp_path)
    manifest = publish_release(
        store, contract=NESTED_CONTRACT, tables={"nested.items": rows},
        source_asset_versions=(SourceAssetVersion(ref="lake.nested.items", version="snapshot:1"),),
        run_id="run-1", today=TODAY,
    )
    report = verify_release(store, "nested-catalog", manifest.release, contract=NESTED_CONTRACT)
    assert report.passed
    assert any(
        c.name == "nested.items.ducklake_schema_matches" and c.passed for c in report.checks
    )

    attach = duckdb.connect()
    attach.execute("INSTALL ducklake; LOAD ducklake;")
    attach.execute(
        frozen_ducklake_attach_sql(str(tmp_path / "nested-catalog" / manifest.release), alias="f")
    )
    got = attach.execute(
        'SELECT id, items FROM f.main."nested.items" ORDER BY id'
    ).fetchall()
    assert got == [("k1", [{"a": "x", "b": 1}, {"a": "y", "b": 2}]), ("k2", []), ("k3", None)]


def test_wrong_inner_type_raises_from_parquet_contract_check(tmp_path: Path):
    con = duckdb.connect()
    wrong = con.sql("SELECT 'k1' AS id, [{'a': 'x', 'b': 'not-an-int'}] AS items")
    path = tmp_path / "wrong.parquet"
    wrong.write_parquet(str(path))
    with pytest.raises(ValueError, match=r"nested\.items\.items: parquet type"):
        _check_parquet_matches_contract(NESTED_TABLE, path)

    right = con.sql("SELECT 'k1' AS id, [{'a': 'x', 'b': 1::BIGINT}] AS items")
    ok = tmp_path / "ok.parquet"
    right.write_parquet(str(ok))
    _check_parquet_matches_contract(NESTED_TABLE, ok)


def test_wrong_inner_type_fails_build_release(tmp_path: Path):
    con = duckdb.connect()
    wrong = con.sql("SELECT 'k1' AS id, [{'a': 'x', 'b': 'oops'}] AS items")
    store = LocalDirStore(tmp_path)
    with pytest.raises(ValueError, match="parquet type"):
        publish_release(
            store, contract=NESTED_CONTRACT, tables={"nested.items": wrong},
            source_asset_versions=(), run_id="r", today=TODAY,
        )
    assert not (tmp_path / "nested-catalog" / "releases.json").exists()


def test_publish_release_rejects_missing_or_unexpected_tables(tmp_path: Path):
    store = LocalDirStore(tmp_path)
    con = duckdb.connect()
    partial = {"demo.events": _tables(con)["demo.events"]}
    with pytest.raises(ValueError, match=r"missing: \['demo.entities'\]"):
        _publish(store, partial)
    extra = {**_tables(con), "demo.other": con.sql("SELECT 1 AS x")}
    with pytest.raises(ValueError, match=r"unexpected: \['demo.other'\]"):
        _publish(store, extra)
    assert not (tmp_path / DATASET).exists()


def test_publish_release_finishes_promotion_of_published_unindexed_release(
    tmp_path: Path, monkeypatch
):
    from cdsci.lake.publish import pipeline

    store = LocalDirStore(tmp_path)
    con = duckdb.connect()
    real_promote = pipeline.promote_release
    monkeypatch.setattr(
        pipeline, "promote_release", lambda *a, **k: (_ for _ in ()).throw(OSError("boom"))
    )
    with pytest.raises(OSError, match="boom"):
        _publish(store, _tables(con))
    # published (manifest.json written) but never indexed
    assert (tmp_path / DATASET / "2026-10-02" / "manifest.json").exists()
    assert not (tmp_path / DATASET / "releases.json").exists()

    monkeypatch.setattr(pipeline, "promote_release", real_promote)
    manifest = _publish(store, _tables(con))
    assert manifest.release == "2026-10-02.2"  # the published id was preserved, not rebuilt
    assert [r.release for r in load_index(store, DATASET).releases] == [
        "2026-10-02", "2026-10-02.2",
    ]
    assert verify_release(store, DATASET, "2026-10-02", contract=fx.DATASET_CONTRACT).passed
