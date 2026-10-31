"""Offline tests for ``verify_release_over_release`` (bioc-on-ice#183 step 5).

Each test writes two tiny release bundles (Parquet + ``manifest.json`` in the
``bioconice publish`` layout) to ``tmp_path``; no network beyond a loopback http.server.
"""

from __future__ import annotations

import dataclasses
import functools
import http.server
import json
import threading
from pathlib import Path

import duckdb
import pytest

from cdsci.lake.contracts import ColumnContract, DatasetContract, TableContract, TemporalModel
from cdsci.lake.publish.verify import verify_release_over_release

ENTITIES = TableContract(
    name="demo.entities",
    description="Entity catalog.",
    grain="one row per entity_id",
    primary_key=("entity_id",),
    temporal_model=TemporalModel.UPSERT_LATEST_SNAPSHOT,
    owner="cdsci-lake",
    license="cc0",
    columns=(
        ColumnContract("entity_id", "int64", "Business key.", nullable=False),
        ColumnContract("label", "string", "Tracked attribute.", nullable=True),
    ),
)
EVENTS = TableContract(
    name="demo.events",
    description="Events about entities.",
    grain="one row per (event_id, entity_id)",
    primary_key=("event_id", "entity_id"),
    temporal_model=TemporalModel.APPEND_IMMUTABLE,
    owner="cdsci-lake",
    license="cc0",
    columns=(
        ColumnContract("event_id", "int64", "Event id.", nullable=False),
        ColumnContract("entity_id", "int64", "Entity the event is about.", nullable=False),
    ),
    references={"entity_id": "demo.entities.entity_id"},
)
CONTRACTS = [
    DatasetContract(
        id="demo", title="Demo", description="Fixture.", publisher="cdsci-lake",
        tables={"demo.entities": ENTITIES, "demo.events": EVENTS},
    )
]

ENTITIES_SQL = "SELECT i::BIGINT AS entity_id, 'e' || i AS label FROM range(1, {n} + 1) t(i)"
EVENTS_SQL = "SELECT i::BIGINT AS event_id, (i % 10 + 1)::BIGINT AS entity_id FROM range(20) t(i)"


def _bundle(root: Path, release: str, tables: dict[str, str]) -> Path:
    """Write ``tables`` (name -> SELECT) as a release bundle under ``root``."""
    con = duckdb.connect()
    root.mkdir(parents=True, exist_ok=True)
    entries = []
    for name, sql in tables.items():
        schema, table = name.split(".")
        rel = f"{schema}/{table}.parquet"
        (root / schema).mkdir(parents=True, exist_ok=True)
        n = con.execute(f"COPY ({sql}) TO '{root / rel}' (FORMAT parquet)").fetchone()[0]
        cols = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{root / rel}')").fetchall()
        entries.append({
            "schema": schema, "table": table, "path": rel, "row_count": n,
            "columns": [{"name": c[0], "type": c[1], "doc": None} for c in cols],
        })
    (root / "manifest.json").write_text(json.dumps({"release": release, "tables": entries}))
    return root


@pytest.fixture
def reference(tmp_path: Path) -> Path:
    return _bundle(
        tmp_path / "v2026-10-01", "2026-10-01",
        {"demo.entities": ENTITIES_SQL.format(n=10), "demo.events": EVENTS_SQL},
    )


def _gate(
    tmp_path: Path, reference: Path | str | None, tables: dict[str, str],
    contracts=CONTRACTS, uncontracted_tables=(),
):
    candidate = _bundle(tmp_path / "candidate", "2026-10-31", tables)
    return verify_release_over_release(
        "demo", candidate, None if reference is None else str(reference), contracts,
        uncontracted_tables,
    )


def _failed(report) -> set[str]:
    return {c.name for c in report.checks if c.required and not c.passed}


def test_grown_release_passes(tmp_path, reference):
    report = _gate(
        tmp_path, reference,
        {"demo.entities": ENTITIES_SQL.format(n=12), "demo.events": EVENTS_SQL},
    )
    assert report.passed, report.to_json()
    names = {c.name for c in report.checks}
    assert {
        "reference_manifest_loads", "demo.entities.columns_unchanged",
        "demo.entities.row_count_within_bounds", "demo.events.key_not_null",
        "demo.events.key_unique", "demo.events.entity_id.references_covered",
    } <= names


def test_type_change_fails(tmp_path, reference):
    report = _gate(
        tmp_path, reference,
        {
            "demo.entities": "SELECT entity_id::VARCHAR AS entity_id, label FROM ("
            + ENTITIES_SQL.format(n=10) + ")",
            "demo.events": EVENTS_SQL,
        },
    )
    assert "demo.entities.columns_unchanged" in _failed(report)


def test_row_count_out_of_bounds_fails(tmp_path, reference):
    report = _gate(
        tmp_path, reference,
        {"demo.entities": ENTITIES_SQL.format(n=10), "demo.events": EVENTS_SQL + " LIMIT 5"},
    )
    assert _failed(report) == {"demo.events.row_count_within_bounds"}


def test_duplicate_and_null_keys_fail(tmp_path, reference):
    report = _gate(
        tmp_path, reference,
        {
            "demo.entities": "SELECT CASE WHEN entity_id = 1 THEN NULL WHEN entity_id = 2 "
            "THEN 3 ELSE entity_id END AS entity_id, label FROM ("
            + ENTITIES_SQL.format(n=10) + ")",
            "demo.events": EVENTS_SQL,
        },
    )
    assert {"demo.entities.key_not_null", "demo.entities.key_unique"} <= _failed(report)


def test_join_coverage_gap_fails(tmp_path, reference):
    report = _gate(
        tmp_path, reference,
        {"demo.entities": ENTITIES_SQL.format(n=9), "demo.events": EVENTS_SQL},
    )
    assert _failed(report) == {"demo.events.entity_id.references_covered"}
    check = next(c for c in report.checks if c.name == "demo.events.entity_id.references_covered")
    assert check.detail.endswith(": 1")


def test_new_table_gets_only_key_and_join_checks(tmp_path):
    reference = _bundle(
        tmp_path / "v2026-10-01", "2026-10-01", {"demo.entities": ENTITIES_SQL.format(n=10)}
    )
    report = _gate(
        tmp_path, reference,
        {"demo.entities": ENTITIES_SQL.format(n=10), "demo.events": EVENTS_SQL},
    )
    assert report.passed, report.to_json()
    events = {c.name for c in report.checks if c.name.startswith("demo.events.")}
    assert events == {
        "demo.events.key_not_null", "demo.events.key_unique",
        "demo.events.entity_id.references_covered",
    }


def test_dropped_table_fails(tmp_path, reference):
    report = _gate(tmp_path, reference, {"demo.entities": ENTITIES_SQL.format(n=10)})
    assert _failed(report) == {"demo.events.present"}


def test_missing_join_target_fails(tmp_path):
    report = _gate(tmp_path, None, {"demo.events": EVENTS_SQL})
    assert _failed(report) == {"demo.events.entity_id.references_covered"}


def test_unreadable_reference_fails(tmp_path):
    report = _gate(
        tmp_path, tmp_path / "no-such-release",
        {"demo.entities": ENTITIES_SQL.format(n=10), "demo.events": EVENTS_SQL},
    )
    assert _failed(report) == {"reference_manifest_loads"}


def test_table_without_contract_fails(tmp_path, reference):
    report = _gate(
        tmp_path, reference,
        {"demo.entities": ENTITIES_SQL.format(n=10), "demo.events": EVENTS_SQL},
        contracts=[],
    )
    assert _failed(report) == {"demo.entities.contract_declared", "demo.events.contract_declared"}


def test_uncontracted_exemption_is_an_optional_failure(tmp_path, reference):
    report = _gate(
        tmp_path, reference,
        {"demo.entities": ENTITIES_SQL.format(n=10), "demo.events": EVENTS_SQL},
        contracts=[],
        uncontracted_tables={"demo.entities", "demo.events"},
    )
    assert report.passed, report.to_json()
    optional = {c.name for c in report.checks if not c.required and not c.passed}
    assert optional == {"demo.entities.contract_declared", "demo.events.contract_declared"}
    assert "demo.events.row_count_within_bounds" in {c.name for c in report.checks}


def _only(table: TableContract) -> list[DatasetContract]:
    return [dataclasses.replace(CONTRACTS[0], tables={table.name: table})]


def test_duplicate_keys_without_contract_fail(tmp_path, reference):
    report = _gate(
        tmp_path, reference,
        {
            "demo.entities": ENTITIES_SQL.format(n=10) + " UNION ALL SELECT 1, 'dup'",
            "demo.events": EVENTS_SQL,
        },
        contracts=_only(EVENTS),
    )
    assert _failed(report) == {"demo.entities.contract_declared"}


def test_orphan_references_without_contract_fail(tmp_path, reference):
    report = _gate(
        tmp_path, reference,
        {
            "demo.entities": ENTITIES_SQL.format(n=10),
            "demo.events": EVENTS_SQL.replace("+ 1", "+ 5"),
        },
        contracts=_only(ENTITIES),
    )
    assert _failed(report) == {"demo.events.contract_declared"}


def test_empty_reference_fails(tmp_path):
    reference = _bundle(tmp_path / "v2026-10-01", "2026-10-01", {})
    report = _gate(
        tmp_path, reference,
        {"demo.entities": ENTITIES_SQL.format(n=10), "demo.events": EVENTS_SQL},
    )
    assert _failed(report) == {"reference_manifest_loads"}


def test_empty_candidate_fails(tmp_path):
    report = _gate(tmp_path, None, {})
    assert _failed(report) == {"candidate_has_tables"}


def test_reference_target_column_named_v(tmp_path):
    targets = TableContract(
        **{**vars(ENTITIES), "name": "demo.targets", "primary_key": ("v",),
           "columns": (ColumnContract("v", "int64", "Key.", nullable=False),)},
    )
    refs = TableContract(
        **{**vars(ENTITIES), "name": "demo.refs", "primary_key": ("id",),
           "columns": (ColumnContract("id", "int64", "Key.", nullable=False),
                       ColumnContract("v", "int64", "Target.", nullable=True)),
           "references": {"v": "demo.targets.v"}},
    )
    contracts = [dataclasses.replace(
        CONTRACTS[0], tables={"demo.targets": targets, "demo.refs": refs}
    )]
    tables = {
        "demo.targets": "SELECT i::BIGINT AS v FROM range(5) t(i)",
        "demo.refs": "SELECT i::BIGINT AS id, (i % 5)::BIGINT AS v FROM range(10) t(i)",
    }
    report = _gate(tmp_path, None, tables, contracts=contracts)
    assert report.passed, report.to_json()
    report = _gate(
        tmp_path, None, {**tables, "demo.refs": tables["demo.refs"].replace("% 5", "% 7")},
        contracts=contracts,
    )
    check = next(c for c in report.checks if c.name == "demo.refs.v.references_covered")
    assert not check.passed and check.detail.endswith(": 2")


def test_reference_manifest_over_local_http(tmp_path, reference):
    handler = functools.partial(
        http.server.SimpleHTTPRequestHandler, directory=str(reference.parent)
    )
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        report = _gate(
            tmp_path, f"http://127.0.0.1:{server.server_port}/{reference.name}/",
            {"demo.entities": ENTITIES_SQL.format(n=12), "demo.events": EVENTS_SQL},
        )
    finally:
        server.shutdown()
    assert report.passed, report.to_json()
    assert "demo.entities.row_count_within_bounds" in {c.name for c in report.checks}


def test_contract_rejects_bad_references_and_bounds():
    with pytest.raises(ValueError, match="references column"):
        TableContract(**{**vars(EVENTS), "references": {"nope": "demo.entities.entity_id"}})
    with pytest.raises(ValueError, match="references target"):
        TableContract(**{**vars(EVENTS), "references": {"entity_id": "entities"}})
    with pytest.raises(ValueError, match="row_count_bounds"):
        TableContract(**{**vars(EVENTS), "row_count_bounds": (1.2, 1.1)})
