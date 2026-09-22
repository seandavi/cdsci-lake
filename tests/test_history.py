"""Offline tests for ``cdsci.lake.history`` (cdsci-lake#96, M0).

One running R1 -> R2 -> R3 conformance narrative against
``fixtures/contracts/dataset.py``'s ``demo.entities`` scd2_release table,
covering every §11.4 scenario except the assembly/coordinate one (domain-
local, out of scope here):

new key, identical row, attribute change, missing from scope, retired key
reappears, same-release correction, out-of-order release rejected, duplicate
incoming key rejected, row outside scope rejected, another writer's scope
untouched.
"""

from __future__ import annotations

import duckdb
import pytest
from fixtures.contracts import dataset as fx

from cdsci.lake.history import CompleteScope, HistoryPlan, SCD2Policy, plan_scd2_release

SCOPE = CompleteScope(fx.WRITER_A_SCOPE_SQL)
POLICY = SCD2Policy(business_key=fx.BUSINESS_KEY, tracked_columns=("label", "source"))


@pytest.fixture
def con():
    c = duckdb.connect(":memory:")
    c.execute(
        "CREATE TABLE history (entity_id VARCHAR, label VARCHAR, source VARCHAR, "
        "valid_from VARCHAR, valid_to VARCHAR)"
    )
    c.execute(
        "INSERT INTO history VALUES (?, ?, ?, ?, ?)",
        [
            fx.SEED_WRITER_B_ROW["entity_id"],
            fx.SEED_WRITER_B_ROW["label"],
            fx.SEED_WRITER_B_ROW["source"],
            fx.SEED_WRITER_B_ROW["valid_from"],
            fx.SEED_WRITER_B_ROW["valid_to"],
        ],
    )
    yield c
    c.close()


def _incoming_relation(con: duckdb.DuckDBPyConnection, rows: list[dict]) -> duckdb.DuckDBPyRelation:
    con.execute("CREATE OR REPLACE TEMP TABLE _incoming_src AS SELECT * FROM history LIMIT 0")
    con.execute("ALTER TABLE _incoming_src DROP COLUMN valid_from")
    con.execute("ALTER TABLE _incoming_src DROP COLUMN valid_to")
    for row in rows:
        con.execute(
            "INSERT INTO _incoming_src VALUES (?, ?, ?)",
            [row["entity_id"], row["label"], row["source"]],
        )
    return con.table("_incoming_src")


def _apply(con: duckdb.DuckDBPyConnection, plan: HistoryPlan) -> None:
    for row in plan.to_close:
        con.execute(
            "UPDATE history SET valid_to = ? WHERE entity_id = ? AND valid_to IS NULL",
            [row["valid_to"], row["entity_id"]],
        )
    for row in plan.to_open:
        con.execute(
            "INSERT INTO history VALUES (?, ?, ?, ?, ?)",
            [row["entity_id"], row["label"], row["source"], row["valid_from"], row["valid_to"]],
        )
    for row in plan.to_replace_draft:
        con.execute(
            "UPDATE history SET label = ?, source = ? WHERE entity_id = ? AND valid_from = ?",
            [row["label"], row["source"], row["entity_id"], row["valid_from"]],
        )


def _plan(con, rows, release) -> HistoryPlan:
    incoming = _incoming_relation(con, rows)
    current = con.table("history")
    return plan_scd2_release(con, current, incoming, release=release, scope=SCOPE, policy=POLICY)


def test_r1_new_keys_duplicate_and_out_of_scope_rejected(con):
    plan = _plan(con, fx.R1_INCOMING, "R1")
    assert plan.inserted == 3
    assert plan.changed == plan.retired == plan.reopened == plan.unchanged == 0
    assert {r["entity_id"] for r in plan.to_open} == {"e1", "e2", "e3"}
    assert all(r["valid_from"] == "R1" and r["valid_to"] is None for r in plan.to_open)
    reasons = {(r.reason, r.business_key) for r in plan.rejections}
    assert ("duplicate_incoming_key", ("e_dup",)) in reasons
    assert ("outside_declared_scope", ("e_out",)) in reasons
    _apply(con, plan)

    # writer_b's pre-existing row is untouched by writer_a's R1 plan.
    w1 = con.execute(
        "SELECT label, source, valid_from, valid_to FROM history WHERE entity_id = 'w1'"
    ).fetchone()
    assert w1 == ("zed", "writer_b", "R0", None)
    # rejected rows were never written.
    rejected_count = con.execute(
        "SELECT count(*) FROM history WHERE entity_id IN ('e_dup', 'e_out')"
    ).fetchone()[0]
    assert rejected_count == 0


def test_r2_identical_change_missing_and_new(con):
    _apply(con, _plan(con, fx.R1_INCOMING, "R1"))
    plan = _plan(con, fx.R2_INCOMING, "R2")

    assert plan.unchanged == 1  # e1
    assert plan.changed == 1  # e3 gamma -> delta
    assert plan.retired == 1  # e2 missing from scope
    assert plan.inserted == 1  # e4
    assert plan.rejections == ()

    close_keys = {r["entity_id"] for r in plan.to_close}
    assert close_keys == {"e2", "e3"}
    open_keys = {r["entity_id"] for r in plan.to_open}
    assert open_keys == {"e3", "e4"}

    _apply(con, plan)
    e1 = con.execute("SELECT valid_from, valid_to FROM history WHERE entity_id = 'e1'").fetchone()
    assert e1 == ("R1", None)  # untouched interval
    e2 = con.execute("SELECT valid_from, valid_to FROM history WHERE entity_id = 'e2'").fetchall()
    assert e2 == [("R1", "R2")]  # closed, not reopened
    e3 = con.execute(
        "SELECT valid_from, valid_to, label FROM history WHERE entity_id = 'e3' ORDER BY valid_from"
    ).fetchall()
    assert e3 == [("R1", "R2", "gamma"), ("R2", None, "delta")]
    # writer_b's row is still untouched.
    w1 = con.execute("SELECT valid_from, valid_to FROM history WHERE entity_id = 'w1'").fetchone()
    assert w1 == ("R0", None)


def test_r3_reappear_missing_then_same_release_correction(con):
    _apply(con, _plan(con, fx.R1_INCOMING, "R1"))
    _apply(con, _plan(con, fx.R2_INCOMING, "R2"))

    pass1 = _plan(con, fx.R3_PASS1_INCOMING, "R3")
    assert pass1.reopened == 1  # e2 reappears
    assert pass1.retired == 1  # e4 missing from scope
    assert pass1.inserted == 1  # e5 drafted
    assert pass1.unchanged == 2  # e1, e3
    _apply(con, pass1)

    e2_intervals = con.execute(
        "SELECT valid_from, valid_to FROM history WHERE entity_id = 'e2' ORDER BY valid_from"
    ).fetchall()
    assert e2_intervals == [("R1", "R2"), ("R3", None)]  # old interval untouched, new one opened
    e5_draft = con.execute(
        "SELECT label, valid_from, valid_to FROM history WHERE entity_id = 'e5'"
    ).fetchone()
    assert e5_draft == ("draft1", "R3", None)

    # same-release correction: e5's draft is replaced, not closed+reopened.
    pass2 = _plan(con, fx.R3_PASS2_INCOMING, "R3")
    assert pass2.same_release_corrections == 2  # e2 (identical replace) + e5 (changed replace)
    assert pass2.to_close == ()
    assert pass2.to_open == ()
    _apply(con, pass2)

    e5_final = con.execute(
        "SELECT label, valid_from, valid_to FROM history WHERE entity_id = 'e5'"
    ).fetchall()
    assert e5_final == [("draft2", "R3", None)]  # no zero-length interval, single row
    e2_final = con.execute(
        "SELECT valid_from, valid_to FROM history WHERE entity_id = 'e2' ORDER BY valid_from"
    ).fetchall()
    assert e2_final == [("R1", "R2"), ("R3", None)]

    # postconditions across the whole run.
    open_rows = con.execute("SELECT entity_id FROM history WHERE valid_to IS NULL").fetchall()
    assert len(open_rows) == len({r[0] for r in open_rows})  # at most one open row per key
    overlap = con.execute(
        "SELECT a.entity_id FROM history a JOIN history b "
        "ON a.entity_id = b.entity_id AND a.valid_from < b.valid_from "
        "WHERE a.valid_to IS NULL OR a.valid_to > b.valid_from"
    ).fetchall()
    assert overlap == []  # no overlapping validity intervals

    # writer_b's row is still untouched after three writer_a releases.
    w1 = con.execute("SELECT valid_from, valid_to FROM history WHERE entity_id = 'w1'").fetchone()
    assert w1 == ("R0", None)


def test_out_of_order_release_rejected(con):
    _apply(con, _plan(con, fx.R1_INCOMING, "R1"))
    _apply(con, _plan(con, fx.R2_INCOMING, "R2"))
    _apply(con, _plan(con, fx.R3_PASS1_INCOMING, "R3"))
    _apply(con, _plan(con, fx.R3_PASS2_INCOMING, "R3"))

    plan = _plan(con, fx.R2_INCOMING, "R2")
    assert len(plan.rejections) == 1
    assert plan.rejections[0].reason == "out_of_order_release"
    assert plan.to_close == plan.to_open == plan.to_replace_draft == ()
    assert plan.inserted == plan.changed == plan.retired == plan.reopened == plan.unchanged == 0
