"""Offline tests for ``cdsci.lake.history`` (cdsci-lake#96, M0).

One running R1 -> R2 -> R3 conformance narrative against
``fixtures/contracts/dataset.py``'s ``demo.entities`` scd2_release table,
covering every §11.4 scenario except the assembly/coordinate one (domain-
local, out of scope here):

new key, identical row, attribute change, missing from scope, retired key
reappears, same-release correction, out-of-order release rejected, duplicate
incoming key rejected, row outside scope rejected, another writer's scope
untouched, already-published release rejected, every rejection emptying the
whole plan, a key closed under this writer but currently open under another
writer (N1), an incoming row for a key open under another writer rejected
rather than transferred (N1), and another writer's incompatible release
vocabulary never reaching this writer's ``release_key`` (N1).
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
            "UPDATE history SET valid_to = ? WHERE entity_id = ? AND valid_from = ? "
            "AND valid_to IS NULL",
            [row["valid_to"], row["entity_id"], row["valid_from"]],
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


def _plan(con, rows, release, **kwargs) -> HistoryPlan:
    incoming = _incoming_relation(con, rows)
    current = con.table("history")
    return plan_scd2_release(
        con, current, incoming, release=release, scope=SCOPE, policy=POLICY,
        release_key=fx.release_key, **kwargs,
    )


def test_r1_new_keys(con):
    plan = _plan(con, fx.R1_INCOMING, "R1")
    assert plan.inserted == 3
    assert plan.changed == plan.retired == plan.reopened == plan.unchanged == 0
    assert plan.rejections == ()
    assert {r["entity_id"] for r in plan.to_open} == {"e1", "e2", "e3"}
    assert all(r["valid_from"] == "R1" and r["valid_to"] is None for r in plan.to_open)
    _apply(con, plan)

    # writer_b's pre-existing row is untouched by writer_a's R1 plan.
    w1 = con.execute(
        "SELECT label, source, valid_from, valid_to FROM history WHERE entity_id = 'w1'"
    ).fetchone()
    assert w1 == ("zed", "writer_b", "R0", None)


def test_any_rejection_empties_the_whole_plan(con):
    """cdsci-lake#96 review F1: a duplicate key or an out-of-scope row rejects the whole release.

    Rejected keys must never flow into retirement, and valid keys in the same
    batch (e1, e2, e3) must not be partially written either -- exactly like
    ``out_of_order_release`` already empties the plan.
    """
    plan = _plan(con, fx.R1_INCOMING_WITH_REJECTIONS, "R1")
    reasons = {(r.reason, r.business_key) for r in plan.rejections}
    assert ("duplicate_incoming_key", ("e_dup",)) in reasons
    assert ("outside_declared_scope", ("e_out",)) in reasons
    assert plan.to_close == plan.to_open == plan.to_replace_draft == ()
    assert plan.inserted == plan.changed == plan.retired == plan.reopened == plan.unchanged == 0

    _apply(con, plan)
    written = con.execute("SELECT count(*) FROM history WHERE entity_id != 'w1'").fetchone()[0]
    assert written == 0  # nothing from the rejected batch was written, not even the valid keys


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
    assert all(r["valid_from"] == "R1" for r in plan.to_close)  # F5: closing row's own valid_from
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

    # F10: as-of-release join gives exactly one version per business key, at
    # each release actually planned above (single-digit ids -- string compare
    # is fine for this test-local as-of predicate, unlike history.py itself).
    for as_of in ("R1", "R2", "R3"):
        dupes = con.execute(
            "SELECT entity_id FROM history "
            "WHERE valid_from <= ? AND (valid_to IS NULL OR valid_to > ?) "
            "GROUP BY entity_id HAVING count(*) > 1",
            [as_of, as_of],
        ).fetchall()
        assert dupes == []

    # F10: business_key + valid_from is a unique row key.
    dup_rows = con.execute(
        "SELECT entity_id, valid_from FROM history "
        "GROUP BY entity_id, valid_from HAVING count(*) > 1"
    ).fetchall()
    assert dup_rows == []


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


@pytest.mark.parametrize(
    ("seen", "incoming"),
    [
        (["R9"], "R10"),  # string compare says "R10" < "R9" -- wrong, R10 is newer.
        (["2026.9"], "2026.10"),  # string compare says "2026.10" < "2026.9" -- wrong.
    ],
)
def test_release_key_avoids_string_comparison_bugs(seen: list[str], incoming: str):
    def key(value: str):
        return tuple(int(p) for p in value.split(".")) if "." in value else int(value.lstrip("R"))

    c = duckdb.connect(":memory:")
    try:
        c.execute(
            "CREATE TABLE h (entity_id VARCHAR, label VARCHAR, valid_from VARCHAR, "
            "valid_to VARCHAR)"
        )
        c.execute("INSERT INTO h VALUES ('e1', 'x', ?, NULL)", [seen[0]])
        c.execute("CREATE TEMP TABLE inc AS SELECT entity_id, label FROM h")
        scope = CompleteScope("1=1")
        policy = SCD2Policy(business_key=("entity_id",), tracked_columns=("label",))
        plan = plan_scd2_release(
            c, c.table("h"), c.table("inc"),
            release=incoming, scope=scope, policy=policy, release_key=key,
        )
        assert plan.rejections == ()  # a numerically-later release is never rejected out-of-order
    finally:
        c.close()


def test_already_published_release_is_rejected(con):
    """F4: same-release planning against a release already marked published is a no-op."""
    plan = _plan(con, fx.R1_INCOMING, "R1", published_releases=frozenset({"R1"}))
    assert len(plan.rejections) == 1
    assert plan.rejections[0].reason == "release_already_published"
    assert plan.to_close == plan.to_open == plan.to_replace_draft == ()
    assert plan.inserted == plan.changed == plan.retired == plan.reopened == plan.unchanged == 0


def test_out_of_order_and_already_published_guards_are_complementary(con):
    """Replanning the same release (R3) is legal for correction, but rejected once published."""
    _apply(con, _plan(con, fx.R1_INCOMING, "R1"))
    _apply(con, _plan(con, fx.R2_INCOMING, "R2"))
    _apply(con, _plan(con, fx.R3_PASS1_INCOMING, "R3"))

    # same-release correction: out_of_order_release does not fire for an equal key.
    correction = _plan(con, fx.R3_PASS2_INCOMING, "R3")
    assert correction.rejections == ()

    # once R3 is published, replanning it again is rejected by published_releases instead.
    replan = _plan(con, fx.R3_PASS2_INCOMING, "R3", published_releases=frozenset({"R3"}))
    assert replan.rejections[0].reason == "release_already_published"


def test_null_attribute_and_null_scope_column_are_safe():
    """F10/F16: a NULL tracked attribute round-trips; a NULL scope column is never retired."""
    c = duckdb.connect(":memory:")
    try:
        c.execute(
            "CREATE TABLE h (entity_id VARCHAR, label VARCHAR, source VARCHAR, "
            "valid_from VARCHAR, valid_to VARCHAR)"
        )
        c.execute("INSERT INTO h VALUES ('e_null_scope', 'unrelated', NULL, 'R0', NULL)")
        c.execute("CREATE TEMP TABLE inc AS SELECT entity_id, label, source FROM h LIMIT 0")
        c.execute("INSERT INTO inc VALUES ('e_new', NULL, 'writer_a')")  # NULL tracked attribute

        plan = plan_scd2_release(
            c, c.table("h"), c.table("inc"),
            release="R1", scope=SCOPE, policy=POLICY, release_key=fx.release_key,
        )
        assert plan.rejections == ()
        assert plan.inserted == 1
        assert plan.to_open[0]["entity_id"] == "e_new"
        assert plan.to_open[0]["label"] is None  # NULL attribute round-trips, not coerced/rejected
        # e_null_scope's NULL source never satisfies "source = 'writer_a'" (SQL 3-valued logic),
        # so it is outside current_in_scope_keys and never a retirement candidate.
        assert plan.retired == 0
        assert plan.to_close == ()
    finally:
        c.close()


def test_key_closed_under_writer_a_but_open_under_writer_b_is_untouched():
    """N1: a stale row closed under A must never make A's plan see B's currently open row.

    ``shared`` was closed under writer_a's scope in the past (R0->R1) and is
    currently open under writer_b (R1->). writer_a plans a release that omits
    ``shared`` entirely -- it must not be retired, since the only *currently
    open* row for that key belongs to a different scope.
    """
    c = duckdb.connect(":memory:")
    try:
        c.execute(
            "CREATE TABLE h (entity_id VARCHAR, label VARCHAR, source VARCHAR, "
            "valid_from VARCHAR, valid_to VARCHAR)"
        )
        c.execute("INSERT INTO h VALUES ('shared', 'old-a', 'writer_a', 'R0', 'R1')")
        c.execute("INSERT INTO h VALUES ('shared', 'b-current', 'writer_b', 'R1', NULL)")
        c.execute("CREATE TEMP TABLE inc AS SELECT entity_id, label, source FROM h LIMIT 0")
        c.execute("INSERT INTO inc VALUES ('e_new', 'x', 'writer_a')")  # 'shared' omitted

        plan = plan_scd2_release(
            c, c.table("h"), c.table("inc"),
            release="R3", scope=SCOPE, policy=POLICY, release_key=fx.release_key,
        )
        assert plan.rejections == ()
        assert plan.to_close == ()  # B's open row is never a retirement candidate for A
        assert plan.retired == 0
        b_row = c.execute(
            "SELECT source, valid_from, valid_to FROM h WHERE entity_id = 'shared' "
            "AND valid_to IS NULL"
        ).fetchone()
        assert b_row == ("writer_b", "R1", None)  # untouched
    finally:
        c.close()


def test_incoming_key_open_under_another_scope_is_rejected_not_transferred():
    """N1: an incoming row for a key currently open under another scope is a rejection.

    Even though the incoming row itself declares ``source = 'writer_a'`` (in
    writer_a's own declared scope), the key ``shared`` is currently open under
    writer_b -- this is ``key_owned_by_other_scope``, never an ownership
    transfer.
    """
    c = duckdb.connect(":memory:")
    try:
        c.execute(
            "CREATE TABLE h (entity_id VARCHAR, label VARCHAR, source VARCHAR, "
            "valid_from VARCHAR, valid_to VARCHAR)"
        )
        c.execute("INSERT INTO h VALUES ('shared', 'old-a', 'writer_a', 'R0', 'R1')")
        c.execute("INSERT INTO h VALUES ('shared', 'b-current', 'writer_b', 'R1', NULL)")
        c.execute("CREATE TEMP TABLE inc AS SELECT entity_id, label, source FROM h LIMIT 0")
        c.execute("INSERT INTO inc VALUES ('shared', 'a-attempt', 'writer_a')")

        plan = plan_scd2_release(
            c, c.table("h"), c.table("inc"),
            release="R3", scope=SCOPE, policy=POLICY, release_key=fx.release_key,
        )
        assert len(plan.rejections) == 1
        assert plan.rejections[0].reason == "key_owned_by_other_scope"
        assert plan.rejections[0].business_key == ("shared",)
        assert plan.to_close == plan.to_open == plan.to_replace_draft == ()
    finally:
        c.close()


def test_writer_b_release_vocabulary_never_reaches_writer_a_release_key():
    """N1: scope-filtered ``seen`` keeps B's foreign release vocabulary out of A's plan.

    writer_b's ``valid_from`` (``'v5'``) is not parseable by ``fx.release_key``
    (it expects ``R<int>``). Without scope-filtering the release-ordering
    query, this would raise inside A's own R3 plan.
    """
    c = duckdb.connect(":memory:")
    try:
        c.execute(
            "CREATE TABLE h (entity_id VARCHAR, label VARCHAR, source VARCHAR, "
            "valid_from VARCHAR, valid_to VARCHAR)"
        )
        c.execute("INSERT INTO h VALUES ('e1', 'a1', 'writer_a', 'R1', NULL)")
        c.execute("INSERT INTO h VALUES ('w1', 'b1', 'writer_b', 'v5', NULL)")
        c.execute("CREATE TEMP TABLE inc AS SELECT entity_id, label, source FROM h LIMIT 0")
        c.execute("INSERT INTO inc VALUES ('e1', 'a1', 'writer_a')")

        plan = plan_scd2_release(
            c, c.table("h"), c.table("inc"),
            release="R3", scope=SCOPE, policy=POLICY, release_key=fx.release_key,
        )
        assert plan.rejections == ()
        assert plan.unchanged == 1
    finally:
        c.close()
