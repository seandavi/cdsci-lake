"""``cdsci.lake.history`` — pure ``scd2_release`` planning (design §6.4/§11.4).

Computes which rows to close, open, or reject for one release of an
``scd2_release`` table. Pure DuckDB-SQL computation over two relations already
registered on ``con`` — it makes no writes to any lake; the caller applies the
returned ``HistoryPlan``.

``CompleteScope.predicate_sql`` is a raw SQL boolean expression supplied by
the calling producer contract (trusted internal code — the domain repository
that owns this table's scope), never derived from untrusted/external input;
it is not exposed as a public gateway parameter (AGENTS.md interpolation
rule).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import duckdb


@dataclass(frozen=True)
class CompleteScope:
    """A SQL predicate over columns present on both relations, e.g. ``"source = 'omicidx'"``.

    Says incoming rows are a complete accounting of this writer's declared
    scope — a current row failing this predicate belongs to another writer
    and must never be touched by this plan.
    """

    predicate_sql: str


@dataclass(frozen=True)
class SCD2Policy:
    business_key: tuple[str, ...]
    valid_from: str = "valid_from"
    valid_to: str = "valid_to"
    tracked_columns: tuple[str, ...] | None = None


@dataclass(frozen=True)
class Rejection:
    reason: str
    business_key: tuple[Any, ...] | None
    detail: str = ""


@dataclass(frozen=True)
class HistoryPlan:
    to_close: tuple[dict[str, Any], ...] = ()
    to_open: tuple[dict[str, Any], ...] = ()
    to_replace_draft: tuple[dict[str, Any], ...] = ()
    inserted: int = 0
    changed: int = 0
    retired: int = 0
    reopened: int = 0
    unchanged: int = 0
    same_release_corrections: int = 0
    rejections: tuple[Rejection, ...] = ()


def _rows(con: duckdb.DuckDBPyConnection, sql: str) -> list[dict[str, Any]]:
    cur = con.execute(sql)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row, strict=True)) for row in cur.fetchall()]


def _key(row: dict[str, Any], business_key: tuple[str, ...]) -> tuple[Any, ...]:
    return tuple(row[c] for c in business_key)


def plan_scd2_release(
    con: duckdb.DuckDBPyConnection,
    current_history: duckdb.DuckDBPyRelation,
    incoming: duckdb.DuckDBPyRelation,
    *,
    release: str,
    scope: CompleteScope,
    policy: SCD2Policy,
) -> HistoryPlan:
    """Plan an ``scd2_release`` publish of ``incoming`` as release ``release``.

    ``current_history`` and ``incoming`` must be relations on ``con``.
    """
    current_history.create_view("_scd2_current", replace=True)
    incoming.create_view("_scd2_incoming", replace=True)
    key_cols = ", ".join(policy.business_key)

    max_seen = con.execute(
        f"SELECT max(x) FROM ("
        f"SELECT {policy.valid_from} AS x FROM _scd2_current "
        f"UNION ALL SELECT {policy.valid_to} AS x FROM _scd2_current)"
    ).fetchone()[0]
    if max_seen is not None and release < max_seen:
        return HistoryPlan(
            rejections=(
                Rejection(
                    "out_of_order_release",
                    None,
                    f"{release!r} is older than latest seen {max_seen!r}",
                ),
            )
        )

    dup_keys = {
        _key(r, policy.business_key)
        for r in _rows(
            con, f"SELECT {key_cols} FROM _scd2_incoming GROUP BY {key_cols} HAVING count(*) > 1"
        )
    }
    rejections: list[Rejection] = [Rejection("duplicate_incoming_key", k) for k in sorted(dup_keys)]

    valid_incoming: dict[tuple[Any, ...], dict[str, Any]] = {}
    for row in _rows(con, "SELECT * FROM _scd2_incoming"):
        k = _key(row, policy.business_key)
        if k not in dup_keys:
            valid_incoming[k] = row

    in_scope_keys = {
        _key(r, policy.business_key)
        for r in _rows(con, f"SELECT * FROM _scd2_incoming WHERE ({scope.predicate_sql})")
    }
    for k in list(valid_incoming):
        if k not in in_scope_keys:
            rejections.append(Rejection("outside_declared_scope", k))
            del valid_incoming[k]

    tracked = policy.tracked_columns or tuple(
        c for c in incoming.columns if c not in policy.business_key
    )
    current_open = {
        _key(r, policy.business_key): r
        for r in _rows(con, f"SELECT * FROM _scd2_current WHERE {policy.valid_to} IS NULL")
    }
    current_closed_keys = {
        _key(r, policy.business_key)
        for r in _rows(con, f"SELECT * FROM _scd2_current WHERE {policy.valid_to} IS NOT NULL")
    }
    current_in_scope_keys = {
        _key(r, policy.business_key)
        for r in _rows(con, f"SELECT * FROM _scd2_current WHERE ({scope.predicate_sql})")
    }

    to_close: list[dict[str, Any]] = []
    to_open: list[dict[str, Any]] = []
    to_replace_draft: list[dict[str, Any]] = []
    inserted = changed = retired = reopened = unchanged = same_release_corrections = 0

    for k, row in valid_incoming.items():
        current = current_open.get(k)
        if current is None:
            to_open.append({**row, policy.valid_from: release, policy.valid_to: None})
            if k in current_closed_keys:
                reopened += 1
            else:
                inserted += 1
            continue
        if current[policy.valid_from] == release:
            to_replace_draft.append({**row, policy.valid_from: release, policy.valid_to: None})
            same_release_corrections += 1
            continue
        if all(current.get(c) == row.get(c) for c in tracked):
            unchanged += 1
            continue
        to_close.append({**{c: current[c] for c in policy.business_key}, policy.valid_to: release})
        to_open.append({**row, policy.valid_from: release, policy.valid_to: None})
        changed += 1

    for k, current in current_open.items():
        if k not in current_in_scope_keys or k in valid_incoming:
            continue
        if current[policy.valid_from] == release:
            continue  # same-release draft, nothing incoming: leave as-is (not a required scenario)
        to_close.append({**{c: current[c] for c in policy.business_key}, policy.valid_to: release})
        retired += 1

    return HistoryPlan(
        to_close=tuple(to_close),
        to_open=tuple(to_open),
        to_replace_draft=tuple(to_replace_draft),
        inserted=inserted,
        changed=changed,
        retired=retired,
        reopened=reopened,
        unchanged=unchanged,
        same_release_corrections=same_release_corrections,
        rejections=tuple(rejections),
    )
