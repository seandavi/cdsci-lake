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

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import duckdb


@dataclass(frozen=True)
class CompleteScope:
    """A SQL predicate over columns present on both relations, e.g. ``"source = 'omicidx'"``.

    Says incoming rows are a complete accounting of this writer's declared
    scope — a current row failing this predicate belongs to another writer
    and must never be touched by this plan.

    SQL's three-valued logic makes this null-safe already: a current row with
    ``NULL`` in a predicate column makes ``predicate_sql`` evaluate to
    ``NULL`` (not ``TRUE``), so ``WHERE (predicate_sql)`` excludes it from
    ``current_in_scope_keys`` and it is never a retirement candidate — a
    ``NULL`` scope column is never retired, by construction, not by an
    explicit ``IS NOT NULL`` guard.
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
    release_key: Callable[[str], Any],
    published_releases: frozenset[str] = frozenset(),
) -> HistoryPlan:
    """Plan an ``scd2_release`` publish of ``incoming`` as release ``release``.

    ``current_history`` and ``incoming`` must be relations on ``con``. ``release_key`` maps a
    release identifier to a comparable, ordered key (e.g. ``int``, a ``(year, month)`` tuple) --
    release identifiers are opaque to this module and must never be compared as raw strings
    (``"R10" < "R9"`` and ``"2026.10" < "2026.9"`` are both wrong under a string ordering).

    Any rejection (already-published release, out-of-order release, duplicate incoming key,
    incoming row outside declared scope) makes the whole plan a no-op: rejected keys must never
    flow into retirement, and a rejected release must never partially write.

    Replanning the *same* release is legal on its own (same-release correction, §11.4) --
    ``out_of_order_release`` only rejects a release strictly older than the latest seen key.
    ``published_releases`` is the complementary guard: once a release is published, replanning it
    again (same key, not older) is rejected there instead.
    """
    if release in published_releases:
        return HistoryPlan(
            rejections=(
                Rejection("release_already_published", None, f"{release!r} is already published"),
            )
        )

    current_history.create_view("_scd2_current", replace=True)
    incoming.create_view("_scd2_incoming", replace=True)
    key_cols = ", ".join(policy.business_key)

    seen = _rows(
        con,
        f"SELECT {policy.valid_from} AS x FROM _scd2_current "
        f"WHERE {policy.valid_from} IS NOT NULL "
        f"UNION ALL SELECT {policy.valid_to} AS x FROM _scd2_current "
        f"WHERE {policy.valid_to} IS NOT NULL",
    )
    seen_keys = [release_key(r["x"]) for r in seen]
    max_seen_key = max(seen_keys) if seen_keys else None
    release_idx = release_key(release)
    if max_seen_key is not None and release_idx < max_seen_key:
        return HistoryPlan(
            rejections=(
                Rejection(
                    "out_of_order_release",
                    None,
                    f"{release!r} (key {release_idx!r}) is older than latest seen key "
                    f"{max_seen_key!r}",
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

    in_scope_keys = {
        _key(r, policy.business_key)
        for r in _rows(con, f"SELECT * FROM _scd2_incoming WHERE ({scope.predicate_sql})")
    }
    all_incoming: dict[tuple[Any, ...], dict[str, Any]] = {}
    for row in _rows(con, "SELECT * FROM _scd2_incoming"):
        k = _key(row, policy.business_key)
        all_incoming[k] = row
        if k not in dup_keys and k not in in_scope_keys:
            rejections.append(Rejection("outside_declared_scope", k))

    if rejections:
        return HistoryPlan(rejections=tuple(rejections))

    valid_incoming = all_incoming

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
        to_close.append(
            {
                **{c: current[c] for c in policy.business_key},
                policy.valid_from: current[policy.valid_from],
                policy.valid_to: release,
            }
        )
        to_open.append({**row, policy.valid_from: release, policy.valid_to: None})
        changed += 1

    for k, current in current_open.items():
        if k not in current_in_scope_keys or k in valid_incoming:
            continue
        if current[policy.valid_from] == release:
            continue  # same-release draft, nothing incoming: leave as-is (not a required scenario)
        to_close.append(
            {
                **{c: current[c] for c in policy.business_key},
                policy.valid_from: current[policy.valid_from],
                policy.valid_to: release,
            }
        )
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
