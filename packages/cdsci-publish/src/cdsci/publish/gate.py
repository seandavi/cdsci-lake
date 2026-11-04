"""cdsci.publish.gate (ADR-0027 step 4c): a release bundle against the previous published
release, the check a product runs on a freshly built bundle before `upload` / `hf_push`.

The best of bioc-on-ice's and cancer-on-ice's gates. Reads the bundle's Parquet and
manifest.json, and the reference's manifest.json only (no reference Parquet). Required:
every table's manifest row count and columns match its Parquet; against the reference, no
table is missing, columns and types are unchanged (unless the owner allows the change, the
change recorded), row counts stay within bounds; the keys a product declares are not null
and unique; the references it declares are covered. README queries run as optional checks.
Product rules (domain checks, contract policy) come in through `extra_checks`.

A pass writes gate.json into the bundle, bound to its manifest.json by sha256. `upload` and
`hf_push` carry it as an `extra_files` entry; a product calls `check_proof` before either.
Re-running the gate later is no substitute: once uploaded, latest/ is the bundle itself,
and the gate refuses a reference that is not older.
"""

import hashlib
import json
import re
import urllib.error
import urllib.request
from collections.abc import Callable, Collection, Iterable, Mapping, Sequence
from datetime import UTC, date, datetime
from pathlib import Path, PurePosixPath
from typing import NamedTuple

import duckdb

from .bundle import sql_literal

GATE_FILE = "gate.json"
# Against the reference, a table's row count may shrink to `low` times it or grow to `high`
# times it: bioc-on-ice's bounds (cdsci.lake.contracts.DEFAULT_ROW_COUNT_BOUNDS, which every
# bioc table used). cancer-on-ice passes (0.8, 3.0) with growth_required=False.
ROW_BOUNDS = (0.9, 1.5)
# Cloudflare answers urllib's default User-Agent with 403 (error 1010) on cancerdatasci.org.
USER_AGENT = "cdsci-publish gate (+https://github.com/seandavi/cdsci-lake)"


class Check(NamedTuple):
    """One gate check. A failed required check fails the gate; a failed optional one warns.
    Table checks are named `<schema>.<table>.<check>`; release-wide ones have no dot."""

    name: str
    passed: bool
    required: bool = True
    detail: str = ""


ExtraChecks = Callable[[duckdb.DuckDBPyConnection, dict, dict | None], Iterable[Check]]


def _q(identifier):
    return '"' + identifier.replace('"', '""') + '"'


def _name(t):
    return f"{t['schema']}.{t['table']}"


def _view(name):
    schema, table = name.split(".", 1)
    return f"{_q(schema)}.{_q(table)}"


def _columns(t):
    return [(c["name"], c["type"]) for c in t["columns"]]


def readme_queries(path: Path, fence="sql"):
    """(line, sql) for each ```<fence> block in `path` that is a query (SELECT / WITH); none
    for a missing file (an installed wheel has no README). bioc-on-ice marks its bundle
    queries ```sql, cancer-on-ice ```sql bundle."""
    if not path.exists():
        return []
    text = path.read_text()
    return [(text[:m.start()].count("\n") + 2, m[1].strip().rstrip(";"))
            for m in re.finditer(rf"```{re.escape(fence)}\n(.*?)```", text, re.DOTALL)
            if re.match(r"\s*(SELECT|WITH)\b", m[1], re.IGNORECASE)]


def bundle_con(bundle: Path, tables, catalog: str | None = None):
    """A DuckDB connection with the bundle's local Parquet as `<schema>.<table>` views, in
    catalog `catalog` when given and made current, so `<catalog>.<schema>.<table>` (the
    names bioc-on-ice's README uses) resolves as well."""
    con = duckdb.connect()
    if catalog:
        con.execute(f"ATTACH ':memory:' AS {_q(catalog)}")
        con.execute(f"USE {_q(catalog)}")
    for s in sorted({t["schema"] for t in tables}):
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {_q(s)}")
    for t in tables:
        p = PurePosixPath(t["path"])
        if p.is_absolute() or ".." in p.parts:
            raise ValueError(f"{_name(t)}: path must be relative to the bundle: {t['path']!r}")
        con.execute(f"CREATE VIEW {_view(_name(t))} AS "
                    f"SELECT * FROM read_parquet({sql_literal(str(bundle / t['path']))})")
    return con


def manifest_sha256(bundle: Path) -> str:
    return hashlib.sha256((bundle / "manifest.json").read_bytes()).hexdigest()


def check_proof(bundle: Path):
    """The bundle's gate.json, which must certify exactly its manifest.json; SystemExit if
    there is none or it is for another manifest. For a product to call before `upload` /
    `hf_push` (gate.json binds manifest.json, not the Parquet: those two re-hash the files)."""
    try:
        proof = json.loads((bundle / GATE_FILE).read_text())
    except FileNotFoundError:
        raise SystemExit(f"{bundle}: no {GATE_FILE}; only a bundle that passed the gate "
                         "is uploaded") from None
    if proof.get("manifest_sha256") != manifest_sha256(bundle):
        raise SystemExit(f"{bundle}/{GATE_FILE} is for another manifest.json; "
                         "re-run the gate on this bundle")
    return proof


def reference_manifest(reference, user_agent=USER_AGENT):
    """The manifest.json of `reference` (a release directory, or its base URL), or None when
    no release is published there (HTTP 404: a first release). Anything else, including a
    200 that is not a manifest, raises OSError or ValueError."""
    ref = str(reference).rstrip("/")
    if "://" not in ref:
        text = (Path(ref) / "manifest.json").read_text()
    else:
        req = urllib.request.Request(f"{ref}/manifest.json", headers={"User-Agent": user_agent})
        try:
            with urllib.request.urlopen(req) as r:
                text = r.read().decode()
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None
            raise
    m = json.loads(text)
    if not (isinstance(m, dict) and "release" in m and isinstance(m.get("tables"), list)):
        raise ValueError("not a manifest")
    return m


def bundle_checks(con, tables, queries=()):
    """Checks on the bundle alone. Required: the bundle has tables, and each table's manifest
    row_count, columns and types match its Parquet. Optional: each `(label, sql)` query runs."""
    checks = [Check("candidate_has_tables", bool(tables), detail=f"{len(tables)} tables")]
    for t in tables:
        name, view = _name(t), _view(_name(t))
        n = con.execute(f"SELECT count(*) FROM {view}").fetchone()[0]
        columns = [tuple(c[:2]) for c in con.execute(f"DESCRIBE {view}").fetchall()]
        declared = _columns(t)
        checks += [Check(f"{name}.manifest_row_count", n == t["row_count"],
                         detail=f"parquet {n}, manifest {t['row_count']}"),
                   Check(f"{name}.manifest_columns", columns == declared,
                         detail="" if columns == declared else "manifest and Parquet differ")]
    for label, sql in queries:
        try:
            n, detail = len(con.execute(sql).fetchall()), ""
        except duckdb.Error as e:  # type name only: the message can carry a local path
            n, detail = None, type(e).__name__
        checks.append(Check(f"readme_query_line_{label}", n is not None, False,
                            detail or f"{n} rows"))
    return checks


def column_diff(before, after):
    """The exact change from column list `before` to `after`, of (name, type) pairs: a
    rename is a removal and an addition."""
    old, new = dict(before), dict(after)
    return {"added": {n: t for n, t in after if n not in old},
            "removed": {n: t for n, t in before if n not in new},
            "retyped": {n: {"from": t, "to": new[n]} for n, t in before
                        if n in new and new[n] != t},
            "reordered": [n for n, _ in before if n in new] != [n for n, _ in after if n in old]}


def describe_diff(diff):
    parts = [f"{k} " + ", ".join(f"{n} {t}" for n, t in diff[k].items())
             for k in ("added", "removed") if diff[k]]
    if diff["retyped"]:
        parts.append("retyped " + ", ".join(f"{n} {t['from']} -> {t['to']}"
                                            for n, t in diff["retyped"].items()))
    return "; ".join(parts + ["reordered"] * diff["reordered"]) or "none"


def reference_checks(tables, ref_tables, *, row_bounds=ROW_BOUNDS, growth_required=True,
                     allow_dropped: Mapping[str, Collection[str]] | None = None,
                     allow_column_change: Collection[str] = ()):
    """Checks per reference table against the bundle's, from the two manifests: present;
    columns and types unchanged, where a table in `allow_dropped` may lose exactly the
    columns named for it and nothing else, and one in `allow_column_change` may change in
    any way (both owner-approved; the change is the detail); row count not below `low`
    times the reference's; not above `high` times it, a warning only when
    `growth_required` is False (whole new vintages are legitimate growth). A reference
    count of 0 bounds nothing above."""
    low, high = row_bounds
    have = {_name(t): t for t in tables}
    allow_dropped = allow_dropped or {}
    checks = []
    for r in ref_tables:
        name = _name(r)
        t = have.get(name)
        checks.append(Check(f"{name}.present", t is not None,
                            detail="" if t else "in the previous release, missing from this one"))
        if t is None:
            continue
        before, after = _columns(r), _columns(t)
        if before == after:
            passed, detail = True, ""
        elif name in allow_column_change:
            passed = True
            detail = "column change allowed: " + describe_diff(column_diff(before, after))
        elif name in allow_dropped and [c for c in before
                                        if c[0] not in allow_dropped[name]] == after:
            passed = True
            detail = "schema change allowed: dropped " + ", ".join(
                n for n, _ in before if n in allow_dropped[name])
        else:
            passed, detail = False, "differs: " + describe_diff(column_diff(before, after))
        checks.append(Check(f"{name}.columns_unchanged", passed, detail=detail))
        n, n_ref = t["row_count"], r["row_count"]
        detail = f"{n} rows, reference {n_ref}, bounds ({low}, {high})"
        checks.append(Check(f"{name}.row_count_bounds", n >= low * n_ref, detail=detail))
        checks.append(Check(f"{name}.row_count_growth", not n_ref or n <= high * n_ref,
                            growth_required, detail))
    return checks


def _count_is_zero(con, name, sql, what):
    try:
        n = con.execute(sql).fetchone()[0]
    except duckdb.Error as e:  # type name only: the message can carry a local path
        return Check(name, False, detail=type(e).__name__)
    return Check(name, n == 0, detail=f"{what}: {n}")


def key_checks(con, name, key: Sequence[str]):
    """The `key` columns of table `name`: none NULL, and no two rows alike on them."""
    view, cols = _view(name), [_q(k) for k in key]
    return [_count_is_zero(con, f"{name}.key_not_null",
                           f"SELECT count(*) FROM {view} WHERE "
                           + " OR ".join(f"{k} IS NULL" for k in cols),
                           "rows with a null key column"),
            _count_is_zero(con, f"{name}.key_unique",
                           f"SELECT count(*) FROM (SELECT 1 FROM {view} "
                           f"GROUP BY {', '.join(cols)} HAVING count(*) > 1)",
                           "duplicated keys")]


def reference_covered(con, name, column, target, names):
    """Every non-null value of `name.column` is in `target` (`schema.table.column`), a table
    of this bundle (`names`)."""
    target_table, _, target_column = target.rpartition(".")
    check = f"{name}.{column}.references_covered"
    if target_table not in names:
        return Check(check, False, detail=f"target table {target_table} not in this release")
    return _count_is_zero(
        con, check,
        f"SELECT count(DISTINCT s.{_q(column)}) FROM {_view(name)} s "
        f"ANTI JOIN {_view(target_table)} t ON s.{_q(column)} = t.{_q(target_column)} "
        f"WHERE s.{_q(column)} IS NOT NULL",
        f"distinct values missing from {target}")


def print_report(checks):
    by_table: dict[str, list] = {}
    for c in checks:
        parts = c.name.split(".", 2)
        table, check = (".".join(parts[:2]), parts[2]) if len(parts) == 3 else ("(release)", c.name)
        by_table.setdefault(table, []).append((check, c))
    for table, rows in by_table.items():
        print(table)
        for check, c in rows:
            status = "ok" if c.passed else "FAIL" if c.required else "warn"
            print(f"  {status:4}  {check}" + (f"  ({c.detail})" if c.detail else ""))


def gate(bundle: Path, reference, *, queries=(), catalog: str | None = None,
         keys: Mapping[str, Sequence[str]] | None = None,
         references: Mapping[str, Mapping[str, str]] | None = None,
         row_bounds=ROW_BOUNDS, growth_required=True,
         allow_dropped: Mapping[str, Collection[str]] | None = None,
         allow_column_change: Collection[str] = (),
         extra_checks: ExtraChecks | None = None,
         user_agent=USER_AGENT) -> tuple[bool, list[Check]]:
    """Print the gate report for `bundle` against `reference` (a release directory or base
    URL; None for the bundle checks only, a first release); `(passed, checks)`, and on a
    pass gate.json is in the bundle. The reference must be older than the bundle, and both
    release ids YYYY-MM-DD dates.

    `queries`: `(label, sql)` to run on the bundle as optional checks (`readme_queries`).
    `catalog`: name the bundle's views live under. `keys`: `{"schema.table": (columns,)}`
    checked not null and unique; `references`: `{"schema.table": {column:
    "schema.table.column"}}` checked covered; tables not in the bundle are ignored.
    `row_bounds`, `growth_required`, `allow_dropped`, `allow_column_change`: see
    `reference_checks`. `extra_checks(con, manifest, reference_manifest_or_None)`: the
    product's own checks.
    """
    bundle = Path(bundle)
    (bundle / GATE_FILE).unlink(missing_ok=True)
    # The bytes read here are the bytes parsed and the bytes hashed into gate.json.
    manifest_bytes = (bundle / "manifest.json").read_bytes()
    manifest = json.loads(manifest_bytes)
    ref = None
    if reference is not None:
        try:
            ref = reference_manifest(reference, user_agent)
        except (OSError, ValueError) as e:
            print(f"FAIL: reference {reference}: no readable manifest.json ({type(e).__name__})")
            return False, []
    if ref is None:
        print(f"release {manifest['release']}: bundle checks only, "
              + ("no reference given" if reference is None
                 else f"no published release at {reference}"))
    else:
        print(f"release {manifest['release']} against {reference} (release {ref['release']})")
        try:
            older = date.fromisoformat(ref["release"]) < date.fromisoformat(manifest["release"])
        except (TypeError, ValueError):
            print(f"FAIL: release ids must be YYYY-MM-DD dates: reference {ref['release']!r}, "
                  f"release {manifest['release']!r}")
            return False, []
        # latest/ after an upload is the bundle itself.
        if not older:
            print(f"FAIL: reference release {ref['release']} is not older than "
                  f"{manifest['release']}; pass an earlier release's folder as the reference")
            return False, []

    tables = manifest["tables"]
    names = {_name(t) for t in tables}
    con = bundle_con(bundle, tables, catalog)
    try:
        checks = bundle_checks(con, tables, queries)
        if ref is not None:
            # An empty reference would make every table look new, and nothing missing.
            checks.append(Check("reference_has_tables", bool(ref["tables"]), detail=(
                "" if ref["tables"] else "reference manifest lists no tables")))
            checks += reference_checks(tables, ref["tables"], row_bounds=row_bounds,
                                       growth_required=growth_required,
                                       allow_dropped=allow_dropped,
                                       allow_column_change=allow_column_change)
        for name, key in (keys or {}).items():
            if name in names and key:
                checks += key_checks(con, name, key)
        for name, refs in (references or {}).items():
            if name in names:
                checks += [reference_covered(con, name, col, target, names)
                           for col, target in refs.items()]
        if extra_checks is not None:
            checks += list(extra_checks(con, manifest, ref))
    finally:
        con.close()

    before = {_name(t): _columns(t) for t in ref["tables"]} if ref else {}
    after = {_name(t): _columns(t) for t in tables}
    schema_changes = {t: [n for n, _ in before.get(t, [])
                          if n in cols and n not in dict(after.get(t, []))]
                      for t, cols in sorted((allow_dropped or {}).items())}
    column_changes = {t: column_diff(before.get(t, []), after.get(t, []))
                      for t in sorted(allow_column_change)}
    if schema_changes:
        print("schema change allowed (owner approval): " + "; ".join(
            f"{t} drops {', '.join(cols) or 'nothing'}" for t, cols in schema_changes.items()))
    if column_changes:
        print("column change allowed (owner approval): " + "; ".join(
            f"{t}: {describe_diff(d)}" for t, d in column_changes.items()))
    print_report(checks)
    failed = sum(c.required and not c.passed for c in checks)
    print(f"FAIL: {failed} required check(s) failed" if failed else "PASS")
    if failed:
        return False, checks
    if (bundle / "manifest.json").read_bytes() != manifest_bytes:
        print("FAIL: manifest.json changed while the gate ran; not certifying it")
        return False, checks
    (bundle / GATE_FILE).write_text(json.dumps({
        "release": manifest["release"],
        "manifest_sha256": hashlib.sha256(manifest_bytes).hexdigest(),
        "reference": None if reference is None else str(reference),
        "reference_release": ref and ref["release"],
        "schema_changes": schema_changes, "column_changes": column_changes,
        "passed_at": datetime.now(UTC).isoformat(timespec="seconds")}, indent=2) + "\n")
    return True, checks
