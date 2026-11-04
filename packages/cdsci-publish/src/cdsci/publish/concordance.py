"""Concordance check (ADR-0027): compare two release manifests on what a user sees.

Row counts and schema are good enough (owner decision): byte identity, sha256, bytes,
timestamps and git_sha are build-time fields, not compared.
"""


def _by_name(tables):
    return {f"{t['schema']}.{t['table']}": t for t in tables}


def _columns(table):
    return [(c["name"], c["type"]) for c in table["columns"]]


def _column_docs(table):
    return {c["name"]: c.get("doc") for c in table["columns"]}


def concordance(old_manifest: dict, new_manifest: dict) -> list[str]:
    """Human-readable differences between two manifests: tables, row counts, column
    names/types (ordered), and table/column docs. Empty when the two manifests agree
    on everything this function compares."""
    old = _by_name(old_manifest.get("tables", []))
    new = _by_name(new_manifest.get("tables", []))
    diffs = []
    for name in sorted(old.keys() - new.keys()):
        diffs.append(f"{name}: dropped")
    for name in sorted(new.keys() - old.keys()):
        diffs.append(f"{name}: added")
    for name in sorted(old.keys() & new.keys()):
        o, n = old[name], new[name]
        if o["row_count"] != n["row_count"]:
            diffs.append(f"{name}: row_count {o['row_count']} -> {n['row_count']}")
        if _columns(o) != _columns(n):
            diffs.append(f"{name}: columns {_columns(o)} -> {_columns(n)}")
        if o.get("comment") != n.get("comment"):
            diffs.append(f"{name}: comment {o.get('comment')!r} -> {n.get('comment')!r}")
        if _column_docs(o) != _column_docs(n):
            diffs.append(f"{name}: column docs changed")
    return diffs
