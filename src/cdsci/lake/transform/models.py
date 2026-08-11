"""``cdsci.lake.transform.models`` — SQL-file model discovery (ADR-0015 decision 1).

A model is one ``*.sql`` file. Its path relative to the models root *is* its
target table: ``ref/id_crosswalk.sql`` -> ``ref.id_crosswalk``. The file body is
the ``SELECT`` the runner wraps in ``CREATE OR REPLACE TABLE {target} AS (...)``
— no macro DSL, no per-model config file, so ``sqlglot`` can parse the body
directly for :mod:`.graph` and :mod:`.lineage`.

Header directives (anywhere in the file, one per line), all optional:

* ``-- description: ...`` / ``-- license: ...`` — feed the table/view comment
  :func:`cdsci.lake.ops.run` stamps on success. Deliberately not inferred from
  the target's schema — a schema doesn't reliably name one owning license
  (``ref.id_crosswalk`` merges icite/pmc/openalex/reporter/ctgov, each under
  its own license; ``ref``'s own registered EL source, ``census_geo``, has
  nothing to do with any of them) — so an unset license is a loud placeholder,
  not a silently wrong guess.
* ``-- column <name>: ...`` — a per-column comment, stamped via
  ``COMMENT ON COLUMN`` (table-materialized models only — DuckDB rejects
  column comments on views outright).
* ``-- materialized: view`` — ``CREATE OR REPLACE VIEW`` instead of the
  default ``TABLE``. A view has no data of its own, so DuckLake can't
  copy-on-write it the way an unchanged table re-run skips a new snapshot —
  every ``run`` of a view model is a "changed" snapshot, always. Right
  tradeoff for a cheap, always-fresh derivation; wrong one for anything
  ``ops.run``'s idempotent/changed signal should track meaningfully.

A sibling ``<name>.test.sql`` (same directory, filename match on the model's
own stem) holds named assertions, each a query that must return **zero rows**
to pass — same shape as bioc-on-ice's own ``merge.py`` integrity checks. No
test file is a legitimate, common case (not every model has one natural
business key to assert), not an error.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

_DIRECTIVE = re.compile(r"^--\s*(description|license|materialized):\s*(.+?)\s*$", re.MULTILINE)
_COLUMN_DIRECTIVE = re.compile(r"^--\s*column\s+(\w+):\s*(.+?)\s*$", re.MULTILINE)
_TEST_BLOCK = re.compile(r"^--\s*test:\s*(.+?)\s*$", re.MULTILINE)


_UNSET_LICENSE = "UNSPECIFIED -- no `-- license:` directive in the model file, verify"
_UNSET_DESCRIPTION = "no `-- description:` directive"


@dataclass(frozen=True)
class Model:
    """One SQL-file transform model, keyed by its ``target`` table name.

    ``description``/``license`` default to loud-but-generic placeholders,
    not silence -- ``load_models()`` overrides both with a target-specific
    message when reading a real file with no directive (see ``_directives``);
    these class-level defaults only matter for direct construction (tests,
    ad hoc scripts), which shouldn't have to restate boilerplate to build a
    ``Model`` for something that isn't going to be commented into the catalog.
    """

    target: str  # "ref.id_crosswalk" -- catalog-less; the runner prefixes LAKE
    sql: str  # the file body, stripped -- a SELECT, no leading CREATE
    path: Path
    description: str = _UNSET_DESCRIPTION
    license: str = _UNSET_LICENSE
    materialized: Literal["table", "view"] = "table"
    column_comments: dict[str, str] = field(default_factory=dict)
    tests: dict[str, str] = field(default_factory=dict)


def _directives(sql: str, target: str) -> tuple[str, str, Literal["table", "view"]]:
    found = dict(_DIRECTIVE.findall(sql))
    materialized = found.get("materialized", "table")
    if materialized not in ("table", "view"):
        raise ValueError(
            f"{target}: -- materialized: {materialized!r} unrecognized (want 'table' or 'view')"
        )
    return (
        found.get("description", f"cdsci-lake transform model: {target}"),
        found.get("license", _UNSET_LICENSE),
        materialized,  # type: ignore[return-value]
    )


def _parse_tests(sql: str, target: str) -> dict[str, str]:
    """``-- test: <name>`` markers split the file into named zero-rows-to-pass queries."""
    marks = list(_TEST_BLOCK.finditer(sql))
    tests: dict[str, str] = {}
    for i, m in enumerate(marks):
        name = m.group(1)
        end = marks[i + 1].start() if i + 1 < len(marks) else len(sql)
        query = sql[m.end() : end].strip().rstrip(";").strip()
        if not query:
            raise ValueError(f"{target}: test {name!r} has no query")
        if name in tests:
            raise ValueError(f"{target}: duplicate test name {name!r}")
        tests[name] = query
    return tests


def load_models(models_dir: Path | str) -> dict[str, Model]:
    """Load every ``*.sql`` file under ``models_dir`` into a ``{target: Model}`` map.

    Raises on a duplicate target (two files mapping to the same table) or an
    empty file — both are authoring mistakes, not runtime conditions to tolerate.
    A ``*.test.sql`` file is never itself a model — it's loaded as the
    matching model's :attr:`Model.tests`, not discovered as its own target.
    """
    root = Path(models_dir)
    models: dict[str, Model] = {}
    for path in sorted(root.rglob("*.sql")):
        if path.name.endswith(".test.sql"):
            continue
        target = ".".join(path.relative_to(root).with_suffix("").parts)
        if target in models:
            raise ValueError(
                f"duplicate transform model target {target!r}: "
                f"{models[target].path} and {path}"
            )
        sql = path.read_text().strip()
        if not sql:
            raise ValueError(f"empty transform model: {path}")
        description, license_, materialized = _directives(sql, target)
        column_comments = dict(_COLUMN_DIRECTIVE.findall(sql))

        test_path = path.with_suffix("").with_suffix(".test.sql")
        tests = _parse_tests(test_path.read_text(), target) if test_path.exists() else {}

        models[target] = Model(
            target=target, sql=sql, path=path, description=description, license=license_,
            materialized=materialized, column_comments=column_comments, tests=tests,
        )
    return models
