"""``cdsci.lake.transform.models`` — SQL-file model discovery (ADR-0015 decision 1).

A model is one ``*.sql`` file. Its path relative to the models root *is* its
target table: ``ref/id_crosswalk.sql`` -> ``ref.id_crosswalk``. The file body is
the ``SELECT`` the runner wraps in ``CREATE OR REPLACE TABLE {target} AS (...)``
— no macro DSL, no per-model config file, so ``sqlglot`` can parse the body
directly for :mod:`.graph` and :mod:`.lineage`.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Model:
    """One SQL-file transform model, keyed by its ``target`` table name."""

    target: str  # "ref.id_crosswalk" -- catalog-less; the runner prefixes LAKE
    sql: str  # the file body, stripped -- a SELECT, no leading CREATE
    path: Path


def load_models(models_dir: Path | str) -> dict[str, Model]:
    """Load every ``*.sql`` file under ``models_dir`` into a ``{target: Model}`` map.

    Raises on a duplicate target (two files mapping to the same table) or an
    empty file — both are authoring mistakes, not runtime conditions to tolerate.
    """
    root = Path(models_dir)
    models: dict[str, Model] = {}
    for path in sorted(root.rglob("*.sql")):
        target = ".".join(path.relative_to(root).with_suffix("").parts)
        if target in models:
            raise ValueError(
                f"duplicate transform model target {target!r}: "
                f"{models[target].path} and {path}"
            )
        sql = path.read_text().strip()
        if not sql:
            raise ValueError(f"empty transform model: {path}")
        models[target] = Model(target=target, sql=sql, path=path)
    return models
