"""``cdsci.lake.contracts_render`` — render/lint ``cdsci.lake.contracts`` objects (design §6.1).

The domain repository constructs ``TableContract``/``DatasetContract`` instances
(``cdsci.lake.contracts``); this module only renders and lints them. No biological or
cancer meaning lives here — see AGENTS.md "Domain meaning stays domain-local". Every
function is a pure, deterministic string/dict transform of its input contract: same
contract in, same output out, so a repeat ``build_release`` still produces
byte-identical release trees (cdsci-lake#95 M1 determinism).
"""

from __future__ import annotations

from typing import Any

from .contracts import ColumnContract, DatasetContract, TableContract, TemporalModel

# One-line definitions from design §7 "Temporal standards" -- naming what each model
# means, not restating the enum value.
_TEMPORAL_MODEL_DEFINITION: dict[TemporalModel, str] = {
    TemporalModel.APPEND_IMMUTABLE: (
        "Existing records are never revised; used for immutable event/artifact sources."
    ),
    TemporalModel.UPSERT_LATEST_SNAPSHOT: (
        "One mutable current row per natural key, updated only when tracked values change."
    ),
    TemporalModel.SCD2_RELEASE: (
        "Type-2 history: an attribute change closes the old [valid_from, valid_to) interval "
        "and opens a new one; at most one current row per business key."
    ),
    TemporalModel.SCD2_BITEMPORAL: (
        "Type-2 history with two time axes: effective_from/effective_to are source-world "
        "time, valid_from/valid_to are publication-system time."
    ),
}

# Column names that read as coordinates even without an explicit coordinate_system --
# a generic name-shape heuristic (design §11.9 #2-3 made domain-neutral), not a
# biological vocabulary.
_COORDINATE_LIKE_NAMES = frozenset({"start", "end", "position", "pos", "chrom", "chromosome"})

_TABLE_COLUMN_HEADERS = (
    "Column",
    "Type",
    "Nullable",
    "Description",
    "Identifier Namespace",
    "Units",
    "Coordinate System",
    "Null Meaning",
    "Enum",
)


def _escape_cell(value: str) -> str:
    """Markdown table cells can't contain a literal ``|`` or newline."""
    return value.replace("|", "\\|").replace("\n", " ")


def _column_row(c: ColumnContract) -> str:
    cells = (
        c.name,
        c.arrow_type,
        "Yes" if c.nullable else "No",
        c.description,
        c.identifier_namespace or "",
        c.units or "",
        c.coordinate_system or "",
        c.null_meaning or "",
        ", ".join(c.enum),
    )
    return "| " + " | ".join(_escape_cell(v) for v in cells) + " |"


def render_table_markdown(table: TableContract) -> str:
    """Render one ``TableContract`` as a markdown document: heading, description,
    grain, primary key, named temporal model (with its design §7 one-line
    definition), owner, license, sort/partition order, a column table, and any
    ``examples`` as fenced SQL. Deterministic in the contract's own field order."""
    lines: list[str] = [
        f"# {table.name}",
        "",
        table.description,
        "",
        f"- **Grain:** {table.grain}",
        f"- **Primary key:** {', '.join(table.primary_key)}",
        f"- **Temporal model:** `{table.temporal_model.value}` — "
        f"{_TEMPORAL_MODEL_DEFINITION[table.temporal_model]}",
        f"- **Owner:** {table.owner}",
        f"- **License:** {table.license}",
    ]
    if table.sort_by:
        lines.append(f"- **Sort by:** {', '.join(table.sort_by)}")
    if table.partition_by:
        lines.append(f"- **Partition by:** {', '.join(table.partition_by)}")
    lines.append("")
    lines.append("| " + " | ".join(_TABLE_COLUMN_HEADERS) + " |")
    lines.append("|" + "---|" * len(_TABLE_COLUMN_HEADERS))
    for c in table.columns:
        lines.append(_column_row(c))
    if table.examples:
        lines.append("")
        lines.append("## Examples")
        for example in table.examples:
            lines.append("")
            lines.append("```sql")
            lines.append(example)
            lines.append("```")
    return "\n".join(lines) + "\n"


def render_dataset_markdown(dataset: DatasetContract) -> str:
    """Render a ``DatasetContract`` as a markdown document: title, description,
    publisher, required artifacts, then every table (sorted by name for
    determinism) via :func:`render_table_markdown`."""
    lines: list[str] = [
        f"# {dataset.title}",
        "",
        dataset.description,
        "",
        f"- **Publisher:** {dataset.publisher}",
        f"- **Required artifacts:** {', '.join(sorted(dataset.required_artifacts))}",
        "",
    ]
    for name in sorted(dataset.tables):
        lines.append(render_table_markdown(dataset.tables[name]).rstrip("\n"))
        lines.append("")
    return "\n".join(lines).rstrip("\n") + "\n"


def render_dataset_json(dataset: DatasetContract) -> dict[str, Any]:
    """Render the design §5.1 dataset-metadata shape: ``id``/``title``/``description``/
    ``publisher`` plus ``tables[]``, each table rendered by
    ``TableContract.to_schema_dict()`` (not re-derived here). JSON-serialisable;
    this is what DuckDock's registry embeds."""
    return {
        "id": dataset.id,
        "title": dataset.title,
        "description": dataset.description,
        "publisher": dataset.publisher,
        "tables": [dataset.tables[name].to_schema_dict() for name in sorted(dataset.tables)],
    }


def lint_contract(table: TableContract) -> list[str]:
    """Mechanics-only checks a renderer can surface (design §11.9 #1-3 made generic,
    AGENTS.md "shared modules own mechanics ... not biological or cancer schema
    values"). Returns one human-readable message per problem found; ``[]`` means
    clean. Encodes no biological or cancer vocabulary -- only generic name-shape
    heuristics (``_id`` suffix, a small coordinate-like name set) and structural
    invariants."""
    problems: list[str] = []
    column_names = {c.name for c in table.columns}

    for c in table.columns:
        if not c.description.strip():
            problems.append(f"{table.name}.{c.name}: description is empty")

        looks_like_identifier = c.name.endswith("_id")
        if looks_like_identifier and not c.identifier_namespace:
            problems.append(
                f"{table.name}.{c.name}: identifier column has no identifier_namespace"
            )

        looks_like_coordinate = c.name in _COORDINATE_LIKE_NAMES or c.coordinate_system is not None
        if looks_like_coordinate and not c.coordinate_system:
            problems.append(f"{table.name}.{c.name}: coordinate column has no coordinate_system")

        if c.nullable and not c.null_meaning:
            problems.append(f"{table.name}.{c.name}: nullable column has no null_meaning")

    is_scd2 = table.temporal_model in (TemporalModel.SCD2_RELEASE, TemporalModel.SCD2_BITEMPORAL)
    if is_scd2 and ("valid_from" not in column_names or "valid_to" not in column_names):
        problems.append(
            f"{table.name}: temporal_model {table.temporal_model.value} requires "
            "valid_from/valid_to columns"
        )

    # __post_init__ already rejects these at construction time -- surfaced again here
    # so a lint report names the same invariant a renderer's caller can see, per the
    # task's "already validated ... just surface".
    for label, keys in (
        ("sort_by", table.sort_by),
        ("partition_by", table.partition_by),
        ("primary_key", table.primary_key),
    ):
        missing = [k for k in keys if k not in column_names]
        if missing:
            problems.append(f"{table.name}: {label} column(s) not in columns: {missing}")

    return problems
