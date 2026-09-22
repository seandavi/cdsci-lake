"""Offline tests for ``cdsci.lake.contracts_render`` (cdsci-lake#95 M1).

Golden markdown is compared byte-for-byte against the shared M0 conformance
fixture (``tests/fixtures/contracts/dataset.py``); ``lint_contract`` is tested
against a locally-built, deliberately lint-clean ``TableContract`` (the shared
fixture itself is not lint-clean -- it predates ``identifier_namespace``/
``null_meaning`` population, exercised separately in ``test_contracts.py``) with
one defect injected per test case.
"""

from __future__ import annotations

import dataclasses
import json
from pathlib import Path

from fixtures.contracts import dataset as fx

from cdsci.lake.contracts import ColumnContract, DatasetContract, TableContract, TemporalModel
from cdsci.lake.contracts_render import (
    lint_contract,
    render_dataset_json,
    render_dataset_markdown,
    render_table_markdown,
)
from cdsci.lake.publish.release import _UNSAFE_PATTERN

FIXTURES = Path(__file__).parent / "fixtures" / "contracts"
GOLDEN_TABLE_PATH = FIXTURES / "golden_table.md"
GOLDEN_DATASET_PATH = FIXTURES / "golden_dataset.md"

_CLEAN_TABLE = TableContract(
    name="clean.table",
    description="A deliberately lint-clean table for lint_contract's own tests.",
    grain="one row per record_id and validity interval",
    primary_key=("record_id", "valid_from"),
    temporal_model=TemporalModel.SCD2_RELEASE,
    owner="cdsci-lake",
    license="cc0",
    columns=(
        ColumnContract(
            "record_id", "string", "Business key.", nullable=False,
            identifier_namespace="clean.record",
        ),
        ColumnContract(
            "position", "int64", "Ordinal position.", nullable=False,
            coordinate_system="clean.linear",
        ),
        ColumnContract(
            "value", "string", "Observed value.", nullable=True,
            null_meaning="No value was reported.",
        ),
        ColumnContract("valid_from", "string", "Release this interval opened.", nullable=False),
        ColumnContract(
            "valid_to", "string", "Release this interval closed, or null if current.",
            nullable=True, null_meaning="Current version -- not unknown.",
        ),
    ),
    sort_by=("record_id",),
    examples=("SELECT * FROM clean.table WHERE record_id = 'x';",),
)


def test_render_table_markdown_matches_golden_fixture():
    assert render_table_markdown(fx.EVENTS_TABLE) == GOLDEN_TABLE_PATH.read_text()


def test_render_dataset_markdown_matches_golden_fixture():
    assert render_dataset_markdown(fx.DATASET_CONTRACT) == GOLDEN_DATASET_PATH.read_text()


def test_render_table_markdown_renders_enum_units_and_examples():
    rendered = render_table_markdown(
        dataclasses.replace(
            _CLEAN_TABLE,
            columns=(
                *_CLEAN_TABLE.columns[:-1],
                dataclasses.replace(
                    _CLEAN_TABLE.columns[-1], units="count", enum=("open", "closed")
                ),
            ),
        )
    )
    assert "open, closed" in rendered
    assert "count" in rendered
    assert "## Examples" in rendered
    assert "```sql\nSELECT * FROM clean.table WHERE record_id = 'x';\n```" in rendered


def test_render_table_markdown_is_deterministic_across_repeat_calls():
    assert render_table_markdown(fx.EVENTS_TABLE) == render_table_markdown(fx.EVENTS_TABLE)


def test_render_table_markdown_escapes_pipe_in_cell_text():
    table = dataclasses.replace(
        _CLEAN_TABLE,
        columns=(
            dataclasses.replace(_CLEAN_TABLE.columns[0], description="a | b"),
            *_CLEAN_TABLE.columns[1:],
        ),
    )
    assert "a \\| b" in render_table_markdown(table)


def test_render_dataset_json_matches_design_5_1_shape_and_round_trips():
    rendered = render_dataset_json(fx.DATASET_CONTRACT)
    assert rendered.keys() == {"id", "title", "description", "publisher", "tables"}
    assert rendered["id"] == "demo-catalog"
    assert [t["name"] for t in rendered["tables"]] == sorted(fx.DATASET_CONTRACT.tables)
    first_table = fx.DATASET_CONTRACT.tables[rendered["tables"][0]["name"]]
    assert rendered["tables"][0] == first_table.to_schema_dict()

    dumped = json.dumps(rendered, sort_keys=True)
    assert json.loads(dumped) == rendered


def test_lint_contract_returns_empty_list_for_clean_table():
    assert lint_contract(_CLEAN_TABLE) == []


def test_lint_contract_flags_empty_column_description():
    table = dataclasses.replace(
        _CLEAN_TABLE,
        columns=(
            dataclasses.replace(_CLEAN_TABLE.columns[0], description=""),
            *_CLEAN_TABLE.columns[1:],
        ),
    )
    assert lint_contract(table) == ["clean.table.record_id: description is empty"]


def test_lint_contract_flags_identifier_column_with_no_namespace():
    table = dataclasses.replace(
        _CLEAN_TABLE,
        columns=(
            dataclasses.replace(_CLEAN_TABLE.columns[0], identifier_namespace=None),
            *_CLEAN_TABLE.columns[1:],
        ),
    )
    assert lint_contract(table) == [
        "clean.table.record_id: identifier column has no identifier_namespace"
    ]


def test_lint_contract_flags_coordinate_looking_column_with_no_coordinate_system():
    table = dataclasses.replace(
        _CLEAN_TABLE,
        columns=(
            _CLEAN_TABLE.columns[0],
            dataclasses.replace(_CLEAN_TABLE.columns[1], coordinate_system=None),
            *_CLEAN_TABLE.columns[2:],
        ),
    )
    assert lint_contract(table) == [
        "clean.table.position: coordinate column has no coordinate_system"
    ]


def test_lint_contract_flags_nullable_column_with_no_null_meaning():
    table = dataclasses.replace(
        _CLEAN_TABLE,
        columns=(
            *_CLEAN_TABLE.columns[:2],
            dataclasses.replace(_CLEAN_TABLE.columns[2], null_meaning=None),
            *_CLEAN_TABLE.columns[3:],
        ),
    )
    assert lint_contract(table) == ["clean.table.value: nullable column has no null_meaning"]


def test_lint_contract_flags_scd2_table_missing_valid_from_valid_to():
    table = dataclasses.replace(
        _CLEAN_TABLE,
        primary_key=("record_id",),
        columns=_CLEAN_TABLE.columns[:3],
    )
    assert lint_contract(table) == [
        "clean.table: temporal_model scd2_release requires valid_from/valid_to columns"
    ]


def test_render_table_and_dataset_markdown_contain_no_absolute_path_or_secret():
    for rendered in (
        render_table_markdown(fx.EVENTS_TABLE),
        render_dataset_markdown(fx.DATASET_CONTRACT),
    ):
        assert _UNSAFE_PATTERN.search(rendered) is None


def test_dataset_contract_can_construct_without_scd2_valid_columns_and_lint_still_flags_it():
    """The invariant lint_contract surfaces for a hand-broken SCD2 table isn't
    rejected by TableContract construction itself (unlike sort_by/primary_key,
    valid_from/valid_to are not structurally required by __post_init__)."""
    table = TableContract(
        name="broken.table", description="d", grain="g", primary_key=("id",),
        temporal_model=TemporalModel.SCD2_RELEASE, owner="o", license="l",
        columns=(ColumnContract("id", "string", "the id", nullable=False),),
    )
    assert "valid_from/valid_to" in lint_contract(table)[0]


def test_dataset_contract_render_dataset_json_survives_a_second_table():
    """Sanity: the dataset shape's ``tables[]`` really is per-table, not a
    single-table shortcut -- both fixture tables appear."""
    contract = DatasetContract(
        id="d", title="t", description="desc", publisher="p",
        tables={"a.one": fx.EVENTS_TABLE, "b.two": fx.ENTITIES_TABLE},
    )
    rendered = render_dataset_json(contract)
    assert [t["name"] for t in rendered["tables"]] == ["demo.events", "demo.entities"]
