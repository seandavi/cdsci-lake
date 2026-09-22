"""Offline tests for ``cdsci.lake.contracts`` (cdsci-lake#96, M0).

``pyarrow`` is not a base dependency of this package, only a dev dependency
(pyproject.toml) -- ``arrow_schema()``/``validate()`` tests use
``importorskip`` so they still degrade gracefully for a consumer running
against the base install only.
"""

from __future__ import annotations

import pytest
from fixtures.contracts import dataset as fx

from cdsci.lake.contracts import ColumnContract, TableContract, TemporalModel


def test_table_contract_primary_key_must_exist_in_columns():
    with pytest.raises(ValueError, match="primary_key column"):
        TableContract(
            name="t",
            description="d",
            grain="g",
            primary_key=("missing_col",),
            temporal_model=TemporalModel.APPEND_IMMUTABLE,
            owner="o",
            license="l",
            columns=(ColumnContract("id", "string", "the id", nullable=False),),
        )


def test_table_contract_rejects_duplicate_column_names():
    with pytest.raises(ValueError, match="duplicate column"):
        TableContract(
            name="t",
            description="d",
            grain="g",
            primary_key=("id",),
            temporal_model=TemporalModel.APPEND_IMMUTABLE,
            owner="o",
            license="l",
            columns=(
                ColumnContract("id", "string", "the id", nullable=False),
                ColumnContract("id", "int64", "dup", nullable=False),
            ),
        )


def test_fixture_dataset_contract_names_a_temporal_model_per_table():
    for table in fx.DATASET_CONTRACT.tables.values():
        assert isinstance(table.temporal_model, TemporalModel)


def test_arrow_schema_round_trips_with_pyarrow():
    pa = pytest.importorskip("pyarrow")
    schema = fx.EVENTS_TABLE.arrow_schema()
    assert isinstance(schema, pa.Schema)
    assert schema.names == ["event_id", "occurred_at", "payload"]
    fx.EVENTS_TABLE.validate(schema)


def test_validate_rejects_type_mismatch_with_pyarrow():
    pa = pytest.importorskip("pyarrow")
    bad = pa.schema(
        [
            pa.field("event_id", pa.int64(), nullable=False),
            pa.field("occurred_at", pa.string(), nullable=False),
            pa.field("payload", pa.string(), nullable=True),
        ]
    )
    with pytest.raises(ValueError, match="event_id"):
        fx.EVENTS_TABLE.validate(bad)


def test_validate_rejects_order_only_mismatch_with_explicit_message():
    pa = pytest.importorskip("pyarrow")
    reordered = pa.schema(
        [
            pa.field("occurred_at", pa.string(), nullable=False),
            pa.field("event_id", pa.string(), nullable=False),
            pa.field("payload", pa.string(), nullable=True),
        ]
    )
    with pytest.raises(ValueError, match="column order differs"):
        fx.EVENTS_TABLE.validate(reordered)


def test_arrow_schema_without_pyarrow_raises_import_error(monkeypatch):
    import builtins

    real_import = builtins.__import__

    def blocked(name, *args, **kwargs):
        if name == "pyarrow":
            raise ImportError("pyarrow not installed")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", blocked)
    with pytest.raises(ImportError):
        fx.EVENTS_TABLE.arrow_schema()
