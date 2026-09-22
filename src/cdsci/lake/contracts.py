"""``cdsci.lake.contracts`` — semantic and temporal table contracts (design §6.1).

The domain repository (bioc-on-ice, cancer-on-ice) constructs ``TableContract``/
``DatasetContract`` instances; this module only validates and renders them. No
biological or cancer meaning lives here — see AGENTS.md "Domain meaning stays
domain-local".

``pyarrow`` is not a base dependency of this package (see ``pyproject.toml``).
``ColumnContract.arrow_type`` therefore stores the Arrow type as its canonical
string (e.g. ``"int64"``, ``"string"``, ``"timestamp[us]"``) rather than a
``pyarrow.DataType``. ``TableContract.arrow_schema()``/``.validate()`` convert
lazily and raise ``ImportError`` if a caller invokes them without ``pyarrow``
installed.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pyarrow as pa


class TemporalModel(StrEnum):
    """Names a table's temporal write/read model.

    Never describe a table as merely "versioned" (AGENTS.md).
    """

    APPEND_IMMUTABLE = "append_immutable"
    UPSERT_LATEST_SNAPSHOT = "upsert_latest_snapshot"
    SCD2_RELEASE = "scd2_release"
    SCD2_BITEMPORAL = "scd2_bitemporal"


class Materialization(StrEnum):
    """How a table's published files relate to release history (design §5.3)."""

    HISTORY = "history"
    RELEASE_SNAPSHOT = "release_snapshot"
    APPEND_IMMUTABLE = "append_immutable"


def _parse_arrow_type(canonical: str) -> pa.DataType:
    """Convert a canonical Arrow type string to a ``pyarrow.DataType``. Lazy-imports pyarrow."""
    import pyarrow as pa

    simple = {
        "string": pa.string(),
        "bool": pa.bool_(),
        "int8": pa.int8(),
        "int16": pa.int16(),
        "int32": pa.int32(),
        "int64": pa.int64(),
        "uint8": pa.uint8(),
        "uint16": pa.uint16(),
        "uint32": pa.uint32(),
        "uint64": pa.uint64(),
        "float": pa.float32(),
        "double": pa.float64(),
        "date32": pa.date32(),
        "binary": pa.binary(),
    }
    if canonical in simple:
        return simple[canonical]
    if canonical.startswith("timestamp[") and canonical.endswith("]"):
        body = canonical[len("timestamp[") : -1]
        unit, _, tz_part = body.partition(",")
        unit = unit.strip()
        tz = tz_part.split("=", 1)[1].strip() if "=" in tz_part else (tz_part.strip() or None)
        return pa.timestamp(unit, tz=tz)
    raise ValueError(f"unsupported canonical arrow type string: {canonical!r}")


@dataclass(frozen=True)
class ColumnContract:
    name: str
    arrow_type: str
    description: str
    nullable: bool
    identifier_namespace: str | None = None
    units: str | None = None
    coordinate_system: str | None = None
    null_meaning: str | None = None
    enum: tuple[str, ...] = ()


@dataclass(frozen=True)
class TableContract:
    name: str
    description: str
    grain: str
    primary_key: tuple[str, ...]
    temporal_model: TemporalModel
    owner: str
    license: str
    columns: tuple[ColumnContract, ...]
    sort_by: tuple[str, ...] = ()
    partition_by: tuple[str, ...] = ()
    examples: tuple[str, ...] = ()
    properties: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        column_names = {c.name for c in self.columns}
        missing = [k for k in self.primary_key if k not in column_names]
        if missing:
            raise ValueError(f"{self.name}: primary_key column(s) not in columns: {missing}")
        if len(column_names) != len(self.columns):
            raise ValueError(f"{self.name}: duplicate column name(s) in columns")

    def arrow_schema(self) -> pa.Schema:
        import pyarrow as pa

        return pa.schema(
            [
                pa.field(c.name, _parse_arrow_type(c.arrow_type), nullable=c.nullable)
                for c in self.columns
            ]
        )

    def validate(self, incoming: pa.Schema) -> None:
        """Raise ``ValueError`` unless ``incoming`` matches this contract's schema exactly."""
        expected = self.arrow_schema()
        if incoming.equals(expected):
            return
        expected_names = set(expected.names)
        incoming_names = set(incoming.names)
        problems: list[str] = []
        missing = expected_names - incoming_names
        if missing:
            problems.append(f"missing columns: {sorted(missing)}")
        extra = incoming_names - expected_names
        if extra:
            problems.append(f"unexpected columns: {sorted(extra)}")
        for name in expected_names & incoming_names:
            exp_field = expected.field(name)
            inc_field = incoming.field(name)
            if exp_field.type != inc_field.type:
                problems.append(f"{name}: expected type {exp_field.type}, got {inc_field.type}")
            elif exp_field.nullable != inc_field.nullable:
                problems.append(
                    f"{name}: expected nullable={exp_field.nullable}, got {inc_field.nullable}"
                )
        if not problems:
            problems.append(f"column order differs, expected: {list(expected.names)}")
        raise ValueError(f"{self.name}: schema does not match contract: {'; '.join(problems)}")


@dataclass(frozen=True)
class DatasetContract:
    id: str
    title: str
    description: str
    publisher: str
    tables: Mapping[str, TableContract]
    required_artifacts: frozenset[str] = frozenset({"parquet", "ducklake"})
