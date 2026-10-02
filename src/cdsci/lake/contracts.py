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

import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import pyarrow as pa


# Dotted source ref grammar: "<catalog>.<schema>.<table>", lowercase identifiers, no
# scheme, no credentials -- e.g. "lake.demo.events" or "canceronice.measure.observation".
# Exactly three dotted segments; a release-asset ref ("release.<dataset>.<release>",
# which may carry uppercase/hyphenated producer-chosen ids) is deliberately NOT matched
# here -- that ref form goes through the looser :func:`check_asset_ref`.
_SOURCE_REF_PATTERN = re.compile(r"^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*){2}$")

_CONTROL_CHAR_PATTERN = re.compile(r"[\x00-\x1f\x7f]")
_CREDENTIAL_WORD_PATTERN = re.compile(r"password|secret|token|credential", re.IGNORECASE)


def check_asset_ref(ref: str) -> None:
    """Reject a malformed or credential-bearing asset ``ref`` (ADR-0014 §5).

    Generic across ref schemes (``lake.<schema>.<table>``, ``r2://...``,
    ``postgres://...``, ``release.<dataset>.<release>``) -- guards only what matters
    regardless of scheme: no stray whitespace, no control characters, no embedded
    credentials (a bare ``@``, anywhere, not just in a URL's netloc -- a scheme-less
    or ``scheme:``-only ref like ``postgres:user:pass@host`` never populates
    ``urlsplit().netloc``; and no case-insensitive password/secret/token/credential
    substring). Shared by :mod:`cdsci.lake.ops` (any asset type) and
    :mod:`cdsci.lake.publish.release` (layered under the stricter
    :func:`check_source_ref`).
    """
    if not ref or ref != ref.strip():
        raise ValueError(f"invalid asset ref: {ref!r}")
    if _CONTROL_CHAR_PATTERN.search(ref):
        raise ValueError(f"asset ref must not contain control characters: {ref!r}")
    if "@" in ref:
        raise ValueError(f"asset ref must not embed credentials: {ref!r}")
    if _CREDENTIAL_WORD_PATTERN.search(ref):
        raise ValueError(f"asset ref must not embed credentials: {ref!r}")


def check_source_ref(ref: str, *, field_name: str = "ref") -> None:
    """Validate the dotted source ref grammar ``<catalog>.<schema>.<table>``.

    ``lake`` is the shared internal DuckLake; any other first segment names a
    product-local catalog (e.g. ``canceronice.measure.observation``). Lowercase
    dotted identifiers, no scheme, no credentials. Layered on top of
    :func:`check_asset_ref`'s baseline safety check.
    """
    check_asset_ref(ref)
    if "://" in ref:
        raise ValueError(f"{field_name}: source ref must not carry a scheme: {ref!r}")
    if not _SOURCE_REF_PATTERN.match(ref):
        raise ValueError(
            f"{field_name}: source ref must be the dotted form '<catalog>.<schema>.<table>' "
            f"(lowercase identifiers only): {ref!r}"
        )


class TemporalModel(StrEnum):
    """Names a table's temporal write/read model.

    Never describe a table as merely "versioned" (AGENTS.md).
    """

    APPEND_IMMUTABLE = "append_immutable"
    UPSERT_LATEST_SNAPSHOT = "upsert_latest_snapshot"


_PRIMITIVE_TYPES = frozenset(
    {
        "string", "bool", "int8", "int16", "int32", "int64", "uint8", "uint16",
        "uint32", "uint64", "float", "double", "date32", "binary",
    }
)
_FIELD_NAME_PATTERN = re.compile(r"^[a-z_][a-z0-9_]*$")


def _top_level_split(body: str, sep: str) -> list[str]:
    """Split ``body`` on ``sep`` only where bracket depth (``<>`` and ``[]``) is zero."""
    parts: list[str] = []
    depth = 0
    start = 0
    for i, ch in enumerate(body):
        if ch in "<[":
            depth += 1
        elif ch in ">]":
            depth -= 1
        elif ch == sep and depth == 0:
            parts.append(body[start:i])
            start = i + 1
    parts.append(body[start:])
    return parts


def _split_canonical(canonical: str) -> tuple[str, tuple]:
    """Parse one canonical Arrow type string one level deep.

    Returns ``("primitive", (name,))``, ``("list", (inner,))`` or
    ``("struct", ((fname, ftype), ...))``; nested types stay as canonical strings so
    consumers recurse. Struct bodies split only on top-level commas.
    """
    text = canonical.strip()
    if text in _PRIMITIVE_TYPES or (text.startswith("timestamp[") and text.endswith("]")):
        return "primitive", (text,)
    if text.startswith("list<") and text.endswith(">"):
        inner = text[len("list<") : -1].strip()
        if not inner:
            raise ValueError(f"unsupported canonical arrow type string: {canonical!r}")
        return "list", (inner,)
    if text.startswith("struct<") and text.endswith(">"):
        body = text[len("struct<") : -1]
        fields: list[tuple[str, str]] = []
        for part in _top_level_split(body, ","):
            fname, colon, ftype = part.partition(":")
            fname = fname.strip()
            ftype = ftype.strip()
            if not colon or not ftype or not _FIELD_NAME_PATTERN.match(fname):
                raise ValueError(f"unsupported canonical arrow type string: {canonical!r}")
            fields.append((fname, ftype))
        return "struct", tuple(fields)
    raise ValueError(f"unsupported canonical arrow type string: {canonical!r}")


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
    kind, args = _split_canonical(canonical)
    if kind == "list":
        return pa.list_(_parse_arrow_type(args[0]))
    if kind == "struct":
        return pa.struct([pa.field(n, _parse_arrow_type(t)) for n, t in args])
    text = args[0]
    if text in simple:
        return simple[text]
    body = text[len("timestamp[") : -1]
    unit, _, tz_part = body.partition(",")
    unit = unit.strip()
    tz = tz_part.split("=", 1)[1].strip() if "=" in tz_part else (tz_part.strip() or None)
    return pa.timestamp(unit, tz=tz)


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
        missing_sort = [k for k in self.sort_by if k not in column_names]
        if missing_sort:
            raise ValueError(f"{self.name}: sort_by column(s) not in columns: {missing_sort}")
        missing_partition = [k for k in self.partition_by if k not in column_names]
        if missing_partition:
            raise ValueError(
                f"{self.name}: partition_by column(s) not in columns: {missing_partition}"
            )

    def arrow_schema(self) -> pa.Schema:
        import pyarrow as pa

        return pa.schema(
            [
                pa.field(c.name, _parse_arrow_type(c.arrow_type), nullable=c.nullable)
                for c in self.columns
            ]
        )

    def to_schema_dict(self) -> dict[str, Any]:
        """Render this contract as the ``tables/<name>/schema.json`` document (design §5).

        Pure dict construction -- no ``pyarrow`` import, so a publish-side caller
        without the dev/publish extras installed can still render a schema.
        """
        # `properties` is deliberately dropped here: it's free-form producer key/value
        # metadata, not schema-shaped, and unlike `examples` it isn't validated against
        # the public-artifact secret/path allowlists -- rendering it would let a producer
        # accidentally publish arbitrary unchecked content through the schema document.
        return {
            "name": self.name,
            "description": self.description,
            "grain": self.grain,
            "primary_key": list(self.primary_key),
            "sort_by": list(self.sort_by),
            "partition_by": list(self.partition_by),
            "temporal_model": self.temporal_model.value,
            "owner": self.owner,
            "license": self.license,
            "examples": list(self.examples),
            "columns": [
                {
                    "name": c.name,
                    "arrow_type": c.arrow_type,
                    "description": c.description,
                    "nullable": c.nullable,
                    "identifier_namespace": c.identifier_namespace,
                    "units": c.units,
                    "coordinate_system": c.coordinate_system,
                    "null_meaning": c.null_meaning,
                    "enum": list(c.enum),
                }
                for c in self.columns
            ],
        }

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
    keep_last: int | None = None

    def __post_init__(self) -> None:
        if self.keep_last is not None and self.keep_last < 1:
            raise ValueError(f"{self.id}: keep_last must be >= 1 or None, got {self.keep_last}")
