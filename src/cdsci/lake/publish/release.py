"""``cdsci.lake.publish.release`` — release manifest, file index, acceptance, and receipt types.

Design §6.5/§6.6, §5.2/§5.3, §11.5. Every type here carries ``schema_version``
and round-trips through ``to_json()``/``from_json()``. The design's §5.2
manifest JSON sketch calls this field ``spec_version``; this module follows
this task's explicit instruction and names it ``schema_version`` instead — see
the M0 report for this naming conflict, to reconcile before M1 wire format is
frozen.

``ReleaseManifest``, ``TableFileIndex``/``FileEntry``, and ``AcceptanceReport``
are the types a public consumer (DuckDock, a downloader) reads cold, with no
private credentials — ``_check_public_path``/``_check_no_secret_keys`` reject
absolute paths, ``s3://``/``r2://`` locations, credential-bearing HTTP(S)
URLs, and secret-shaped mapping keys in those types. ``ReleaseCandidate`` and
``PublicationReceipt`` are internal/staging types (design §3.2's "run state,
watermarks, asset identity, publication receipts" is `lake_ops` territory)
and may reference private staging locations.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

from ..contracts import Materialization, TableContract, TemporalModel

SCHEMA_VERSION = "1.0"


class PublicPathError(ValueError):
    """A public artifact field would leak a private path, storage scheme, or credential."""


_FORBIDDEN_SCHEMES = ("s3", "r2")
_SECRET_KEY_SUBSTRINGS = ("token", "secret", "password")


def _check_public_path(value: str, *, field_name: str) -> None:
    if not value:
        return
    if value.startswith("/") or (len(value) > 1 and value[1] == ":" and value[0].isalpha()):
        raise PublicPathError(
            f"{field_name}: absolute path not allowed in a public artifact: {value!r}"
        )
    if "://" in value:
        scheme, rest = value.split("://", 1)
        scheme = scheme.lower()
        if scheme in _FORBIDDEN_SCHEMES:
            raise PublicPathError(
                f"{field_name}: private storage scheme {scheme!r} not allowed in a public "
                f"artifact: {value!r}"
            )
        if scheme in ("http", "https") and "@" in rest.split("/", 1)[0]:
            raise PublicPathError(
                f"{field_name}: credential-bearing URL not allowed in a public artifact: {value!r}"
            )


def _check_no_secret_keys(mapping: Mapping[str, Any], *, field_name: str) -> None:
    for key in mapping:
        if any(s in key.lower() for s in _SECRET_KEY_SUBSTRINGS):
            raise PublicPathError(
                f"{field_name}: secret-shaped key {key!r} not allowed in a public artifact"
            )


class ArtifactStatus(StrEnum):
    STAGED = "staged"
    VERIFIED = "verified"
    PUBLISHED = "published"
    FAILED = "failed"


@dataclass(frozen=True)
class SourceAssetVersion:
    ref: str
    version: str

    def to_dict(self) -> dict[str, Any]:
        return {"ref": self.ref, "version": self.version}

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]) -> SourceAssetVersion:
        return cls(ref=d["ref"], version=d["version"])


@dataclass(frozen=True)
class ArtifactEntry:
    status: ArtifactStatus
    required: bool
    location: str = ""

    def __post_init__(self) -> None:
        _check_public_path(self.location, field_name="ArtifactEntry.location")

    def to_dict(self) -> dict[str, Any]:
        return {"status": self.status.value, "required": self.required, "location": self.location}

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]) -> ArtifactEntry:
        return cls(
            status=ArtifactStatus(d["status"]),
            required=d["required"],
            location=d.get("location", ""),
        )


@dataclass(frozen=True)
class ManifestTable:
    """A release manifest's summary of one ``TableContract`` (design §11.5 checkpoint 5)."""

    name: str
    description: str
    grain: str
    primary_key: tuple[str, ...]
    temporal_model: TemporalModel
    owner: str
    license: str
    schema_path: str
    files_path: str
    schema_digest: str
    row_count: int | None = None

    def __post_init__(self) -> None:
        _check_public_path(self.schema_path, field_name="ManifestTable.schema_path")
        _check_public_path(self.files_path, field_name="ManifestTable.files_path")

    @classmethod
    def from_contract(
        cls,
        contract: TableContract,
        *,
        schema_path: str,
        files_path: str,
        schema_digest: str,
        row_count: int | None = None,
    ) -> ManifestTable:
        return cls(
            name=contract.name,
            description=contract.description,
            grain=contract.grain,
            primary_key=contract.primary_key,
            temporal_model=contract.temporal_model,
            owner=contract.owner,
            license=contract.license,
            schema_path=schema_path,
            files_path=files_path,
            schema_digest=schema_digest,
            row_count=row_count,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "description": self.description,
            "grain": self.grain,
            "primary_key": list(self.primary_key),
            "temporal_model": self.temporal_model.value,
            "owner": self.owner,
            "license": self.license,
            "schema": self.schema_path,
            "files": self.files_path,
            "schema_digest": self.schema_digest,
            "row_count": self.row_count,
        }

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]) -> ManifestTable:
        return cls(
            name=d["name"],
            description=d["description"],
            grain=d["grain"],
            primary_key=tuple(d["primary_key"]),
            temporal_model=TemporalModel(d["temporal_model"]),
            owner=d["owner"],
            license=d["license"],
            schema_path=d["schema"],
            files_path=d["files"],
            schema_digest=d["schema_digest"],
            row_count=d.get("row_count"),
        )


@dataclass(frozen=True)
class ReleaseManifest:
    dataset: str
    release: str
    status: ArtifactStatus
    run_id: str
    tables: tuple[ManifestTable, ...]
    published_at: str | None = None
    source_asset_versions: tuple[SourceAssetVersion, ...] = ()
    artifacts: Mapping[str, ArtifactEntry] = field(default_factory=dict)
    provenance: str = "provenance.json"
    lineage: str = "lineage.json"
    schema_version: str = SCHEMA_VERSION

    def __post_init__(self) -> None:
        _check_public_path(self.provenance, field_name="ReleaseManifest.provenance")
        _check_public_path(self.lineage, field_name="ReleaseManifest.lineage")
        _check_no_secret_keys(self.artifacts, field_name="ReleaseManifest.artifacts")

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "dataset": self.dataset,
            "release": self.release,
            "status": self.status.value,
            "published_at": self.published_at,
            "run_id": self.run_id,
            "source_asset_versions": [v.to_dict() for v in self.source_asset_versions],
            "artifacts": {k: v.to_dict() for k, v in self.artifacts.items()},
            "tables": [t.to_dict() for t in self.tables],
            "provenance": self.provenance,
            "lineage": self.lineage,
        }

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]) -> ReleaseManifest:
        return cls(
            schema_version=d["schema_version"],
            dataset=d["dataset"],
            release=d["release"],
            status=ArtifactStatus(d["status"]),
            published_at=d.get("published_at"),
            run_id=d["run_id"],
            source_asset_versions=tuple(
                SourceAssetVersion.from_dict(v) for v in d.get("source_asset_versions", ())
            ),
            artifacts={k: ArtifactEntry.from_dict(v) for k, v in d.get("artifacts", {}).items()},
            tables=tuple(ManifestTable.from_dict(t) for t in d["tables"]),
            provenance=d.get("provenance", "provenance.json"),
            lineage=d.get("lineage", "lineage.json"),
        )

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2)

    @classmethod
    def from_json(cls, s: str) -> ReleaseManifest:
        return cls.from_dict(json.loads(s))


@dataclass(frozen=True)
class FileEntry:
    uri: str
    size: int
    sha256: str
    content_type: str
    rows: int | None = None

    def __post_init__(self) -> None:
        _check_public_path(self.uri, field_name="FileEntry.uri")

    def to_dict(self) -> dict[str, Any]:
        return {
            "uri": self.uri,
            "size": self.size,
            "sha256": self.sha256,
            "rows": self.rows,
            "content_type": self.content_type,
        }

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]) -> FileEntry:
        return cls(
            uri=d["uri"],
            size=d["size"],
            sha256=d["sha256"],
            rows=d.get("rows"),
            content_type=d["content_type"],
        )


@dataclass(frozen=True)
class TableFileIndex:
    table: str
    release: str
    materialization: Materialization
    files: tuple[FileEntry, ...]
    schema_version: str = SCHEMA_VERSION

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "table": self.table,
            "release": self.release,
            "materialization": self.materialization.value,
            "files": [f.to_dict() for f in self.files],
        }

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]) -> TableFileIndex:
        return cls(
            schema_version=d["schema_version"],
            table=d["table"],
            release=d["release"],
            materialization=Materialization(d["materialization"]),
            files=tuple(FileEntry.from_dict(f) for f in d["files"]),
        )

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2)

    @classmethod
    def from_json(cls, s: str) -> TableFileIndex:
        return cls.from_dict(json.loads(s))


@dataclass(frozen=True)
class AcceptanceCheck:
    name: str
    passed: bool
    required: bool
    detail: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "passed": self.passed,
            "required": self.required,
            "detail": self.detail,
        }

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]) -> AcceptanceCheck:
        return cls(
            name=d["name"], passed=d["passed"], required=d["required"], detail=d.get("detail", "")
        )


@dataclass(frozen=True)
class AcceptanceReport:
    dataset: str
    release: str
    run_id: str
    checked_at: str
    checks: tuple[AcceptanceCheck, ...]
    schema_version: str = SCHEMA_VERSION

    @property
    def passed(self) -> bool:
        """False if any required check failed. Optional failures stay visible but don't gate."""
        return all(c.passed for c in self.checks if c.required)

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "dataset": self.dataset,
            "release": self.release,
            "run_id": self.run_id,
            "checked_at": self.checked_at,
            "checks": [c.to_dict() for c in self.checks],
        }

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]) -> AcceptanceReport:
        return cls(
            schema_version=d["schema_version"],
            dataset=d["dataset"],
            release=d["release"],
            run_id=d["run_id"],
            checked_at=d["checked_at"],
            checks=tuple(AcceptanceCheck.from_dict(c) for c in d["checks"]),
        )

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2)

    @classmethod
    def from_json(cls, s: str) -> AcceptanceReport:
        return cls.from_dict(json.loads(s))


@dataclass(frozen=True)
class ReleaseCandidate:
    """A built-but-not-yet-promoted release (design §3.3, steps D-K) — internal, pre-acceptance."""

    dataset: str
    release: str
    run_id: str
    built_at: str
    destination: str
    tables: tuple[str, ...]
    status: ArtifactStatus
    acceptance: AcceptanceReport | None = None
    schema_version: str = SCHEMA_VERSION

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "dataset": self.dataset,
            "release": self.release,
            "run_id": self.run_id,
            "built_at": self.built_at,
            "destination": self.destination,
            "tables": list(self.tables),
            "status": self.status.value,
            "acceptance": self.acceptance.to_dict() if self.acceptance else None,
        }

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]) -> ReleaseCandidate:
        return cls(
            schema_version=d["schema_version"],
            dataset=d["dataset"],
            release=d["release"],
            run_id=d["run_id"],
            built_at=d["built_at"],
            destination=d["destination"],
            tables=tuple(d["tables"]),
            status=ArtifactStatus(d["status"]),
            acceptance=AcceptanceReport.from_dict(d["acceptance"]) if d.get("acceptance") else None,
        )

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2)

    @classmethod
    def from_json(cls, s: str) -> ReleaseCandidate:
        return cls.from_dict(json.loads(s))


@dataclass(frozen=True)
class PublicationReceipt:
    """Recorded by ``OpsSink.record_publication`` (design §6.3/§6.6).

    Internal -- not a public artifact.
    """

    dataset: str
    release: str
    format: str
    destination: str
    schema_digest: str
    run_id: str
    status: ArtifactStatus
    row_counts: Mapping[str, int] = field(default_factory=dict)
    checksums: Mapping[str, str] = field(default_factory=dict)
    version: str | None = None
    details: Mapping[str, Any] = field(default_factory=dict)
    schema_version: str = SCHEMA_VERSION

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "dataset": self.dataset,
            "release": self.release,
            "format": self.format,
            "destination": self.destination,
            "schema_digest": self.schema_digest,
            "run_id": self.run_id,
            "status": self.status.value,
            "row_counts": dict(self.row_counts),
            "checksums": dict(self.checksums),
            "version": self.version,
            "details": dict(self.details),
        }

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]) -> PublicationReceipt:
        return cls(
            schema_version=d["schema_version"],
            dataset=d["dataset"],
            release=d["release"],
            format=d["format"],
            destination=d["destination"],
            schema_digest=d["schema_digest"],
            run_id=d["run_id"],
            status=ArtifactStatus(d["status"]),
            row_counts=d.get("row_counts", {}),
            checksums=d.get("checksums", {}),
            version=d.get("version"),
            details=d.get("details", {}),
        )

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2)

    @classmethod
    def from_json(cls, s: str) -> PublicationReceipt:
        return cls.from_dict(json.loads(s))
