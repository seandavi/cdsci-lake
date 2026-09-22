"""``cdsci.lake.publish.release`` — release manifest, file index, acceptance, and receipt types.

Design §6.5/§6.6, §5.2/§5.3, §11.5. Every type here carries ``spec_version``
(design §5.2) and round-trips through ``to_json()``/``from_json()``.

``ReleaseManifest``, ``TableFileIndex``/``FileEntry``, and ``AcceptanceReport``
are the types a public consumer (DuckDock, a downloader) reads cold, with no
private credentials. Fields are split into two classes: a small, explicit set
of *locator* fields (``_LOCATOR_FIELDS`` -- ``schema_path``, ``files_path``,
``location``, ``uri``, ``provenance``, ``lineage``) is run through
``_check_public_path``, an *allowlist*: only a relative POSIX path or a
credential-free ``https://`` URL to a public host passes; everything else
(``s3://``, ``postgresql://``, ``file://``, ``gs://``, UNC paths, ``..``
traversal, leading/trailing whitespace, absolute paths, drive letters, any
non-``https`` URL scheme) is rejected. Every other string field (prose like
``description``, ``grain``, ``detail``, identifiers like ``dataset``,
``run_id``) is free text and only gets a pattern scan
(``_check_no_unsafe_pattern``) for embedded private-storage schemes, absolute
private-looking POSIX paths, and secret-shaped substrings -- it is never run
through the locator allowlist, which would reject ordinary prose that happens
to mention a public URL. ``_check_no_secret_keys`` rejects secret-shaped
mapping keys (e.g. ``artifacts``). ``SourceAssetVersion.ref`` is the one
exception: it is an internal-lake *asset identifier* (design §5.2's
``"ducklake://lake/..."``), not a resolvable public location, so it gets its
own narrow ``_check_asset_ref`` instead of the path allowlist -- only the
``ducklake`` scheme, no userinfo, no private/loopback host, no ``..``.
``ReleaseCandidate`` and ``PublicationReceipt`` are internal/staging types
(design §3.2's "run state, watermarks, asset identity, publication
receipts" is `lake_ops` territory) and may reference private staging
locations -- neither check runs on them.
"""

from __future__ import annotations

import dataclasses
import ipaddress
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any
from urllib.parse import unquote, urlsplit

from ..contracts import DatasetContract, Materialization, TableContract, TemporalModel

SPEC_VERSION = "1.0"


class PublicPathError(ValueError):
    """A public artifact field would leak a private path, storage scheme, or credential."""


class RequiredArtifactMissingError(ValueError):
    """A dataset contract's ``required_artifacts`` is not a subset of a manifest's artifacts."""


_SECRET_KEY_SUBSTRINGS = ("token", "secret", "password", "key", "credential", "auth", "apikey")

# Fields that name a location -- everything else on a public dataclass is free text and only
# gets the lighter pattern scan below (``_check_no_unsafe_pattern``), not the locator allowlist.
_LOCATOR_FIELDS = frozenset(
    {"schema_path", "files_path", "location", "uri", "provenance", "lineage"}
)

_UNSAFE_PATTERN = re.compile(
    r"(?ix)"
    r"\b(s3|r2|gs|file|postgres(?:ql)?)://"  # private/local storage schemes
    r"|ducklake://[^/@]*@"  # ducklake ref with userinfo
    r"|(?<![\w./-])/(mnt|home|tmp|etc|var|opt|data)/"  # absolute POSIX path
    r"|token="
    r"|password"
    r"|secret"
    r"|api[-_]?key"
    r"|AKIA[0-9A-Z]{16}"
    r"|ya29\."
)


def _is_private_or_loopback_host(host: str) -> bool:
    host = host.strip("[]").lower()
    if host in ("localhost", ""):
        return True
    try:
        addr = ipaddress.ip_address(host)
    except ValueError:
        return False
    return addr.is_private or addr.is_loopback or addr.is_link_local or addr.is_reserved


def _looks_like_a_public_hostname(host: str) -> bool:
    """Cheaply kills decimal/hex/octal IP spellings: a real hostname has a dot and an
    all-alphabetic top-level label (``0x7f.0.0.1`` has a letter but a numeric TLD)."""
    labels = host.split(".")
    return len(labels) > 1 and all(labels) and labels[-1].isalpha()


def _check_no_unsafe_pattern(value: str, *, field_name: str) -> None:
    """Free-text scan: reject embedded private-storage schemes, private paths, or secrets.

    Unlike ``_check_public_path`` this is not an allowlist -- prose that happens to mention a
    public ``https://`` URL is fine; only these specific shapes are rejected.
    """
    match = _UNSAFE_PATTERN.search(value)
    if match:
        raise PublicPathError(
            f"{field_name}: looks like it contains a private location or secret "
            f"({match.group(0)!r}): {value!r}"
        )


def _check_public_path(value: str, *, field_name: str) -> None:
    """Allowlist: a relative POSIX path, or a credential-free ``https://`` URL to a public host."""
    if not value:
        return
    if value != value.strip():
        raise PublicPathError(
            f"{field_name}: leading/trailing whitespace not allowed in a public artifact: {value!r}"
        )
    if "\\" in value:
        raise PublicPathError(
            f"{field_name}: backslash not allowed in a public artifact: {value!r}"
        )
    parts = urlsplit(value)
    if parts.scheme:
        if parts.scheme.lower() != "https":
            raise PublicPathError(
                f"{field_name}: only https:// URLs are allowed in a public artifact (got scheme "
                f"{parts.scheme!r}): {value!r}"
            )
        if "@" in parts.netloc:
            raise PublicPathError(
                f"{field_name}: credential-bearing URL not allowed in a public artifact: {value!r}"
            )
        host = parts.hostname or ""
        if (
            not host
            or _is_private_or_loopback_host(host)
            or not _looks_like_a_public_hostname(host)
        ):
            raise PublicPathError(
                f"{field_name}: private, loopback, or non-hostname address not allowed in a "
                f"public artifact: {value!r}"
            )
        if ".." in unquote(parts.path).split("/"):
            raise PublicPathError(
                f"{field_name}: path traversal ('..') not allowed in a public artifact: {value!r}"
            )
        return
    if value.startswith("/") or (len(value) > 1 and value[1] == ":" and value[0].isalpha()):
        raise PublicPathError(
            f"{field_name}: absolute path not allowed in a public artifact: {value!r}"
        )
    if ".." in unquote(value).split("/"):
        raise PublicPathError(
            f"{field_name}: path traversal ('..') not allowed in a public artifact: {value!r}"
        )


def _check_asset_ref(value: str, *, field_name: str) -> None:
    """``SourceAssetVersion.ref`` is an internal-lake asset identifier, not a public location.

    Only ``ducklake://`` is accepted -- no userinfo, no private/loopback host, no ``..``.
    """
    if not value:
        return
    if value != value.strip() or "\\" in value:
        raise PublicPathError(f"{field_name}: malformed asset reference: {value!r}")
    parts = urlsplit(value)
    if parts.scheme.lower() != "ducklake":
        raise PublicPathError(
            f"{field_name}: only ducklake:// asset references are allowed (got scheme "
            f"{parts.scheme!r}): {value!r}"
        )
    if "@" in parts.netloc:
        raise PublicPathError(f"{field_name}: credential-bearing asset reference: {value!r}")
    if not parts.hostname or _is_private_or_loopback_host(parts.hostname):
        raise PublicPathError(f"{field_name}: private/loopback host in asset reference: {value!r}")
    if ".." in unquote(parts.path).split("/"):
        raise PublicPathError(f"{field_name}: path traversal ('..') in asset reference: {value!r}")


def _check_no_secret_keys(mapping: Mapping[str, Any], *, field_name: str) -> None:
    for key in mapping:
        if any(s in key.lower() for s in _SECRET_KEY_SUBSTRINGS):
            raise PublicPathError(
                f"{field_name}: secret-shaped key {key!r} not allowed in a public artifact"
            )


def _check_public_strings(instance: Any, *, exclude: frozenset[str] = frozenset()) -> None:
    """Run the locator allowlist or the free-text pattern scan over every ``str`` field.

    A field named in ``_LOCATOR_FIELDS`` gets ``_check_public_path``; every other ``str``
    field is free text and gets ``_check_no_unsafe_pattern`` instead. Nested public
    dataclasses (tuple/dict members) self-validate in their own ``__post_init__`` at
    construction time, so this only needs to look at ``instance``'s own fields -- that is
    the recursion.
    """
    type_name = type(instance).__name__
    for f in dataclasses.fields(instance):
        if f.name in exclude:
            continue
        value = getattr(instance, f.name)
        if not isinstance(value, str):
            continue
        if f.name in _LOCATOR_FIELDS:
            _check_public_path(value, field_name=f"{type_name}.{f.name}")
        else:
            _check_no_unsafe_pattern(value, field_name=f"{type_name}.{f.name}")


class ArtifactStatus(StrEnum):
    STAGED = "staged"
    VERIFIED = "verified"
    PUBLISHED = "published"
    FAILED = "failed"


@dataclass(frozen=True)
class SourceAssetVersion:
    ref: str
    version: str

    def __post_init__(self) -> None:
        _check_asset_ref(self.ref, field_name="SourceAssetVersion.ref")
        _check_public_strings(self, exclude=frozenset({"ref"}))

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
        _check_public_strings(self)

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
        _check_public_strings(self)

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
    spec_version: str = SPEC_VERSION

    def __post_init__(self) -> None:
        _check_public_strings(self)
        _check_no_secret_keys(self.artifacts, field_name="ReleaseManifest.artifacts")

    def to_dict(self) -> dict[str, Any]:
        return {
            "spec_version": self.spec_version,
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
            spec_version=d["spec_version"],
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


def check_required_artifacts(manifest: ReleaseManifest, contract: DatasetContract) -> None:
    """Raise unless ``contract.required_artifacts`` is a subset of ``manifest.artifacts``."""
    missing = contract.required_artifacts - manifest.artifacts.keys()
    if missing:
        raise RequiredArtifactMissingError(
            f"{manifest.dataset} {manifest.release}: manifest is missing required artifact(s) "
            f"declared by the dataset contract: {sorted(missing)}"
        )


@dataclass(frozen=True)
class FileEntry:
    uri: str
    size: int
    sha256: str
    content_type: str
    rows: int | None = None

    def __post_init__(self) -> None:
        _check_public_strings(self)

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
    spec_version: str = SPEC_VERSION

    def __post_init__(self) -> None:
        _check_public_strings(self)

    def to_dict(self) -> dict[str, Any]:
        return {
            "spec_version": self.spec_version,
            "table": self.table,
            "release": self.release,
            "materialization": self.materialization.value,
            "files": [f.to_dict() for f in self.files],
        }

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]) -> TableFileIndex:
        return cls(
            spec_version=d["spec_version"],
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

    def __post_init__(self) -> None:
        _check_public_strings(self)

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
    spec_version: str = SPEC_VERSION

    def __post_init__(self) -> None:
        _check_public_strings(self)

    @property
    def passed(self) -> bool:
        """False if any required check failed. Optional failures stay visible but don't gate."""
        return all(c.passed for c in self.checks if c.required)

    def to_dict(self) -> dict[str, Any]:
        return {
            "spec_version": self.spec_version,
            "dataset": self.dataset,
            "release": self.release,
            "run_id": self.run_id,
            "checked_at": self.checked_at,
            "checks": [c.to_dict() for c in self.checks],
        }

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]) -> AcceptanceReport:
        return cls(
            spec_version=d["spec_version"],
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
    spec_version: str = SPEC_VERSION

    def to_dict(self) -> dict[str, Any]:
        return {
            "spec_version": self.spec_version,
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
            spec_version=d["spec_version"],
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
    spec_version: str = SPEC_VERSION

    def to_dict(self) -> dict[str, Any]:
        return {
            "spec_version": self.spec_version,
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
            spec_version=d["spec_version"],
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
