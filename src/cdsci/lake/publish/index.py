"""``cdsci.lake.publish.index`` — per-dataset release index, ``latest`` pointer, retention.

Layout under a dataset's store prefix::

    <dataset>/releases.json   every promoted release, ascending by release_sort_key
    <dataset>/latest.json     pointer to the newest release's manifest

Both are pointer files, rewritten atomically with ``ObjectStore.replace`` (index first).
Releases are immutable full snapshots; retention is per dataset (``keep_last``) and a
pinned release is never pruned. Ordering always goes through
:func:`~cdsci.lake.publish.release.release_sort_key`, never raw string comparison.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, replace
from datetime import date
from pathlib import PurePosixPath
from typing import Any

from ..contracts import DatasetContract
from ..log import event
from .builder import ObjectStore
from .release import (
    SPEC_VERSION,
    ArtifactStatus,
    ReleaseManifest,
    check_release_id,
    release_date,
    release_sort_key,
)

INDEX_FILENAME = "releases.json"
LATEST_FILENAME = "latest.json"
_JSON_CONTENT_TYPE = "application/json"


@dataclass(frozen=True)
class IndexedRelease:
    release: str
    release_date: str
    published_at: str
    pinned: bool = False

    def __post_init__(self) -> None:
        check_release_id(self.release)

    def to_dict(self) -> dict[str, Any]:
        return {
            "release": self.release,
            "release_date": self.release_date,
            "published_at": self.published_at,
            "pinned": self.pinned,
        }

    @classmethod
    def from_dict(cls, d: Any) -> IndexedRelease:
        return cls(
            release=d["release"],
            release_date=d["release_date"],
            published_at=d["published_at"],
            pinned=bool(d.get("pinned", False)),
        )


@dataclass(frozen=True)
class DatasetIndex:
    dataset: str
    releases: tuple[IndexedRelease, ...]
    spec_version: str = SPEC_VERSION

    @property
    def latest(self) -> str | None:
        """The newest release id by ``release_sort_key``; ``None`` for an empty index."""
        if not self.releases:
            return None
        return max((r.release for r in self.releases), key=release_sort_key)

    def to_json(self) -> str:
        ordered = sorted(self.releases, key=lambda r: release_sort_key(r.release))
        return json.dumps(
            {
                "spec_version": self.spec_version,
                "dataset": self.dataset,
                "releases": [r.to_dict() for r in ordered],
            },
            indent=2,
        )

    @classmethod
    def from_json(cls, s: str) -> DatasetIndex:
        d = json.loads(s)
        return cls(
            dataset=d["dataset"],
            releases=tuple(IndexedRelease.from_dict(r) for r in d["releases"]),
            spec_version=d["spec_version"],
        )


def _index_path(dataset: str) -> PurePosixPath:
    return PurePosixPath(dataset) / INDEX_FILENAME


def _write_index(store: ObjectStore, index: DatasetIndex) -> None:
    store.replace(
        _index_path(index.dataset), index.to_json().encode(), content_type=_JSON_CONTENT_TYPE
    )


def load_index(store: ObjectStore, dataset: str) -> DatasetIndex:
    """Read ``<dataset>/releases.json``; a missing file is an empty index."""
    try:
        body = store.get(_index_path(dataset))
    except FileNotFoundError:
        return DatasetIndex(dataset=dataset, releases=())
    return DatasetIndex.from_json(body.decode())


def next_release_id(index: DatasetIndex, today: date) -> str:
    """``YYYY-MM-DD`` for ``today``, or ``YYYY-MM-DD.N`` with ``N`` one above the highest
    rank already indexed for that day."""
    day = today.isoformat()
    ranks = [
        release_sort_key(r.release)[1] for r in index.releases if release_date(r.release) == day
    ]
    if not ranks:
        return day
    return f"{day}.{max(ranks) + 1}"


def promote_release(store: ObjectStore, manifest: ReleaseManifest) -> DatasetIndex:
    """Add a published release to the index and point ``latest.json`` at the newest.

    Raises ``ValueError`` for a non-published manifest, a release with no
    ``manifest.json`` in ``store``, or a release already indexed. ``latest`` is the
    maximum by ``release_sort_key``, so promoting an older id never moves it backwards.
    """
    if manifest.status != ArtifactStatus.PUBLISHED:
        raise ValueError(
            f"{manifest.dataset} {manifest.release}: only a published release can be promoted "
            f"(status={manifest.status.value})"
        )
    manifest_path = PurePosixPath(manifest.dataset) / manifest.release / "manifest.json"
    try:
        store.get(manifest_path)
    except FileNotFoundError as exc:
        raise ValueError(
            f"{manifest.dataset} {manifest.release}: no manifest.json in the store, "
            "refusing to promote"
        ) from exc
    index = load_index(store, manifest.dataset)
    if any(r.release == manifest.release for r in index.releases):
        raise ValueError(f"{manifest.dataset} {manifest.release}: already in the release index")
    entry = IndexedRelease(
        release=manifest.release,
        release_date=release_date(manifest.release),
        published_at=manifest.published_at or "",
    )
    index = replace(index, releases=(*index.releases, entry))
    index = replace(
        index, releases=tuple(sorted(index.releases, key=lambda r: release_sort_key(r.release)))
    )
    latest = index.latest
    assert latest is not None
    _write_index(store, index)
    store.replace(
        PurePosixPath(manifest.dataset) / LATEST_FILENAME,
        json.dumps(
            {
                "spec_version": SPEC_VERSION,
                "dataset": manifest.dataset,
                "release": latest,
                "release_date": release_date(latest),
                "manifest": f"{latest}/manifest.json",
            },
            indent=2,
        ).encode(),
        content_type=_JSON_CONTENT_TYPE,
    )
    event(
        "release_promoted",
        run_id=manifest.run_id,
        asset=f"release.{manifest.dataset}.{manifest.release}",
        release=manifest.release,
        status=manifest.status.value,
    )
    return index


def pin_release(
    store: ObjectStore, dataset: str, release: str, *, pinned: bool = True
) -> DatasetIndex:
    """Set (or clear) the pin on an indexed release. A pinned release is never pruned."""
    index = load_index(store, dataset)
    if not any(r.release == release for r in index.releases):
        raise ValueError(f"{dataset} {release}: not in the release index")
    index = replace(
        index,
        releases=tuple(
            replace(r, pinned=pinned) if r.release == release else r for r in index.releases
        ),
    )
    _write_index(store, index)
    return index


def prune_releases(store: ObjectStore, contract: DatasetContract) -> tuple[str, ...]:
    """Apply ``contract.keep_last``: keep the newest N releases plus every pinned one,
    delete the rest from the store and the index, and return the deleted ids (sorted).

    ``()`` when ``keep_last`` is ``None``. ``latest`` is always among the newest, so it is
    never pruned.
    """
    if contract.keep_last is None:
        return ()
    index = load_index(store, contract.id)
    ordered = sorted(index.releases, key=lambda r: release_sort_key(r.release))
    newest = {r.release for r in ordered[len(ordered) - contract.keep_last :]}
    doomed = tuple(r.release for r in ordered if r.release not in newest and not r.pinned)
    if not doomed:
        return ()
    for release in doomed:
        store.delete_tree(PurePosixPath(contract.id) / release)
    _write_index(
        store, replace(index, releases=tuple(r for r in ordered if r.release not in doomed))
    )
    event(
        "releases_pruned",
        asset=f"release.{contract.id}",
        releases=list(doomed),
        kept=len(ordered) - len(doomed),
    )
    return doomed
