"""``cdsci.lake.publish.pipeline`` — one-call publish of a dataset release."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, date, datetime
from pathlib import PurePosixPath

import duckdb

from ..contracts import DatasetContract
from .builder import LocalDirStore, build_release, finalize_release, record_release
from .frozen import build_frozen_ducklake
from .index import load_index, next_release_id, promote_release, prune_releases
from .release import ArtifactStatus, ReleaseCandidate, ReleaseManifest, SourceAssetVersion
from .verify import verify_release


def publish_release(
    store: LocalDirStore,
    *,
    contract: DatasetContract,
    tables: Mapping[str, duckdb.DuckDBPyRelation],
    source_asset_versions: tuple[SourceAssetVersion, ...],
    run_id: str,
    today: date | None = None,
    con: duckdb.DuckDBPyConnection | None = None,
) -> ReleaseManifest:
    """Build, verify, finalize, index and prune one full-snapshot release of ``contract``.

    The release id is the UTC build date (``YYYY-MM-DD``, ``.N`` for a later release the
    same day). If a directory for the chosen id already exists without a
    ``manifest.json`` it is an incomplete leftover of a failed run, so it is deleted and the
    id reused. With a ``manifest.json`` it is a published release whose promotion failed
    afterwards; it is promoted into the index and the next id is used. This assumes a
    **single writer per dataset store**: concurrent publishers of one dataset would race
    on the id and the index.

    A release that fails acceptance raises out of :func:`finalize_release`: no
    ``manifest.json`` is written and the index is left untouched. ``con``, when given, is
    a lake connection that receives the ``lake_ops`` publication receipt.
    """
    if set(tables) != set(contract.tables):
        missing = sorted(set(contract.tables) - set(tables))
        unexpected = sorted(set(tables) - set(contract.tables))
        raise ValueError(
            f"{contract.id}: tables must be exactly the contract's tables "
            f"(missing: {missing}, unexpected: {unexpected})"
        )

    build_day = today or datetime.now(UTC).date()
    while True:
        index = load_index(store, contract.id)
        release = next_release_id(index, build_day)
        leftover = PurePosixPath(contract.id) / release
        try:
            raw = store.get(leftover / "manifest.json")
        except FileNotFoundError:
            store.delete_tree(leftover)  # incomplete staging output of a failed run
            break
        # A manifest.json means finalize_release already published this id and a later
        # step (receipt or promotion) failed: finish its promotion instead of deleting it.
        promote_release(store, ReleaseManifest.from_json(raw.decode()))

    candidate = ReleaseCandidate(
        dataset=contract.id,
        release=release,
        run_id=run_id,
        built_at=datetime.now(UTC).isoformat(),
        destination=f"{contract.id}/{release}",
        tables=tuple(sorted(tables)),
        status=ArtifactStatus.STAGED,
        source_asset_versions=source_asset_versions,
    )
    manifest = build_release(candidate, tables, store, contract=contract)
    if "ducklake" in contract.required_artifacts:
        manifest = build_frozen_ducklake(store, manifest, contract=contract)
    report = verify_release(store, contract.id, release, manifest=manifest, contract=contract)
    published = finalize_release(store, manifest, report, contract)
    if con is not None:
        record_release(con, published, report)
    promote_release(store, published)
    prune_releases(store, contract)
    return published
