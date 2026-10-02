"""Release ids, dataset index, latest pointer and retention (versioned datasets)."""

from __future__ import annotations

import dataclasses
import json
from datetime import date
from pathlib import Path, PurePosixPath

import pytest
from fixtures.contracts import dataset as fx

from cdsci.lake.publish.builder import LocalDirStore
from cdsci.lake.publish.index import (
    DatasetIndex,
    IndexedRelease,
    load_index,
    next_release_id,
    pin_release,
    promote_release,
    prune_releases,
)
from cdsci.lake.publish.release import (
    ArtifactStatus,
    ReleaseCandidate,
    ReleaseManifest,
    TableFileIndex,
    check_release_id,
    release_date,
    release_sort_key,
)

DATASET = "demo-catalog"


@pytest.mark.parametrize("release", ["2026-10-02", "2026-10-02.2", "2026-10-02.10"])
def test_check_release_id_accepts(release: str):
    check_release_id(release)


@pytest.mark.parametrize(
    "release", ["R1", "2026-13-01", "2026-10-02.1", "2026-10-02.0", "2026-10-02.", "2026-10-2"]
)
def test_check_release_id_rejects(release: str):
    with pytest.raises(ValueError):
        check_release_id(release)


def test_release_sort_key_orders_numerically_not_lexically():
    assert release_sort_key("2026-10-02.10") > release_sort_key("2026-10-02.2")
    assert "2026-10-02.10" < "2026-10-02.2"  # the trap the key avoids
    assert release_sort_key("2026-10-02") < release_sort_key("2026-10-02.2")
    assert release_sort_key("2026-10-03") > release_sort_key("2026-10-02.99")
    assert release_sort_key("2026-10-02") == ("2026-10-02", 1)


def test_release_date_is_the_date_part():
    assert release_date("2026-10-02.3") == "2026-10-02"


def test_release_types_validate_the_release_id():
    with pytest.raises(ValueError):
        ReleaseManifest(
            dataset=DATASET, release="R1", status=ArtifactStatus.STAGED, run_id="r", tables=()
        )
    with pytest.raises(ValueError):
        TableFileIndex(table="t", release="R1", files=())
    with pytest.raises(ValueError):
        ReleaseCandidate(
            dataset=DATASET, release="R1", run_id="r", built_at="x", destination="d",
            tables=(), status=ArtifactStatus.STAGED,
        )


def test_manifest_emits_release_date_after_release_and_ignores_it_on_load():
    m = ReleaseManifest(
        dataset=DATASET, release="2026-10-02.2", status=ArtifactStatus.STAGED, run_id="r",
        tables=(),
    )
    keys = list(m.to_dict())
    assert keys[keys.index("release") + 1] == "release_date"
    assert m.to_dict()["release_date"] == "2026-10-02"
    assert ReleaseManifest.from_json(m.to_json()) == m


# --- index -----------------------------------------------------------------


def _published_manifest(release: str) -> ReleaseManifest:
    return ReleaseManifest(
        dataset=DATASET, release=release, status=ArtifactStatus.PUBLISHED, run_id="r",
        tables=(), published_at=f"{release[:10]}T00:00:00+00:00",
    )


def _put_manifest(store: LocalDirStore, release: str) -> ReleaseManifest:
    manifest = _published_manifest(release)
    store.put_if_absent(
        PurePosixPath(DATASET) / release / "manifest.json",
        manifest.to_json().encode(),
        content_type="application/json",
    )
    return manifest


def _promote(store: LocalDirStore, release: str) -> None:
    promote_release(store, _put_manifest(store, release))


def test_load_index_missing_file_is_empty(tmp_path: Path):
    index = load_index(LocalDirStore(tmp_path), DATASET)
    assert index == DatasetIndex(dataset=DATASET, releases=())
    assert index.latest is None


def test_next_release_id_bare_date_then_numbered_suffixes():
    today = date(2026, 10, 2)
    index = DatasetIndex(dataset=DATASET, releases=())
    assert next_release_id(index, today) == "2026-10-02"

    def add(index: DatasetIndex, release: str) -> DatasetIndex:
        entry = IndexedRelease(release, release[:10], "t")
        return dataclasses.replace(index, releases=(*index.releases, entry))

    index = add(index, "2026-10-02")
    assert next_release_id(index, today) == "2026-10-02.2"
    index = add(index, "2026-10-02.2")
    assert next_release_id(index, today) == "2026-10-02.3"
    # a different day's ids don't count
    assert next_release_id(add(index, "2026-10-01"), date(2026, 10, 3)) == "2026-10-03"
    # ten same-day releases: .10 outranks .9 numerically
    for n in range(3, 11):
        index = add(index, f"2026-10-02.{n}")
    assert next_release_id(index, today) == "2026-10-02.11"


def test_promote_writes_index_and_latest(tmp_path: Path):
    store = LocalDirStore(tmp_path)
    _promote(store, "2026-10-02")
    _promote(store, "2026-10-02.2")
    index = load_index(store, DATASET)
    assert [r.release for r in index.releases] == ["2026-10-02", "2026-10-02.2"]
    assert index.latest == "2026-10-02.2"
    latest = json.loads((tmp_path / DATASET / "latest.json").read_text())
    assert latest == {
        "spec_version": "2.0",
        "dataset": DATASET,
        "release": "2026-10-02.2",
        "release_date": "2026-10-02",
        "manifest": "2026-10-02.2/manifest.json",
    }
    on_disk = json.loads((tmp_path / DATASET / "releases.json").read_text())
    assert on_disk["releases"][0] == {
        "release": "2026-10-02", "release_date": "2026-10-02",
        "published_at": "2026-10-02T00:00:00+00:00", "pinned": False,
    }


def test_promoting_an_older_id_never_moves_latest_backwards(tmp_path: Path):
    store = LocalDirStore(tmp_path)
    _promote(store, "2026-10-05")
    _promote(store, "2026-10-02")
    assert [r.release for r in load_index(store, DATASET).releases] == [
        "2026-10-02", "2026-10-05",
    ]
    assert json.loads((tmp_path / DATASET / "latest.json").read_text())["release"] == "2026-10-05"


def test_promote_refuses_non_published_manifest(tmp_path: Path):
    store = LocalDirStore(tmp_path)
    staged = dataclasses.replace(_published_manifest("2026-10-02"), status=ArtifactStatus.STAGED)
    with pytest.raises(ValueError, match="published"):
        promote_release(store, staged)
    assert not (tmp_path / DATASET / "releases.json").exists()


def test_promote_refuses_missing_manifest_json(tmp_path: Path):
    store = LocalDirStore(tmp_path)
    with pytest.raises(ValueError, match=r"manifest\.json"):
        promote_release(store, _published_manifest("2026-10-02"))
    assert not (tmp_path / DATASET / "releases.json").exists()


def test_promote_refuses_duplicate(tmp_path: Path):
    store = LocalDirStore(tmp_path)
    manifest = _put_manifest(store, "2026-10-02")
    promote_release(store, manifest)
    with pytest.raises(ValueError, match="already"):
        promote_release(store, manifest)


def test_pin_release_requires_indexed_release_and_can_unpin(tmp_path: Path):
    store = LocalDirStore(tmp_path)
    _promote(store, "2026-10-02")
    with pytest.raises(ValueError, match="not in the release index"):
        pin_release(store, DATASET, "2026-10-09")
    assert pin_release(store, DATASET, "2026-10-02").releases[0].pinned is True
    assert load_index(store, DATASET).releases[0].pinned is True
    assert pin_release(store, DATASET, "2026-10-02", pinned=False).releases[0].pinned is False


def _contract(keep_last: int | None):
    return dataclasses.replace(fx.DATASET_CONTRACT, keep_last=keep_last)


def test_prune_is_a_noop_without_keep_last(tmp_path: Path):
    store = LocalDirStore(tmp_path)
    for day in ("01", "02", "03"):
        _promote(store, f"2026-10-{day}")
    assert prune_releases(store, _contract(None)) == ()
    assert len(load_index(store, DATASET).releases) == 3


def test_prune_keeps_newest_n_and_pins(tmp_path: Path):
    store = LocalDirStore(tmp_path)
    for release in ("2026-10-01", "2026-10-02", "2026-10-03", "2026-10-03.2", "2026-10-03.10"):
        _promote(store, release)
    pin_release(store, DATASET, "2026-10-02")
    deleted = prune_releases(store, _contract(2))
    assert deleted == ("2026-10-01", "2026-10-03")
    assert [r.release for r in load_index(store, DATASET).releases] == [
        "2026-10-02", "2026-10-03.2", "2026-10-03.10",
    ]
    assert not (tmp_path / DATASET / "2026-10-01").exists()
    assert not (tmp_path / DATASET / "2026-10-03").exists()
    assert (tmp_path / DATASET / "2026-10-02").exists()
    assert (tmp_path / DATASET / "2026-10-03.10").exists()
    latest = json.loads((tmp_path / DATASET / "latest.json").read_text())
    assert latest["release"] == "2026-10-03.10"


def test_prune_with_fewer_releases_than_keep_last_deletes_nothing(tmp_path: Path):
    store = LocalDirStore(tmp_path)
    for day in ("01", "02", "03", "04"):
        _promote(store, f"2026-10-{day}")
    # keep_last between len and 2*len once sliced from a negative start and pruned the oldest.
    assert prune_releases(store, _contract(7)) == ()
    assert len(load_index(store, DATASET).releases) == 4


def test_local_store_replace_delete_tree_open_and_put_file(tmp_path: Path):
    store = LocalDirStore(tmp_path / "s")
    p = PurePosixPath("a/b.json")
    store.replace(p, b"1", content_type="application/json")
    store.replace(p, b"2", content_type="application/json")
    assert store.get(p) == b"2"
    assert not (tmp_path / "s" / "a" / "b.json.tmp").exists()
    with store.open(p) as f:
        assert f.read() == b"2"
    with pytest.raises(FileNotFoundError):
        store.open(PurePosixPath("nope"))
    store.delete_tree(PurePosixPath("a"))
    store.delete_tree(PurePosixPath("a"))  # absent: no-op
    assert not (tmp_path / "s" / "a").exists()

    src = tmp_path / "src.bin"
    src.write_bytes(b"data")
    dest = PurePosixPath("d/f.bin")
    store.put_file_if_absent(dest, src, content_type="x")
    assert not src.exists()  # moved
    same = tmp_path / "same.bin"
    same.write_bytes(b"data")
    store.put_file_if_absent(dest, same, content_type="x")  # equal digest: no-op
    other = tmp_path / "other.bin"
    other.write_bytes(b"different")
    with pytest.raises(FileExistsError):
        store.put_file_if_absent(dest, other, content_type="x")
    assert store.get(dest) == b"data"


@pytest.mark.parametrize("bad", ["../escape", "/abs/path", "a/../../escape", "."])
def test_local_store_rejects_paths_outside_root(tmp_path: Path, bad: str):
    store = LocalDirStore(tmp_path / "root")
    (tmp_path / "root").mkdir()
    victim = tmp_path / "escape"
    victim.mkdir()
    with pytest.raises(ValueError, match="escapes"):
        store.delete_tree(PurePosixPath(bad))
    with pytest.raises(ValueError, match="escapes"):
        store.put_if_absent(PurePosixPath(bad), b"x", content_type="x")
    assert victim.exists()


def test_acceptance_report_and_receipt_validate_release_id():
    from cdsci.lake.publish.release import AcceptanceReport, PublicationReceipt

    with pytest.raises(ValueError):
        AcceptanceReport(dataset=DATASET, release="R1", run_id="r", checked_at="t", checks=())
    with pytest.raises(ValueError):
        PublicationReceipt(
            dataset=DATASET, release="R1", format="parquet", destination="d",
            schema_digest="s", run_id="r", status=ArtifactStatus.PUBLISHED,
        )
