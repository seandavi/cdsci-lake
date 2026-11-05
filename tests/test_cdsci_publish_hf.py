"""cdsci.publish.hf (ADR-0027 step 4b): a bundle to a stand-in for the Hugging Face API, offline."""

import hashlib
import json
import shutil
from types import SimpleNamespace

import duckdb
import httpx2
import pytest
from cdsci.publish import croissant, hf
from huggingface_hub.errors import HfHubHTTPError, RepositoryNotFoundError

REPO = "me/product"


def _bundle(tmp_path, release="2026-11-17"):
    out = tmp_path / "release"
    tables = []
    for schema, table, n in (("annotation", "gene", 33), ("provenance", "release", 1)):
        (out / schema).mkdir(parents=True, exist_ok=True)
        path = out / schema / f"{table}.parquet"
        duckdb.sql(f"COPY (SELECT range AS id FROM range({n})) TO '{path}' (FORMAT parquet)")
        tables.append({"schema": schema, "table": table, "path": f"{schema}/{table}.parquet",
                       "row_count": n, "comment": None, "columns": [],
                       "sha256": hashlib.sha256(path.read_bytes()).hexdigest()})
    (out / "manifest.json").write_text(json.dumps({"release": release, "tables": tables}))
    (out / "gate.json").write_text('{"passed": true}')
    (out / "product.duckdb").write_bytes(b"not pushed")
    return out


def _card(tag, manifest, tables):
    return f"# Product {tag}\n\n{len(tables)} tables, {sum(t['bytes'] for t in tables)} bytes\n"


def _http_error(status, cls=HfHubHTTPError):
    req = httpx2.Request("POST", "https://huggingface.co/api/datasets/x")
    return cls(str(status), response=httpx2.Response(status, request=req))


class FakeHub:
    """The HfApi calls hf_push makes; `files` None means the repo does not exist.
    `manifests` maps a tag to the manifest.json bytes held at it."""

    def __init__(self, bundle, tmp, manifests=None, files=None):
        self.bundle, self.tmp = bundle, tmp
        self.manifests, self.files, self.calls = dict(manifests or {}), files, []
        self.tag_errors = []

    def create_repo(self, repo, **kw):
        self.calls.append(("create_repo", repo, kw))
        self.files = self.files if self.files is not None else [".gitattributes"]

    def _repo(self):
        if self.files is None:
            raise _http_error(404, RepositoryNotFoundError)

    def list_repo_refs(self, repo, **kw):
        self._repo()
        return SimpleNamespace(tags=[SimpleNamespace(name=t) for t in self.manifests])

    def list_repo_files(self, repo, **kw):
        self._repo()
        return self.files

    def hf_hub_download(self, repo, filename, revision, **kw):
        self.calls.append(("hf_hub_download", repo, filename, revision))
        path = self.tmp / f"{revision}-{filename}"
        path.write_bytes(self.manifests[revision])
        return str(path)

    def create_commit(self, repo, ops, **kw):
        self.calls.append(("create_commit", repo, ops, kw))
        return SimpleNamespace(oid="abc123")

    def create_tag(self, repo, **kw):
        self.calls.append(("create_tag", repo, kw))
        if self.tag_errors:
            raise self.tag_errors.pop(0)
        self.manifests[kw["tag"]] = (self.bundle / "manifest.json").read_bytes()


@pytest.fixture
def bundle(tmp_path):
    return _bundle(tmp_path)


@pytest.fixture
def hub(bundle, tmp_path):
    return lambda **kw: FakeHub(bundle, tmp_path, **kw)


def _push(bundle, api, **kw):
    kw = {"repo": REPO, "tag": "2026-11", "card": _card, "extra_files": ("gate.json",), **kw}
    return hf.hf_push(bundle, api, **kw)


def test_dry_run_writes_nothing_even_before_the_repo_exists(bundle, hub, capsys):
    h = hub()
    assert _push(bundle, h) is None
    assert h.calls == []
    out = capsys.readouterr().out
    assert "put    annotation/gene.parquet" in out and "put    gate.json" in out
    assert "product.duckdb" not in out
    assert out.endswith(f"dry run: would push {REPO}@2026-11; nothing written; --apply to push\n")


def test_apply_is_one_commit_tagged_with_relative_views(bundle, hub, tmp_path, monkeypatch):
    h = hub(manifests={"2026-10": b"{}"},
            files=[".gitattributes", "README.md", "old/dropped.parquet"])
    assert _push(bundle, h, apply=True) == "abc123"
    assert [c[0] for c in h.calls] == ["create_repo", "create_commit", "create_tag"]
    _, _, ops, kw = h.calls[1]
    added = {o.path_in_repo: o.path_or_fileobj for o in ops if hasattr(o, "path_or_fileobj")}
    assert set(added) == {"annotation/gene.parquet", "provenance/release.parquet",
                          "manifest.json", "gate.json", "views.sql", "restore.sql", "README.md"}
    assert [o.path_in_repo for o in ops if o.path_in_repo not in added] == ["old/dropped.parquet"]
    assert kw["commit_message"] == f"{REPO} release 2026-11 (v2026-11-17)"
    assert h.calls[2][2]["tag"] == "2026-11" and h.calls[2][2]["revision"] == "abc123"
    assert added["README.md"].decode().startswith("# Product 2026-11\n\n2 tables, ")
    assert "hf://" not in added["views.sql"].decode() + added["restore.sql"].decode()

    # The archived SQL reads the downloaded files, relative to the download directory.
    download = tmp_path / "download"
    shutil.copytree(bundle, download)
    monkeypatch.chdir(download)
    con = duckdb.connect()
    con.execute(added["views.sql"].decode())
    assert con.execute("SELECT count(*) FROM annotation.gene").fetchone()[0] == 33
    duckdb.connect().execute(added["restore.sql"].decode())  # errors if a row count differs


def test_a_bundles_croissant_json_is_pushed_with_download_relative_content_urls(bundle, hub):
    manifest = json.loads((bundle / "manifest.json").read_text())
    tables = [{**t, "bytes": 1} for t in manifest["tables"]]
    doc = croissant({**manifest, "base_url": "https://example.org/latest", "tables": tables},
                    dataset={"name": "p", "description": "d", "url": "u", "license": "l"})
    (bundle / "croissant.json").write_text(json.dumps(doc))
    h = hub(files=[".gitattributes"])
    _push(bundle, h, apply=True)
    _, _, ops, _ = h.calls[1]
    pushed = json.loads(next(o.path_or_fileobj for o in ops if o.path_in_repo == "croissant.json"))
    assert [f["contentUrl"] for f in pushed["distribution"]] == [
        "./annotation/gene.parquet", "./provenance/release.parquet"]
    assert pushed["recordSet"] == doc["recordSet"]


def test_a_second_run_with_the_same_manifest_is_a_no_op(bundle, hub, capsys):
    h = hub(files=[".gitattributes"])
    _push(bundle, h, apply=True)
    assert _push(bundle, h, apply=True) is None
    assert [c[0] for c in h.calls] == ["create_repo", "create_commit", "create_tag",
                                       "hf_hub_download"]
    assert capsys.readouterr().out.endswith(
        f"{REPO}@2026-11 already exists with this manifest.json; nothing to do\n")


def test_an_existing_tag_with_another_manifest_fails(bundle, hub):
    h = hub(manifests={"2026-11": b'{"release": "2026-11-17", "other": true}'},
            files=[".gitattributes"])
    with pytest.raises(SystemExit, match="exists with a different manifest.json"):
        _push(bundle, h, apply=True)
    assert [c[0] for c in h.calls] == ["hf_hub_download"]


def test_parquet_changed_after_publish_is_refused(bundle, hub):
    (bundle / "annotation/gene.parquet").write_bytes(b"not the published file")
    h = hub(files=[".gitattributes"])
    with pytest.raises(SystemExit, match="gene.parquet does not match its manifest"):
        _push(bundle, h, apply=True)
    assert h.calls == []  # an invalid bundle writes nothing, not even the repo


def test_a_failed_tag_is_retried_by_the_next_run(bundle, hub):
    h = hub(files=[".gitattributes"])
    h.tag_errors = [OSError("tag failed")]
    with pytest.raises(OSError, match="tag failed"):
        _push(bundle, h, apply=True)
    _push(bundle, h, apply=True)  # no tag yet: commit again (a no-op on the Hub), then tag
    assert [c[0] for c in h.calls] == ["create_repo", "create_commit", "create_tag"] * 2
    assert list(h.manifests) == ["2026-11"]


def test_a_409_on_the_tag_right_after_the_commit_is_retried_other_errors_are_not(
        bundle, hub, monkeypatch):
    monkeypatch.setattr(hf, "TAG_WAIT_S", 0)
    h = hub()
    h.tag_errors = [_http_error(409), _http_error(409)]
    _push(bundle, h, apply=True)
    assert [c[0] for c in h.calls] == ["create_repo", "create_commit"] + ["create_tag"] * 3
    assert "2026-11" in h.manifests

    h = hub()
    h.tag_errors = [_http_error(403)]
    with pytest.raises(HfHubHTTPError):
        _push(bundle, h, apply=True)
    assert [c[0] for c in h.calls].count("create_tag") == 1
