"""Push a release bundle to a Hugging Face dataset repo (ADR-0027 step 4b) as one commit,
tagged. The archive copy: users download a tag and query it locally.

The best of bioc-on-ice's and cancer-on-ice's `hf.py` (both descend from omicidx's
`hf_release`). The push sends the bundle's Parquet and manifest.json as they are, plus
`extra_files` (a gate proof, say), and generates three files: views.sql and restore.sql
over paths relative to the download directory (the bundle's own copies read the bucket),
and README.md, the dataset card the product's `card` builds. The product's `<db_file>`
is not pushed: its views read the bucket. Nothing points at `hf://`: anonymous reads
there are rate-limited (HTTP 429).

Rules kept from the products:

- One `create_commit`, not `upload_folder`: with hf_xet installed, `upload_folder` splits a
  large upload into several commits, and a release is exactly one. Remote files not in
  this release leave `main` (they stay reachable at older tags); `.gitattributes` stays.
- If the tag exists and holds this bundle's manifest.json byte for byte, nothing is done,
  so a re-fired timer is harmless. If it holds a different one, the push fails: a release
  is never replaced. Tags are never deleted.
- Every Parquet file is re-hashed against its manifest sha256 before anything is written.
- The Hub can answer 409 Conflict to a tag on a commit it made a moment ago; the tag is
  retried. A run that still fails to tag is repaired by the next run: it commits again (a
  no-op on the Hub) and tags.
- Dry run unless `apply`: the plan is printed, and nothing is created or written.

Product policy is all parameters: the repo, the tag (`YYYY-MM` of the release for a
monthly archive, the release date for every-release), the card builder, the commit
message and `extra_files`. Credentials are not read here: the caller builds `api`
(`HfApi(token=...)`).
"""

import json
import time
from pathlib import Path

from huggingface_hub import CommitOperationAdd, CommitOperationDelete
from huggingface_hub.errors import HfHubHTTPError, RepositoryNotFoundError

from .bundle import check_files, restore_sql, views_sql

TAG_ATTEMPTS, TAG_WAIT_S = 5, 10


def hf_push(bundle: Path, api, *, repo, tag, card, apply=False, extra_files=(), message=None):
    """Push `bundle` to the dataset `repo` through `api` (an `HfApi`) as one commit tagged
    `tag`. Returns the commit sha, or None when nothing was pushed.

    `card(tag, manifest, tables) -> str` builds README.md; `tables` is the manifest's tables,
    each with its file's `bytes`. `extra_files` are bundle files pushed verbatim.
    """
    manifest_bytes = (bundle / "manifest.json").read_bytes()
    manifest = json.loads(manifest_bytes)
    message = message or f"{repo} release {tag} (v{manifest['release']})"
    try:
        tags = {t.name for t in api.list_repo_refs(repo, repo_type="dataset").tags}
        remote = api.list_repo_files(repo, repo_type="dataset")
    except RepositoryNotFoundError:  # a dry run before the first push
        tags, remote = set(), []
    if tag in tags:
        pushed = Path(api.hf_hub_download(repo, "manifest.json", repo_type="dataset", revision=tag))
        if pushed.read_bytes() != manifest_bytes:
            raise SystemExit(f"{repo}@{tag} exists with a different manifest.json; a release "
                             "is never replaced: publish the changes as the next release")
        print(f"{repo}@{tag} already exists with this manifest.json; nothing to do")
        return None
    check_files(bundle, manifest)

    tables = [{**t, "bytes": (bundle / t["path"]).stat().st_size} for t in manifest["tables"]]
    generated = {
        "views.sql": views_sql(tables, "."),
        "restore.sql": restore_sql(tables, "."),
        "README.md": card(tag, manifest, tables),
    }
    files = [t["path"] for t in tables] + ["manifest.json", *extra_files]
    ops = [CommitOperationAdd(p, bundle / p) for p in files]
    ops += [CommitOperationAdd(p, text.encode()) for p, text in generated.items()]
    keep = {*files, *generated}
    deletes = [CommitOperationDelete(p) for p in remote if p not in keep and p != ".gitattributes"]
    for p in files:
        print(f"put    {p}  ({(bundle / p).stat().st_size:,} bytes)", flush=True)
    for p, text in generated.items():
        print(f"put    {p}  (generated, {len(text.encode()):,} bytes)", flush=True)
    for d in deletes:
        print(f"delete {d.path_in_repo}", flush=True)
    if not apply:
        print(f"dry run: would push {repo}@{tag}; nothing written; --apply to push")
        return None
    # Created only now, after the bundle checked out: an invalid bundle writes nothing.
    api.create_repo(repo, repo_type="dataset", private=False, exist_ok=True)
    commit = api.create_commit(repo, ops + deletes, repo_type="dataset", commit_message=message)
    for attempt in range(TAG_ATTEMPTS):
        try:
            api.create_tag(repo, repo_type="dataset", tag=tag, revision=commit.oid,
                           tag_message=message)
            break
        except HfHubHTTPError as e:
            if getattr(e.response, "status_code", None) != 409 or attempt == TAG_ATTEMPTS - 1:
                raise
            time.sleep(TAG_WAIT_S * (attempt + 1))
    print(f"pushed {repo}@{tag} ({commit.oid})")
    return commit.oid
