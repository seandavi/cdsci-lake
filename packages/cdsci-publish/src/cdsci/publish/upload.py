"""Upload a release bundle to a bucket (ADR-0027 step 4b): `v{release}/`, then `latest/`.

The best of bioc-on-ice's and cancer-on-ice's `upload.py` (both descend from omicidx's
`publish_bundle`). Each folder gets the bundle's Parquet plus its own views.sql,
restore.sql and `<db_file>`, regenerated for that folder's public URL, so a pinned
`v{release}/` keeps reading its own Parquet and `latest/` reads `latest/`. manifest.json
goes last in each folder: it marks the folder complete. Then dated folders the product's
retention policy does not keep are pruned. Dry run unless `apply`: everything is built
and listed, nothing is written to the bucket.

Rules kept from the products:

- A completed dated folder (manifest.json present) of other files is never overwritten.
  One without a manifest is an interrupted upload and is retried; one complete with this
  bundle's exact files means `latest/` was interrupted, so only `latest/` is finished.
- `latest/` loses its manifest.json before anything in it is overwritten, so an
  interrupted replacement never looks complete.
- Every Parquet file is re-hashed against its manifest sha256 before anything is written.
- Only completed dated folders count for retention; an incomplete one older than this
  release is pruned outright, a newer one is left alone. The release being uploaded is
  always kept. Folders whose name is not `v{YYYY-MM-DD}` of a real date are never touched.
- Each file is sent to its exact key through pyarrow's `copy_files`, retried: R2 rejects a
  multipart upload whose non-trailing parts differ in length (a buffered `.open("wb")` hit
  it); pyarrow sends uniform 10 MiB parts, which puts the ceiling near 100 GiB per file.
  `latest/` is a second upload from the local bundle, not a server-side copy: R2 times out
  copying multi-GB objects.

Product policy is all parameters: the bucket, its public URL, the DB file name, the
retention policy (`keep_newest`, `keep_daily_weekly`, `keep_monthly`, or any callable of
the same shape), and `extra_files` copied verbatim beside manifest.json (a gate proof,
say). Credentials are not read here: the caller builds `fs` (see `r2_filesystem`).
"""

import json
import re
import shutil
import tempfile
import time
from datetime import UTC, date, datetime
from pathlib import Path

from pyarrow import fs as pafs

from .bundle import build_db, check_files, restore_sql, views_sql

DATED = re.compile(r"v\d{4}-\d{2}-\d{2}")


def r2_filesystem(account_id, access_key, secret_key):
    """pyarrow's S3 client on a Cloudflare R2 account. The caller resolves the credentials."""
    return pafs.S3FileSystem(access_key=access_key, secret_key=secret_key,
                             endpoint_override=f"{account_id}.r2.cloudflarestorage.com",
                             region="auto")


# --- retention policies: (complete dated releases, this release, today) -> releases to keep,
# `current` always among them (`upload` keeps it regardless, whatever a policy returns).


def keep_newest(n):
    """The newest `n` releases by date, this one counted (bioc-on-ice ADR-0016: weekly, 4)."""
    # n < 1 would prune every release but this one, and a negative slice the wrong ones.
    if not isinstance(n, int) or isinstance(n, bool) or n < 1:
        raise ValueError(f"keep_newest: n must be an integer >= 1, got {n!r}")

    def policy(dates, current, today):
        return set(sorted(dates | {current}, reverse=True)[:n])
    return policy


def keep_daily_weekly(daily, weekly):
    """The newest `daily` releases, plus the newest of each of the newest `weekly` ISO weeks
    (omicidx: 7 and 5). Counts, not ages, so a run of failed nights never empties the window."""
    def policy(dates, current, today):
        newest_first = sorted(dates | {current}, reverse=True)
        weeks = {}
        for d in newest_first:
            weeks.setdefault(d.isocalendar()[:2], d)
        weekly_newest = sorted(weeks.values(), reverse=True)[:weekly]
        return set(newest_first[:daily]) | set(weekly_newest) | {current}
    return policy


def keep_monthly(months, grace_days):
    """The newest release of each of the newest `months` calendar months that have one, plus
    any release whose same-month successor is under `grace_days` old (cancer-on-ice ADR-0001:
    12 and 7). Months are counted, not folders, so a month with no release does not shorten
    the window. Ages are in days of `today`."""
    def policy(dates, current, today):
        by_month = {}
        for d in sorted(dates | {current}):
            by_month.setdefault((d.year, d.month), []).append(d)
        keep = {by_month[m][-1] for m in sorted(by_month, reverse=True)[:months]} | {current}
        for group in by_month.values():
            for d, successor in zip(group, group[1:], strict=False):
                if (today - successor).days < grace_days:
                    keep.add(d)
        return keep
    return policy


# --- mechanics


def _put(fs, local, key, apply, attempts=3):
    """One file to its exact key; retried, since that is idempotent."""
    print(f"put    {key}  ({Path(local).stat().st_size:,} bytes)", flush=True)
    if not apply:
        return
    for attempt in range(1, attempts + 1):
        try:
            pafs.copy_files(str(local), key, source_filesystem=pafs.LocalFileSystem(),
                            destination_filesystem=fs)
            return
        except OSError as e:
            if attempt == attempts or isinstance(e, FileNotFoundError):
                raise
            print(f"  retry {attempt}/{attempts - 1} after: {e}", flush=True)
            time.sleep(10 * attempt)


def _exists(fs, key):
    return fs.get_file_info(key).type != pafs.FileType.NotFound


def _folder(fs, root, bundle, manifest, folder, public_url, db_file, extra_files, apply):
    """Upload the bundle into `root/folder/`, with its SQL, DuckDB and manifest for that URL."""
    base = f"{public_url}/{folder}"
    stale = f"{root}/{folder}/manifest.json"
    if folder == "latest" and _exists(fs, stale):
        print(f"delete {stale}", flush=True)
        if apply:
            fs.delete_file(stale)
    for t in manifest["tables"]:
        _put(fs, bundle / t["path"], f"{root}/{folder}/{t['path']}", apply)
    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        views = views_sql(manifest["tables"], base)
        (tmp / "views.sql").write_text(views)
        (tmp / "restore.sql").write_text(restore_sql(manifest["tables"], base))
        (tmp / "manifest.json").write_text(
            json.dumps({**manifest, "base_url": base}, indent=2) + "\n")
        for name in extra_files:
            shutil.copy(bundle / name, tmp / name)
        build_db(tmp / db_file, views, base, bundle)
        for name in ("views.sql", "restore.sql", db_file, *extra_files, "manifest.json"):
            _put(fs, tmp / name, f"{root}/{folder}/{name}", apply)


def _date(name):
    """The date a `v{YYYY-MM-DD}` folder names, or None for anything else (never pruned)."""
    try:
        return date.fromisoformat(name[1:]) if DATED.fullmatch(name) else None
    except ValueError:
        return None


def _expired(fs, root, current, retention, today):
    """Dated folders under `root` to delete; `current` never is."""
    names = {i.base_name for i in fs.get_file_info(pafs.FileSelector(root))
             if i.type == pafs.FileType.Directory and _date(i.base_name)}
    complete = {n for n in names if _exists(fs, f"{root}/{n}/manifest.json")}
    kept = retention({_date(n) for n in complete}, _date(current), today)
    keep = {f"v{d.isoformat()}" for d in kept} | {current}
    return sorted(n for n in names if n not in keep and (n in complete or n < current))


def _prune(fs, root, current, retention, today, apply):
    for name in _expired(fs, root, current, retention, today or datetime.now(UTC).date()):
        print(f"delete {root}/{name}/", flush=True)
        if apply:
            fs.delete_dir(f"{root}/{name}")


def _files(manifest):
    return sorted((t["path"], t.get("sha256")) for t in manifest["tables"])


def prune(fs, root, *, retention, apply=False, today=None):
    """Only the pruning, around the release `latest/` serves; no bundle needed."""
    done = f"{root}/latest/manifest.json"
    if not _exists(fs, done):
        raise SystemExit(f"{done} not found; nothing to prune around")
    with fs.open_input_stream(done) as f:
        current = f"v{json.loads(f.read()).get('release')}"
    if not _date(current):
        raise SystemExit(f"{done} names no dated release ({current!r}); not pruning")
    _prune(fs, root, current, retention, today, apply)
    if not apply:
        print("dry run: nothing deleted; --apply to prune")


def upload(bundle: Path, fs, root, *, public_url, db_file, retention, apply=False,
           extra_files=(), today=None):
    """Upload `bundle` (a `cdsci.publish.publish` directory) to `root/v{release}/` and
    `root/latest/` on `fs`, then prune what `retention` does not keep.

    `public_url` is where `root` is served (each folder's views read `{public_url}/{folder}`);
    `db_file` the product's DuckDB file name; `extra_files` bundle files copied verbatim into
    each folder before manifest.json; `today` the date retention ages against (UTC today).
    """
    manifest = json.loads((bundle / "manifest.json").read_text())
    dated = f"v{manifest.get('release')}"
    if not _date(dated):
        raise SystemExit(f"manifest.json release {manifest.get('release')!r} is not a "
                         "YYYY-MM-DD date")
    check_files(bundle, manifest)
    folders = (dated, "latest")
    done = f"{root}/{dated}/manifest.json"
    if _exists(fs, done):
        with fs.open_input_stream(done) as f:
            remote = json.loads(f.read())
        if _files(remote) != _files(manifest):
            raise SystemExit(f"{root}/{dated}/ is a completed release of other files; "
                             "dated releases are never overwritten")
        print(f"{root}/{dated}/ already complete with this bundle; finishing latest/", flush=True)
        folders = ("latest",)
    for folder in folders:
        _folder(fs, root, bundle, manifest, folder, public_url.rstrip("/"), db_file,
                tuple(extra_files), apply)
    _prune(fs, root, dated, retention, today, apply)
    if not apply:
        print("dry run: nothing written; --apply to upload")
