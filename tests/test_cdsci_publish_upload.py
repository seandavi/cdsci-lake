"""cdsci.publish.upload (ADR-0027 step 4b): a bundle into a local directory standing in for
the bucket, offline. Retention policies as pure functions."""

import hashlib
import json
from datetime import date

import duckdb
import pytest
from cdsci.publish import croissant, upload
from cdsci.publish.upload import keep_daily_weekly, keep_monthly, keep_newest
from fsspec.implementations.local import LocalFileSystem
from pyarrow import fs as pafs

RELEASE = "2026-10-30"
PUBLIC = "https://example.org/data"
# Like S3, writes a key without its parent "directory" existing first.
BUCKET_FS = pafs.PyFileSystem(pafs.FSSpecHandler(LocalFileSystem(auto_mkdir=True)))


def _bundle(tmp_path, release=RELEASE, gene_id=1):
    out = tmp_path / f"release-{release}"
    (out / "annotation").mkdir(parents=True)
    parquet = out / "annotation/gene.parquet"
    duckdb.sql(f"COPY (SELECT {gene_id} AS gene_id) TO '{parquet}' (FORMAT parquet)")
    table = {"schema": "annotation", "table": "gene", "path": "annotation/gene.parquet",
             "row_count": 1, "comment": None, "columns": [],
             "sha256": hashlib.sha256(parquet.read_bytes()).hexdigest()}
    manifest = {"release": release, "base_url": "https://example.org/x", "tables": [table]}
    (out / "manifest.json").write_text(json.dumps(manifest))
    (out / "gate.json").write_text('{"passed": true}')
    return out


def _bucket(tmp_path, dated=("v2026-10-04", "v2026-10-11", "v2026-10-18", "v2026-10-25",
                             "v2026-10-29")):
    bucket = tmp_path / "bucket"
    for d in (*dated, "vnotadate", "latest"):
        (bucket / d).mkdir(parents=True)
        (bucket / d / "manifest.json").write_text("{}")
    return bucket


def _snapshot(bucket):
    return sorted(p.relative_to(bucket) for p in bucket.rglob("*"))


def _upload(bundle, bucket, **kw):
    kw = {"public_url": PUBLIC, "db_file": "product.duckdb", "retention": keep_newest(4),
          "today": date.fromisoformat(RELEASE), **kw}
    return upload.upload(bundle, BUCKET_FS, str(bucket), **kw)


def test_each_folder_reads_its_own_url_extra_files_travel_and_manifest_last(tmp_path, capsys):
    bucket, bundle = _bucket(tmp_path), _bundle(tmp_path)
    _upload(bundle, bucket, apply=True, extra_files=("gate.json",))

    for folder in (f"v{RELEASE}", "latest"):
        url = f"{PUBLIC}/{folder}/annotation/gene.parquet"
        assert f"read_parquet('{url}')" in (bucket / folder / "views.sql").read_text()
        assert f"read_parquet('{url}')" in (bucket / folder / "restore.sql").read_text()
        db = duckdb.connect(str(bucket / folder / "product.duckdb"), read_only=True)
        view = db.sql("SELECT sql FROM duckdb_views() WHERE view_name = 'gene'").fetchone()[0]
        assert url in view
        m = json.loads((bucket / folder / "manifest.json").read_text())
        assert m["base_url"] == f"{PUBLIC}/{folder}" and m["release"] == RELEASE
        assert (bucket / folder / "gate.json").read_text() == '{"passed": true}'
        assert (bucket / folder / "annotation/gene.parquet").exists()

    puts = [ln.split()[1] for ln in capsys.readouterr().out.splitlines() if ln.startswith("put")]
    assert puts[4:6] == [f"{bucket}/v{RELEASE}/gate.json", f"{bucket}/v{RELEASE}/manifest.json"]
    assert puts[-1] == f"{bucket}/latest/manifest.json"
    assert all("/latest/" not in p for p in puts[:6]) and all("/latest/" in p for p in puts[6:])
    # keep 4 of 6, the one-off counted; latest/ and non-dated names are untouched
    assert sorted(p.name for p in bucket.iterdir()) == [
        "latest", "v2026-10-18", "v2026-10-25", "v2026-10-29", f"v{RELEASE}", "vnotadate"]


def test_a_bundles_croissant_json_travels_with_each_folders_own_content_urls(tmp_path, capsys):
    bucket, bundle = _bucket(tmp_path), _bundle(tmp_path)
    manifest = json.loads((bundle / "manifest.json").read_text())
    doc = croissant({**manifest, "tables": [{**manifest["tables"][0], "bytes": 1}]},
                    dataset={"name": "p", "description": "d", "url": "u", "license": "l"})
    (bundle / "croissant.json").write_text(json.dumps(doc))
    _upload(bundle, bucket, apply=True)

    for folder in (f"v{RELEASE}", "latest"):
        got = json.loads((bucket / folder / "croissant.json").read_text())
        assert got["distribution"][0]["contentUrl"] == f"{PUBLIC}/{folder}/annotation/gene.parquet"
        assert got["recordSet"] == doc["recordSet"]
    puts = [ln.split()[1] for ln in capsys.readouterr().out.splitlines() if ln.startswith("put")]
    latest = [p for p in puts if "/latest/" in p]
    assert latest.index(f"{bucket}/latest/croissant.json") < latest.index(
        f"{bucket}/latest/manifest.json")


def test_dry_run_writes_nothing_and_lists_the_plan(tmp_path, capsys):
    bucket = _bucket(tmp_path)
    before = _snapshot(bucket)
    _upload(_bundle(tmp_path), bucket)

    assert _snapshot(bucket) == before
    out = capsys.readouterr().out
    assert [ln for ln in out.splitlines() if ln.startswith("delete")] == [
        f"delete {bucket}/latest/manifest.json",
        f"delete {bucket}/v2026-10-04/", f"delete {bucket}/v2026-10-11/"]
    assert out.count("put ") == 10 and out.endswith("--apply to upload\n")


def test_a_completed_dated_release_of_other_files_is_never_overwritten(tmp_path):
    bucket, bundle = _bucket(tmp_path), _bundle(tmp_path)
    (bucket / f"v{RELEASE}").mkdir()
    other = {"release": RELEASE, "tables": [{"path": "annotation/gene.parquet", "sha256": "other"}]}
    (bucket / f"v{RELEASE}" / "manifest.json").write_text(json.dumps(other))
    with pytest.raises(SystemExit, match="completed release of other files"):
        _upload(bundle, bucket, apply=True)
    assert json.loads((bucket / f"v{RELEASE}" / "manifest.json").read_text()) == other
    assert not (bucket / "latest" / "views.sql").exists()


def test_an_interrupted_dated_upload_is_retried_and_an_interrupted_latest_is_finished(
        tmp_path, capsys, monkeypatch):
    bucket, bundle = _bucket(tmp_path), _bundle(tmp_path)
    (bucket / f"v{RELEASE}" / "annotation").mkdir(parents=True)
    (bucket / f"v{RELEASE}" / "annotation" / "gene.parquet").write_text("partial")
    real = upload._folder

    def fail_on_latest(fs, root, bundle, manifest, folder, *a):
        if folder == "latest":
            raise OSError("connection reset")
        real(fs, root, bundle, manifest, folder, *a)
    monkeypatch.setattr(upload, "_folder", fail_on_latest)
    with pytest.raises(OSError):
        _upload(bundle, bucket, apply=True)
    monkeypatch.setattr(upload, "_folder", real)
    dated = (bucket / f"v{RELEASE}" / "manifest.json").read_text()
    assert json.loads(dated)["release"] == RELEASE
    capsys.readouterr()

    _upload(bundle, bucket, apply=True)
    assert (bucket / f"v{RELEASE}" / "manifest.json").read_text() == dated
    assert json.loads((bucket / "latest" / "manifest.json").read_text())["release"] == RELEASE
    assert f"/v{RELEASE}/" not in "".join(ln for ln in capsys.readouterr().out.splitlines()
                                           if ln.startswith("put"))


def test_latest_loses_its_manifest_before_any_overwrite(tmp_path, monkeypatch):
    bucket, bundle = _bucket(tmp_path), _bundle(tmp_path)
    real = upload._put

    def fail_on_first_latest_put(fs, local, key, apply, attempts=3):
        real(fs, local, key, apply, attempts)
        if "/latest/" in key:
            raise OSError("connection reset")
    monkeypatch.setattr(upload, "_put", fail_on_first_latest_put)
    with pytest.raises(OSError):
        _upload(bundle, bucket, apply=True)
    assert (bucket / "latest" / "annotation/gene.parquet").exists()
    assert not (bucket / "latest" / "manifest.json").exists()


def test_a_changed_parquet_under_an_unchanged_manifest_is_refused_first(tmp_path, capsys):
    bucket, bundle = _bucket(tmp_path), _bundle(tmp_path)
    before = _snapshot(bucket)
    duckdb.sql(f"COPY (SELECT 2 AS gene_id) TO '{bundle}/annotation/gene.parquet' (FORMAT parquet)")
    with pytest.raises(SystemExit, match="gene.parquet does not match its manifest.json sha256"):
        _upload(bundle, bucket, apply=True)
    assert _snapshot(bucket) == before and capsys.readouterr().out == ""


def test_a_release_that_is_not_a_date_is_refused(tmp_path):
    bucket, bundle = _bucket(tmp_path), _bundle(tmp_path, release="nope")
    with pytest.raises(SystemExit, match="not a YYYY-MM-DD date"):
        _upload(bundle, bucket, apply=True)


def test_an_incomplete_dated_folder_is_never_pruned_nor_counted(tmp_path):
    bucket, bundle = _bucket(tmp_path, dated=("v2026-10-25",)), _bundle(tmp_path)
    # Either may be another run's upload in progress: neither counts, neither is pruned.
    (bucket / "v2026-10-27").mkdir()
    (bucket / "v2026-11-06").mkdir()
    _upload(bundle, bucket, apply=True, retention=keep_newest(1))
    assert sorted(p.name for p in bucket.iterdir()) == [
        "latest", "v2026-10-27", f"v{RELEASE}", "v2026-11-06", "vnotadate"]


def test_a_partial_listing_prunes_less_never_more(tmp_path):
    bucket = _bucket(tmp_path)

    class Partial:  # a truncated listing: the newest folders missing
        def get_file_info(self, selector):
            if isinstance(selector, str):
                return BUCKET_FS.get_file_info(selector)
            return [i for i in BUCKET_FS.get_file_info(selector) if i.base_name < "v2026-10-25"]
    assert upload._expired(BUCKET_FS, str(bucket), f"v{RELEASE}", keep_newest(4),
                           date(2026, 10, 30)) == ["v2026-10-04", "v2026-10-11"]
    assert upload._expired(Partial(), str(bucket), f"v{RELEASE}", keep_newest(4),
                           date(2026, 10, 30)) == []


def test_prune_only_around_the_release_latest_serves(tmp_path, capsys):
    bucket = _bucket(tmp_path)
    (bucket / "latest" / "manifest.json").write_text(json.dumps({"release": "2026-10-04"}))
    before = _snapshot(bucket)
    upload.prune(BUCKET_FS, str(bucket), retention=keep_newest(1))
    assert _snapshot(bucket) == before and "--apply to prune" in capsys.readouterr().out
    upload.prune(BUCKET_FS, str(bucket), retention=keep_newest(1), apply=True)
    assert sorted(p.name for p in bucket.iterdir()) == [
        "latest", "v2026-10-04", "v2026-10-29", "vnotadate"]


@pytest.mark.parametrize("latest", [None, "{}", '{"release": "nope"}'])
def test_prune_only_refuses_without_a_dated_latest(tmp_path, latest):
    bucket = _bucket(tmp_path)
    if latest is None:
        (bucket / "latest" / "manifest.json").unlink()
    else:
        (bucket / "latest" / "manifest.json").write_text(latest)
    before = _snapshot(bucket)
    with pytest.raises(SystemExit):
        upload.prune(BUCKET_FS, str(bucket), retention=keep_newest(1), apply=True)
    assert _snapshot(bucket) == before


# --- retention policies


def _d(*s):
    return {date.fromisoformat(x) for x in s}


@pytest.mark.parametrize("n", [0, -1, 2.0, True])
def test_keep_newest_below_one_is_refused(n):
    with pytest.raises(ValueError, match="integer >= 1"):
        keep_newest(n)


def test_keep_newest_counts_the_current_release_even_if_older():
    dates, today = _d("2026-10-04", "2026-10-11", "2026-10-18"), date(2026, 10, 30)
    assert keep_newest(2)(dates, date(2026, 10, 1), today) == _d("2026-10-18", "2026-10-11")
    assert keep_newest(2)(dates, date(2026, 10, 30), today) == _d("2026-10-30", "2026-10-18")


def test_keep_daily_weekly_seven_dailies_and_five_weeklies():
    days = [date.fromordinal(date(2026, 9, 1).toordinal() + i) for i in range(61)]  # to Oct 31
    keep = keep_daily_weekly(7, 5)(set(days), date(2026, 10, 31), date(2026, 10, 31))
    # Oct 31 is a Saturday: that week's newest is Oct 31; the four before end on Sundays.
    assert keep == set(days[-7:]) | _d("2026-10-18", "2026-10-11", "2026-10-04")


def test_keep_daily_weekly_falls_back_to_the_weeks_newest_build_and_counts_not_ages():
    today = date(2026, 10, 31)
    days = {date.fromordinal(date(2026, 10, 1).toordinal() + i) for i in range(31)}
    keep = keep_daily_weekly(7, 5)(days - _d("2026-10-18"), today, today)
    assert date(2026, 10, 17) in keep and date(2026, 10, 18) not in keep
    old = _d("2026-01-05", "2026-01-12", "2026-01-19")
    assert keep_daily_weekly(7, 5)(old, today, today) == old | {today}
    assert keep_daily_weekly(0, 0)(_d("2026-10-30"), today, today) == {today}


def test_keep_monthly_newest_of_each_month_skipping_empty_months_and_a_year_boundary():
    policy = keep_monthly(12, 7)
    dates = _d("2026-01-10", "2026-01-20", "2026-03-05", "2026-06-30")
    assert policy(dates, date(2026, 6, 30), date(2026, 7, 30)) == dates - _d("2026-01-10")
    dates = _d("2025-12-31", "2026-01-01", "2026-02-01")
    assert policy(dates, date(2026, 2, 1), date(2026, 3, 1)) == dates
    thirteen = {date(2025, m, 1) for m in range(10, 13)} | {date(2026, m, 1) for m in range(1, 11)}
    assert policy(thirteen, date(2026, 10, 1), date(2026, 10, 1)) == thirteen - _d("2025-10-01")


@pytest.mark.parametrize("today, kept", [
    ("2026-05-16", _d("2026-05-01", "2026-05-10")),  # successor 6 days old: replaced release stays
    ("2026-05-17", _d("2026-05-10")),  # 7 days: it goes
])
def test_keep_monthly_grace_for_a_replaced_same_month_release(today, kept):
    dates = _d("2026-05-01", "2026-05-10")
    assert keep_monthly(12, 7)(dates, date(2026, 5, 10), date.fromisoformat(today)) == kept


def test_keep_monthly_grace_chains_through_corrections():
    dates = _d("2026-05-01", "2026-05-10", "2026-05-12")
    assert keep_monthly(12, 7)(dates, date(2026, 5, 12), date(2026, 5, 16)) == dates
    assert keep_monthly(12, 7)(dates, date(2026, 5, 12), date(2026, 6, 20)) == _d("2026-05-12")


def test_r2_filesystem_targets_the_account_endpoint():
    fs = upload.r2_filesystem("acct", "key", "secret")
    assert isinstance(fs, pafs.S3FileSystem) and fs.region == "auto"
