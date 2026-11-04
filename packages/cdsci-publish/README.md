# cdsci-publish

Shared release-bundle mechanics for cdsci-lake's public products: every published
table as flat Parquet, plus a product's own `<product>.duckdb`, `views.sql`,
`restore.sql` and `manifest.json`, in one local directory. See
[ADR-0027](../../docs/adr/0027-shared-release-bundle-package.md).

Domain meaning (table lists, column docs, provenance, base URL) stays with the
product. This package owns only the mechanics: streaming each SELECT to Parquet,
building the DuckDB views, writing the manifest, uploading the bundle to a bucket
(`v{release}/` + `latest/`, with retention) and pushing it to a Hugging Face dataset
repo as one tagged commit.

## Install

This is not published to PyPI. Consumers (bioc-on-ice, cancer-on-ice, ...) install
it by git subdirectory, pinned to a cdsci-lake revision:

```
cdsci-publish @ git+https://github.com/seandavi/cdsci-lake@<rev>#subdirectory=packages/cdsci-publish
```

Extras: `[upload]` (pyarrow, for `cdsci.publish.upload`) and `[hf]` (huggingface-hub,
for `cdsci.publish.hf`). The base install needs neither.

## Use

```python
from pathlib import Path
from cdsci.publish import publish

manifest = publish(
    con,                      # a DuckDB connection with the source tables attached
    Path("release"),
    tables=[("annotation", "gene", "SELECT * FROM bioc.annotation.gene")],
    docs=lambda name: (None, {}),   # (comment, {column: doc}) for "schema.table"
    base_url="https://example.org/latest",
    db_file="product.duckdb",
    code_dir=Path(__file__).parent,  # the product's own checkout, for manifest["git_sha"]
    provenance_sql="SELECT * FROM bioc.provenance.release ORDER BY ALL",  # or None
)
```

`cdsci.publish.concordance(old_manifest, new_manifest)` compares two manifests
(table set, row counts, column names/types, docs) and returns a list of
human-readable differences, ignoring build-time fields (timestamps, git_sha,
bytes, sha256).

## Upload

```python
from cdsci.publish.upload import keep_newest, r2_filesystem, upload

fs = r2_filesystem(account_id, access_key, secret_key)  # the product resolves these
upload(
    Path("release"), fs, "data-product",       # bucket (or any pyarrow fs root)
    public_url="https://data.example.org",     # each folder's views read {public_url}/{folder}
    db_file="product.duckdb",
    retention=keep_newest(4),                  # or keep_daily_weekly(7, 5), keep_monthly(12, 7)
    extra_files=("gate.json",),                # bundle files copied beside manifest.json
    apply=False,                               # dry run: the plan is printed, nothing written
)
```

A completed `v{release}/` of other files is never overwritten; `latest/` is always
rewritten for its own URL; every Parquet file is checked against its manifest sha256
first. `prune(fs, root, retention=...)` prunes alone, around the release `latest/`
serves. A retention policy is any `(complete_dates, current, today) -> set[date]`.

## Hugging Face push

```python
from huggingface_hub import HfApi
from cdsci.publish.hf import hf_push

hf_push(
    Path("release"), HfApi(token=token),      # the product resolves the token
    repo="me/product",
    tag="2026-11",                             # product policy: YYYY-MM, or the release date
    card=lambda tag, manifest, tables: "...",  # README.md, the dataset card
    extra_files=("gate.json",),
    apply=False,
)
```

One commit, tagged; `views.sql`/`restore.sql` regenerated over download-relative paths;
the product's DuckDB file not pushed. An existing tag holding this manifest.json is a
no-op; one holding another fails. Tags are never deleted.
