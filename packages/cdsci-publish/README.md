# cdsci-publish

Shared release-bundle mechanics for cdsci-lake's public products: every published
table as flat Parquet, plus a product's own `<product>.duckdb`, `views.sql`,
`restore.sql` and `manifest.json`, in one local directory. See
[ADR-0027](../../docs/adr/0027-shared-release-bundle-package.md).

Domain meaning (table lists, column docs, provenance, base URL) stays with the
product. This package owns only the mechanics: streaming each SELECT to Parquet,
building the DuckDB views, writing the manifest, gating the bundle against the previous
release, uploading it to a bucket (`v{release}/` + `latest/`, with retention) and
pushing it to a Hugging Face dataset repo as one tagged commit.

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

## Croissant

Pass `dataset=` to `publish(...)` and the bundle also gets `croissant.json`,
[Croissant 1.1](https://docs.mlcommons.org/croissant/docs/croissant-spec-1.1.html) JSON-LD
generated from the manifest: a `FileObject` per table (contentUrl, sha256, contentSize)
and a `RecordSet` per table with a `Field` per column (the DuckDB type mapped to a
Croissant data type, the column doc as description, `LIST` as `isArray`, `STRUCT` as
`subField`). `upload` and `hf_push` carry it along, contentUrls regenerated per folder
(`v{release}/`, `latest/`) or download-relative (Hugging Face), like `views.sql`.

```python
publish(
    ...,
    dataset={                                  # merged onto the sc:Dataset; the spec requires
        "name": "biocOnIce",                   # name, description, url and license
        "description": "...",
        "url": "https://github.com/seandavi/bioc-on-ice",
        "license": "https://creativecommons.org/licenses/by/4.0/",
        "creator": {"@type": "Person", "name": "Sean Davis"},
        "citeAs": "...", "keywords": ["genomics"],
        "@context": {"wdt": "http://www.wikidata.org/prop/direct/"},  # extra prefixes
    },
    field_semantics={                          # optional, per column, merged onto the Field
        "annotation.gene": {"gene_id": {"equivalentProperty": "wdt:P594"}},
    },
)
```

`version` is the manifest's `release` and `datePublished` its `published_at` unless
`dataset` overrides them. A column whose DuckDB type has no Croissant data type (`BLOB`,
`MAP`, ...) is emitted as `sc:Text` with the raw type in its description;
`cdsci.publish.croissant.unmapped(doc)` lists them. The manifest's `provenance` rows are
not emitted (no clean PROV-O mapping yet). `croissant(manifest, dataset=..., field_semantics=...)`
is also callable on its own, stdlib only; the dev group's `mlcroissant` validates the output
in tests.

`cdsci.publish.concordance(old_manifest, new_manifest)` compares two manifests
(table set, row counts, column names/types, docs) and returns a list of
human-readable differences, ignoring build-time fields (timestamps, git_sha,
bytes, sha256).

## Gate

```python
from cdsci.publish.gate import check_proof, gate, readme_queries

passed, checks = gate(
    Path("release"), "https://data.example.org/latest",   # or a directory, or None
    queries=readme_queries(Path("README.md"), fence="sql bundle"),  # optional checks
    catalog="bioc",                           # views as bioc.<schema>.<table> (optional)
    keys={"annotation.gene": ("gene_id",)},   # not null and unique
    references={"annotation.exon": {"gene_id": "annotation.gene.gene_id"}},
    row_bounds=(0.9, 1.5), growth_required=True,  # shrink below 0.9x fails; past 1.5x fails
    allow_dropped={"annotation.gene": ("valid_from", "valid_to")},  # owner-approved
    allow_column_change=(),                   # owner-approved: any change, recorded
    extra_checks=lambda con, manifest, ref: [...],  # the product's own Check(...)s
)
check_proof(Path("release"))  # before upload / hf_push: gate.json matches manifest.json
```

Reads the bundle's Parquet and manifest.json, and the reference's `manifest.json` only.
The reference must be an older release; an HTTP 404 there means a first release and runs
the bundle checks alone. A pass prints the report and writes `gate.json` into the bundle,
bound to its `manifest.json` by sha256; pass it to `upload` / `hf_push` as an
`extra_files` entry.

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
