# cdsci-publish

Shared release-bundle mechanics for cdsci-lake's public products: every published
table as flat Parquet, plus a product's own `<product>.duckdb`, `views.sql`,
`restore.sql` and `manifest.json`, in one local directory. See
[ADR-0027](../../docs/adr/0027-shared-release-bundle-package.md).

Domain meaning (table lists, column docs, provenance, base URL) stays with the
product. This package owns only the mechanics: streaming each SELECT to Parquet,
building the DuckDB views, and writing the manifest.

## Install

This is not published to PyPI. Consumers (bioc-on-ice, cancer-on-ice, ...) install
it by git subdirectory, pinned to a cdsci-lake revision:

```
cdsci-publish @ git+https://github.com/seandavi/cdsci-lake@<rev>#subdirectory=packages/cdsci-publish
```

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
