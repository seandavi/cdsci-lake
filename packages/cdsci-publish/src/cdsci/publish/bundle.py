"""Release bundle mechanics (ADR-0027): every published table as flat Parquet, plus a
product's `<db_file>`, `views.sql`, `restore.sql` and `manifest.json`, in one local
directory.

Extracted unchanged in behaviour from bioc-on-ice's `publish.py` (ADR-0014/0015).
Everything product-specific -- the table list, column/table docs, the provenance
query, the DB file name, the base URL, and the code checkout `git_sha` is read from
-- is a parameter here, not a hardcoded value.
"""

import hashlib
import json
import subprocess
import tempfile
from collections.abc import Callable, Collection, Sequence
from datetime import UTC, datetime
from pathlib import Path

import duckdb
from fsspec.implementations.dirfs import DirFileSystem
from fsspec.implementations.local import LocalFileSystem

MANIFEST_VERSION = 1

TableSpec = tuple[str, str, str]
DocsLookup = Callable[[str], tuple[str | None, dict[str, str]]]


def _lit(s):
    return "'" + s.replace("'", "''") + "'"


def _url(base_url, t):
    return f"{base_url}/{t['schema']}/{t['table']}.parquet"


def views_sql(tables, base_url):
    """One view per table over its absolute Parquet URL, with the declared comments."""
    stmts = [f"CREATE SCHEMA IF NOT EXISTS {s}" for s in sorted({t["schema"] for t in tables})]
    for t in tables:
        name = f"{t['schema']}.{t['table']}"
        url = _lit(_url(base_url, t))
        stmts.append(f"CREATE OR REPLACE VIEW {name} AS SELECT * FROM read_parquet({url})")
        if t["comment"]:
            stmts.append(f"COMMENT ON VIEW {name} IS {_lit(t['comment'])}")
        stmts += [f'COMMENT ON COLUMN {name}."{c["name"]}" IS {_lit(c["doc"])}'
                  for c in t["columns"] if c["doc"]]
    return ";\n".join(stmts) + ";\n"


def restore_sql(tables, base_url):
    """A materialised local copy of every table, then a row-count check against manifest.json."""
    stmts = [f"CREATE SCHEMA IF NOT EXISTS {s}" for s in sorted({t["schema"] for t in tables})]
    stmts += [f"CREATE OR REPLACE TABLE {t['schema']}.{t['table']} AS "
              f"SELECT * FROM read_parquet({_lit(_url(base_url, t))})" for t in tables]
    counts = "\n    UNION ALL ".join(
        f"SELECT '{t['schema']}' AS schema, '{t['table']}' AS \"table\", count(*) AS n "
        f"FROM {t['schema']}.{t['table']}" for t in tables)
    stmts.append(
        "SELECT CASE WHEN count(*) = 0 THEN 'row counts match manifest.json'\n"
        "  ELSE error('row counts differ from manifest.json: ' || "
        "string_agg(r.schema || '.' || r.\"table\", ', ')) END AS restore_check\n"
        f"FROM (\n    {counts}\n) r\n"
        "LEFT JOIN (SELECT t.schema, t.\"table\", t.row_count FROM "
        f"(SELECT unnest(tables) AS t FROM read_json({_lit(base_url + '/manifest.json')}))) m\n"
        '  ON r.schema = m.schema AND r."table" = m."table"\n'
        "WHERE m.row_count IS DISTINCT FROM r.n")
    return ";\n".join(stmts) + ";\n"


def _git_sha(cwd):
    """The build's code version; None outside a checkout."""
    try:
        return subprocess.run(["git", "rev-parse", "HEAD"], cwd=cwd,
                              capture_output=True, text=True, check=True).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return None


class _HttpAsLocal(DirFileSystem):
    protocol = ("http", "https")


def _build_db(path, sql, base_url, out):
    """Write `<db_file>` from views.sql before anything is at `base_url`.

    DuckDB binds a view (reads the Parquet footer) when it is created, so an https base
    that is not uploaded yet would fail. For the build only, http(s) reads are served
    from `out` by an fsspec filesystem standing in for httpfs; the stored view SQL
    keeps the absolute URL.
    """
    path.unlink(missing_ok=True)
    con = duckdb.connect(str(path), config={"autoload_known_extensions": False})
    with tempfile.TemporaryDirectory() as root:
        scheme, rest = base_url.split("://", 1)
        if scheme in ("http", "https"):
            link = Path(root, rest)
            link.parent.mkdir(parents=True, exist_ok=True)
            link.symlink_to(out.absolute())
            con.register_filesystem(_HttpAsLocal(root, LocalFileSystem()))
        con.execute(sql)
    con.close()


def publish(
    con,
    out: Path,
    *,
    tables: Sequence[TableSpec],
    docs: DocsLookup,
    base_url: str,
    db_file: str,
    code_dir: Path,
    provenance_sql: str | None = None,
    only: Collection[str] | None = None,
) -> dict:
    """Build one release bundle in `out` from `con`, which has the source(s) attached.

    Each table streams to `<schema>/<table>.parquet` (ZSTD) through COPY, so nothing is
    materialised in Python. manifest.json is written last, atomically: it marks the bundle
    complete, so a stale one from an earlier run is removed first.

    `tables`: the product's `(schema, table, SELECT)` list. `docs`: `"schema.table"` ->
    `(comment, {column: doc})`. `code_dir`: the product's own checkout, for
    `manifest["git_sha"]` (not this package's). `provenance_sql`: the product's
    provenance query, or `None` for no provenance section.
    """
    selected = [t for t in tables if only is None or f"{t[0]}.{t[1]}" in only]
    unknown = set(only or ()) - {f"{s}.{t}" for s, t, _ in tables}
    if unknown:
        raise ValueError(f"not a published table: {sorted(unknown)}")
    base_url = base_url.rstrip("/")
    (out / "manifest.json").unlink(missing_ok=True)
    # Row order is not part of the bundle, and keeping it makes COPY buffer much more.
    con.execute("SET preserve_insertion_order = false")
    built = []
    for schema, table, sql in selected:
        name = f"{schema}.{table}"
        path = out / schema / f"{table}.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        copy_sql = f"COPY ({sql}) TO {_lit(str(path))} (FORMAT parquet, COMPRESSION zstd)"
        n = con.execute(copy_sql).fetchone()[0]
        with path.open("rb") as f:
            sha256 = hashlib.file_digest(f, "sha256").hexdigest()
        comment, col_docs = docs(name)
        cols = con.sql(f"DESCRIBE SELECT * FROM read_parquet({_lit(str(path))})").fetchall()
        columns = [{"name": c[0], "type": c[1], "doc": col_docs.get(c[0])} for c in cols]
        built.append({"schema": schema, "table": table, "path": f"{schema}/{table}.parquet",
                      "row_count": n, "bytes": path.stat().st_size, "sha256": sha256,
                      "comment": comment, "columns": columns})
        print(f"{name:45} {n:>14,} rows {path.stat().st_size:>16,} bytes", flush=True)

    provenance = []
    if provenance_sql is not None:
        prov = con.sql(provenance_sql)
        provenance = [dict(zip(prov.columns, r, strict=False)) for r in prov.fetchall()]
    now = datetime.now(UTC)
    manifest = {
        "manifest_version": MANIFEST_VERSION,
        "release": now.date().isoformat(),
        "published_at": now.isoformat(timespec="seconds"),
        "git_sha": _git_sha(code_dir),
        "base_url": base_url,
        "tables": built,
        # what the data was built from: every provenance row at build time
        "provenance": provenance,
    }
    views = views_sql(built, base_url)
    (out / "views.sql").write_text(views)
    (out / "restore.sql").write_text(restore_sql(built, base_url))
    _build_db(out / db_file, views, base_url, out)
    (out / "manifest.json.tmp").write_text(json.dumps(manifest, indent=2) + "\n")
    (out / "manifest.json.tmp").replace(out / "manifest.json")
    print(f"{len(built)} tables, {sum(t['row_count'] for t in built):,} rows, "
          f"{sum(t['bytes'] for t in built):,} bytes -> {out}")
    return manifest
