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


def sql_literal(s):
    """`s` as a single-quoted SQL string literal."""
    return "'" + s.replace("'", "''") + "'"


def _url(base_url, t):
    return f"{base_url}/{t['path']}"


def default_path(schema, table):
    """`<schema>/<table>.parquet`, the layout `publish` uses unless given another."""
    return f"{schema}/{table}.parquet"


def views_sql(tables, base_url):
    """One view per table over its absolute Parquet URL, with the declared comments."""
    stmts = [f"CREATE SCHEMA IF NOT EXISTS {s}" for s in sorted({t["schema"] for t in tables})]
    for t in tables:
        name = f"{t['schema']}.{t['table']}"
        url = sql_literal(_url(base_url, t))
        stmts.append(f"CREATE OR REPLACE VIEW {name} AS SELECT * FROM read_parquet({url})")
        if t["comment"]:
            stmts.append(f"COMMENT ON VIEW {name} IS {sql_literal(t['comment'])}")
        stmts += [f'COMMENT ON COLUMN {name}."{c["name"]}" IS {sql_literal(c["doc"])}'
                  for c in t["columns"] if c["doc"]]
    return ";\n".join(stmts) + ";\n"


def restore_sql(tables, base_url):
    """A materialised local copy of every table, then a row-count check against manifest.json."""
    stmts = [f"CREATE SCHEMA IF NOT EXISTS {s}" for s in sorted({t["schema"] for t in tables})]
    stmts += [f"CREATE OR REPLACE TABLE {t['schema']}.{t['table']} AS "
              f"SELECT * FROM read_parquet({sql_literal(_url(base_url, t))})" for t in tables]
    manifest_url = sql_literal(base_url + "/manifest.json")
    counts = "\n    UNION ALL ".join(
        f"SELECT '{t['schema']}' AS schema, '{t['table']}' AS \"table\", count(*) AS n "
        f"FROM {t['schema']}.{t['table']}" for t in tables)
    stmts.append(
        "SELECT CASE WHEN count(*) = 0 THEN 'row counts match manifest.json'\n"
        "  ELSE error('row counts differ from manifest.json: ' || "
        "string_agg(r.schema || '.' || r.\"table\", ', ')) END AS restore_check\n"
        f"FROM (\n    {counts}\n) r\n"
        "LEFT JOIN (SELECT t.schema, t.\"table\", t.row_count FROM "
        f"(SELECT unnest(tables) AS t FROM read_json({manifest_url}))) m\n"
        '  ON r.schema = m.schema AND r."table" = m."table"\n'
        "WHERE m.row_count IS DISTINCT FROM r.n")
    return ";\n".join(stmts) + ";\n"


def check_files(bundle, manifest):
    """Every table's Parquet in `bundle` matches its manifest sha256, streamed; SystemExit if not.

    Disk-bound: sha256 runs ~1.6 GB/s, so a cold 94 GB bundle takes about six minutes,
    against an hours-long upload.
    """
    for t in manifest["tables"]:
        with (bundle / t["path"]).open("rb") as f:
            if hashlib.file_digest(f, "sha256").hexdigest() != t["sha256"]:
                raise SystemExit(f"{bundle}/{t['path']} does not match its manifest.json "
                                 "sha256; re-publish this bundle")


def _git_sha(cwd):
    """The build's code version; None outside a checkout."""
    try:
        return subprocess.run(["git", "rev-parse", "HEAD"], cwd=cwd,
                              capture_output=True, text=True, check=True).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return None


class _HttpAsLocal(DirFileSystem):
    protocol = ("http", "https")


def build_db(path, sql, base_url, out):
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
    path: Callable[[str, str], str] = default_path,
) -> dict:
    """Build one release bundle in `out` from `con`, which has the source(s) attached.

    Each table streams to `path(schema, table)` under `out`, by default
    `<schema>/<table>.parquet` (ZSTD), through COPY, so nothing is
    materialised in Python. manifest.json is written last, atomically: it marks the bundle
    complete. `out` must be new or empty.

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
    # A new or empty `out` only: nothing from an earlier run can sit beside a manifest
    # that does not list it. A caller re-running after a crash clears its own directory.
    if out.exists() and any(out.iterdir()):
        raise ValueError(f"{out} is not empty: publish into a new directory")
    out.mkdir(parents=True, exist_ok=True)
    # Row order is not part of the bundle, and keeping it makes COPY buffer much more.
    con.execute("SET preserve_insertion_order = false")
    built = []
    for schema, table, sql in selected:
        name = f"{schema}.{table}"
        rel = path(schema, table)
        file = out / rel
        file.parent.mkdir(parents=True, exist_ok=True)
        copy_sql = f"COPY ({sql}) TO {sql_literal(str(file))} (FORMAT parquet, COMPRESSION zstd)"
        n = con.execute(copy_sql).fetchone()[0]
        with file.open("rb") as f:
            sha256 = hashlib.file_digest(f, "sha256").hexdigest()
        comment, col_docs = docs(name)
        cols = con.sql(f"DESCRIBE SELECT * FROM read_parquet({sql_literal(str(file))})").fetchall()
        columns = [{"name": c[0], "type": c[1], "doc": col_docs.get(c[0])} for c in cols]
        built.append({"schema": schema, "table": table, "path": rel,
                      "row_count": n, "bytes": file.stat().st_size, "sha256": sha256,
                      "comment": comment, "columns": columns})
        print(f"{name:45} {n:>14,} rows {file.stat().st_size:>16,} bytes", flush=True)

    provenance = []
    if provenance_sql is not None:
        prov = con.sql(provenance_sql)
        provenance = [dict(zip(prov.columns, r, strict=False)) for r in prov.fetchall()]
    # default=str: provenance rows may carry dates/timestamps (cancer-on-ice's do).
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
    build_db(out / db_file, views, base_url, out)
    (out / "manifest.json.tmp").write_text(json.dumps(manifest, indent=2, default=str) + "\n")
    (out / "manifest.json.tmp").replace(out / "manifest.json")
    print(f"{len(built)} tables, {sum(t['row_count'] for t in built):,} rows, "
          f"{sum(t['bytes'] for t in built):,} bytes -> {out}")
    return manifest
