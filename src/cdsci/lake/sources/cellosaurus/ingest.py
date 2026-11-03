"""Cellosaurus flat file -> ``lake.cellosaurus.line`` (EL only; bioc-on-ice#183).

Cellosaurus (CALIPHO group, SIB; CC BY 4.0) is the cell-line registry: one entry
per line, each with a stable CVCL_ accession (168,970 in release 56.0). Ported
from bioc-on-ice's ``src/bioconice/cellosaurus.py`` (its
``raw.cellosaurus__release`` landing); bioc-on-ice derives
``annotation.cellosaurus__{cell_line,xref,disease}`` from this table at release
time (bioc-on-ice ADR-0015).

``cellosaurus.txt`` is landed, not the .obo or .xml: it is the one form that
needs no interpretation to land. Each body line is ``XX   value`` (a two-letter
line code, three spaces, the value), entries end with ``//``, and the file opens
with a header (banner, version, licence, the line-code table).

**One row per line of the file, header included**, in file order: the
``line_number``, the entry's ``accession``, the line ``code`` and the ``value``.
``code || '   ' || value`` ordered by ``line_number`` is the file again. The
parse is the lander's SQL verbatim, so ``SELECT *`` reproduces raw without
``landed_in``. The key is ``line_number``.

**Versioned by the file's own header** (`` Version: 57.0``), as
``cellosaurus_version``. Upstream serves only the current release from one
rolling URL and keeps no dated copies, so ``version`` cannot fetch an older one:
it is checked against the header, and an older release lands from a local copy
(``file``).

The whole file is checked before anything lands, not just a limited load's
lines: at least one entry, a version line, every body line ``XX   value`` or
``//``, and every entry opened by ID, carrying an AC line and closed by ``//``.
What the values mean is bioc-on-ice's to check.
"""

from __future__ import annotations

import re
from pathlib import Path

import duckdb

from ... import ops
from ...config import Settings, get_settings
from ...connect import LAKE, csv_source, lake_connect, raw_dir, upsert
from ...download import download

_RAW = "cellosaurus"
_STAGE = "_cellosaurus_line"


def file_version(path: Path) -> str | None:
    """The release the file's header names (`` Version: 57.0`` -> ``57.0``)."""
    with open(path, encoding="utf-8", errors="replace") as f:
        for line in f:
            if line.startswith("ID   "):
                break
            if m := re.match(r" Version: (\S+)", line):
                return m.group(1)
    return None


def download_txt(settings: Settings | None = None) -> Path:
    """Download the current release into the raw layer, keyed by the version its header names."""
    s = settings or get_settings()
    d = raw_dir(_RAW, s)
    # The URL is rolling: a cached copy under this name would be a stale release.
    fresh = d / "cellosaurus.txt"
    fresh.unlink(missing_ok=True)
    download(s.cellosaurus_url, fresh)
    if not (version := file_version(fresh)):
        raise ValueError(f"cellosaurus: {s.cellosaurus_url} header carries no ' Version:' line")
    return fresh.replace(d / f"{version}-cellosaurus.txt")


def _parse_sql(path: Path) -> str:
    # The lander's parse, verbatim but for its landed_in column.
    # The file is read as one string and split on newlines, rather than through
    # read_csv: the split keeps blank header lines and gives every line its
    # ordinal deterministically, which a parallel CSV scan does not promise.
    # ponytail: the whole file is held in memory (117 MB -> 3.6 GB peak RSS);
    # stream it if a release ever grows past what one string can hold (4 GB).
    return f"""
        WITH f AS (SELECT string_split(content, chr(10)) AS lines
                   FROM read_text({csv_source([path])})),
        l AS (SELECT unnest(range(1, len(lines) + 1)) AS line_number, unnest(lines) AS line,
                     len(lines) AS n FROM f),
        -- The header runs up to the first entry, which an ID line opens. The
        -- empty string after the file's final newline is not a line.
        b AS (SELECT line_number, line,
                     line_number >= min(line_number) FILTER (starts_with(line, 'ID   ')) OVER ()
                         AS body
              FROM l WHERE NOT (line_number = n AND line = '')),
        -- Entries are '//'-terminated, so a line's entry is the number of
        -- terminators before it. The accession is the entry's AC line; the ID
        -- line comes first, so it cannot simply be carried downwards. A running
        -- sum less the line's own terminator, not a frame ending at 1 PRECEDING:
        -- DuckDB evaluates that frame with a FILTER quadratically (148 s here).
        e AS (SELECT *, CASE WHEN body THEN left(line, 2) END AS code,
                     CASE WHEN NOT body THEN line WHEN line <> '//' THEN substr(line, 6) END
                         AS value,
                     sum((body AND line = '//')::INT) OVER (ORDER BY line_number)
                         - (body AND line = '//')::INT AS entry
              FROM b)
        SELECT line_number,
               CASE WHEN body THEN max(value) FILTER (code = 'AC') OVER (PARTITION BY entry) END
                   AS accession,
               code, value,
               max(regexp_extract(line, '^ Version: (\\S+)', 1)) FILTER (NOT body) OVER ()
                   AS cellosaurus_version,
               body AND NOT regexp_matches(line, '^([A-Z]{{2}}   .*|//)$') AS malformed
        FROM e ORDER BY line_number
    """


def _stage(con: duckdb.DuckDBPyConnection, path: Path, version: str) -> None:
    """Parse the whole file into a temp table and check its structure; raise before landing."""
    con.execute(f"CREATE OR REPLACE TEMP TABLE {_STAGE} AS {_parse_sql(path)}")
    lines, entries, found, malformed, first_malformed, no_ac, unclosed, last = con.execute(f"""
        SELECT count(*), count(*) FILTER (code = 'ID'), any_value(cellosaurus_version),
               count(*) FILTER (malformed), min(line_number) FILTER (malformed),
               min(line_number) FILTER (code IS NOT NULL AND accession IS NULL),
               -- an ID line follows the header (NULL code) or a terminator
               min(line_number) FILTER (code = 'ID' AND coalesce(prev, '//') <> '//'),
               arg_max(code, line_number) FILTER (code IS NOT NULL)
        FROM (SELECT *, lag(code) OVER (ORDER BY line_number) AS prev FROM {_STAGE})
    """).fetchone()
    if not lines:
        raise ValueError(f"cellosaurus: {path} yielded no lines")
    if not entries:
        raise ValueError(f"cellosaurus: {path} has no entries (no 'ID   ' line)")
    if not found:
        raise ValueError(f"cellosaurus: {path} header carries no ' Version:' line")
    if found != version:
        raise ValueError(f"cellosaurus: {path} is release {found}, not {version}; upstream "
                         "serves only the current release, so land an older one from a file")
    if malformed:
        raise ValueError(f"cellosaurus: {path} has {malformed} body lines that are neither "
                         f"'XX   value' nor '//', first at line {first_malformed}")
    if no_ac:
        raise ValueError(f"cellosaurus: {path} line {no_ac} is in an entry with no AC line")
    if unclosed:
        raise ValueError(f"cellosaurus: {path} line {unclosed} opens an entry before the "
                         "last one is closed by '//'")
    if last != "//":
        raise ValueError(f"cellosaurus: {path} does not end with '//' (truncated?)")


def curate(
    con: duckdb.DuckDBPyConnection,
    path: Path,
    version: str,
    *,
    target: str | None = None,
    limit: int | None = None,
) -> int:
    """MERGE-upsert one release's lines into ``line`` on ``line_number``; return the row count.

    The file is parsed and checked whole before it lands; ``limit`` then takes its
    first lines and never prunes. A full load prunes lines past the new file's
    end. ``cellosaurus_version`` is compared like any column, so a new release
    restamps every row: the table is one release, never a mix.
    """
    _stage(con, path, version)
    limit_sql = f" LIMIT {int(limit)}" if limit else ""
    target = target or f"{LAKE}.cellosaurus.line"
    try:
        return upsert(
            con, target,
            f"SELECT * EXCLUDE (malformed) FROM {_STAGE} ORDER BY line_number{limit_sql}",
            key="line_number", prune=None if limit else "true",
        )
    finally:
        con.execute(f"DROP TABLE IF EXISTS {_STAGE}")


def ingest(
    *,
    file: str | None = None,
    version: str | None = None,
    schema: str = "cellosaurus",
    limit: int | None = None,
    settings: Settings | None = None,
) -> dict:
    """End-to-end: download the current release (unless ``file``) -> MERGE-upsert -> summary.

    The file's header names the version; ``version``, if given, must match it.
    """
    s = settings or get_settings()
    path = Path(file) if file else download_txt(s)
    version = version or file_version(path)
    if not version:
        raise ValueError(f"cellosaurus: {path} header carries no ' Version:' line")
    target = f"{LAKE}.{schema}.line"
    con = lake_connect(s)
    try:
        with ops.run(con, source="cellosaurus", target=target, version=version) as r:
            r.rows = curate(con, path, version, target=target, limit=limit)
    finally:
        con.close()
    return r.summary()
