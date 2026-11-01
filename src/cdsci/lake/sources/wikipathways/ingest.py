"""WikiPathways GMT -> ``lake.wikipathways.gene_set`` (EL only; bioc-on-ice#183 step 9).

WikiPathways (CC0) publishes its pathways as gene sets once a month: one GMT file
per species, the release date in every file name
(``wikipathways-20260910-gmt-Homo_sapiens.gmt``). 18 species files and 1,957
pathways on 2026-09-10. Ported from bioc-on-ice's ``src/bioconice/wikipathways.py``
(its ``raw.wikipathways__gmt`` landing); bioc-on-ice derives
``annotation.wikipathways__{pathway,gene_pathway}`` from this table at release
time (bioc-on-ice ADR-0015).

The date is the version, in WikiPathways' own form (``20260910``). The newest
release is under ``current/gmt/``; each release is also kept as
``<YYYYMMDD>/gmt/`` for twelve months, which is where a version is fetched from.

**GMT is ragged, so it is not read as CSV.** A line is a set name, a
description, then as many Entrez GeneIDs as the set has, tab-separated, with no
header and no quoting. Each file is read as text and split on LF. One row per
line: the gene ids stay the verbatim tab-joined rest of the line, repeats and
order as the file has them, and the packed set name
(``name%WikiPathways_<date>%<WP id>%<species>``) lands unparsed. Splitting
either is interpretation, which is bioc-on-ice's.

Every species file of the release lands. A file set that is not exactly one
release (a stray file name, two dates) fails before anything is read.
"""

from __future__ import annotations

import re
from pathlib import Path

import duckdb
import httpx

from ... import ops
from ...config import Settings, get_settings
from ...connect import LAKE, csv_source, lake_connect, raw_dir, upsert
from ...download import download

_RAW = "wikipathways"
FILE = re.compile(r"wikipathways-(\d{8})-gmt-([A-Za-z_]+)\.gmt")


def _names(url: str) -> list[str]:
    """The GMT file names an http directory listing links to."""
    resp = httpx.get(url, timeout=60, follow_redirects=True)
    resp.raise_for_status()
    return sorted({m.group(0) for m in FILE.finditer(resp.text)})


def release_of(names: list[str]) -> str:
    """The one date every file name shares; anything else is not one release."""
    dates = {m.group(1) for n in names if (m := FILE.fullmatch(n))}
    if len(dates) != 1 or not all(FILE.fullmatch(n) for n in names):
        raise ValueError(f"wikipathways: expected the GMT files of exactly one release; "
                         f"got {len(names)} files, dates {sorted(dates)}")
    return dates.pop()


def latest(settings: Settings | None = None) -> str:
    """The newest release, from the file names under ``current/gmt/``."""
    s = settings or get_settings()
    return release_of(_names(f"{s.wikipathways_base_url}current/gmt/"))


def download_release(version: str, settings: Settings | None = None) -> list[Path]:
    """Download every species GMT of one release into the raw layer."""
    s = settings or get_settings()
    url = f"{s.wikipathways_base_url}{version}/gmt/"
    names = _names(url)
    if not names or release_of(names) != version:
        raise RuntimeError(f"wikipathways: no GMT files of release {version} at {url}")
    dest = raw_dir(_RAW, s) / version
    return [download(url + n, dest / n) for n in names]


def _select_sql(paths: list[Path], version: str, limit: int | None) -> str:
    limit_sql = f" LIMIT {int(limit)}" if limit else ""
    # No CSV reader: a line is a record and tab the only separator. The final LF
    # leaves one empty string after the split; that and nothing else is dropped.
    # A set with no genes keeps genes NULL.
    return f"""
        WITH f AS (SELECT parse_filename(filename) AS file_name,
                          str_split(content, '\n') AS lines
                   FROM read_text({csv_source(paths)})),
             l AS (SELECT file_name, unnest(lines) AS line,
                          generate_subscripts(lines, 1) AS line_number FROM f)
        SELECT file_name, line_number::INTEGER AS line_number,
               split_part(line, '\t', 1) AS name,
               split_part(line, '\t', 2) AS description,
               nullif(array_to_string(str_split(line, '\t')[3:], '\t'), '') AS genes,
               regexp_extract(file_name, '-gmt-(.*)\\.gmt$', 1) AS species,
               '{version}' AS wikipathways_version
        FROM l WHERE line <> ''
        {limit_sql}
    """


def curate(
    con: duckdb.DuckDBPyConnection,
    paths: list[Path],
    version: str,
    *,
    target: str | None = None,
    limit: int | None = None,
) -> int:
    """MERGE-upsert one release's GMT files into ``gene_set`` on (file_name, line_number).

    A full load prunes what the release no longer has; ``limit`` never prunes. The
    file names carry the date, so a new release replaces every row: the table is
    one release, never a mix.
    """
    if release_of([p.name for p in paths]) != version:
        raise ValueError(f"wikipathways: the files are not release {version}")
    target = target or f"{LAKE}.wikipathways.gene_set"
    return upsert(
        con, target, _select_sql(sorted(paths), version, limit),
        key=["file_name", "line_number"], prune=None if limit else "true",
    )


def ingest(
    *,
    directory: str | None = None,
    version: str | None = None,
    schema: str = "wikipathways",
    limit: int | None = None,
    settings: Settings | None = None,
) -> dict:
    """End-to-end: download a release (unless ``directory``) -> MERGE-upsert -> summary.

    ``directory`` holds one release's ``*.gmt`` files; their names give the version.
    Otherwise ``version`` (default: the newest release, :func:`latest`) is downloaded.
    """
    s = settings or get_settings()
    if directory:
        paths = sorted(Path(directory).glob("*.gmt"))
        version = version or release_of([p.name for p in paths])
    else:
        version = version or latest(s)
        paths = download_release(version, s)
    target = f"{LAKE}.{schema}.gene_set"
    con = lake_connect(s)
    try:
        with ops.run(con, source="wikipathways", target=target, version=version) as r:
            r.rows = curate(con, paths, version, target=target, limit=limit)
    finally:
        con.close()
    return r.summary()
