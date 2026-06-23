"""Ingest BioC-PMC full text into ``pmc.fulltext``.

Bulk = per-PMCID-range tarballs at the BioC-PMC FTP (``PMC{range}XXXXX_json_unicode
.tar.gz``); each tarball holds one BioC *collection* JSON per article (the inner
files are named ``.xml`` but the content is JSON). The whole json-unicode set is
~130 GB / ~6M articles, so we process **one range at a time** to bound local disk.

Per range (medallion, ADR-0012):
1. **download** the tarball.
2. **stream → NDJSON** (Python ``tarfile``; one compact collection per line).
3. **materialize** a bronze Parquet ``(pmcid, record)`` — the faithful capture.
4. **curate** ``pmc.fulltext`` from it (extract pmid/doi/license/title + keep the
   full record), MERGE on ``pmcid``.
Then delete the local tarball + NDJSON and move to the next range.

Incrementals use the per-article REST API (``biocpmc_api``) for PMCIDs newer than
the last bulk; the same MERGE-on-pmcid makes bulk and API top-ups idempotent.
"""

from __future__ import annotations

import json
import re
import tarfile
from pathlib import Path

import duckdb
import httpx

from ...config import Settings, get_settings
from ...connect import LAKE, csv_source, lake_connect, raw_dir, upsert
from ...download import download

_TABLE = "fulltext"
_DOC = "$.documents[0]"


def list_ranges(settings: Settings | None = None) -> list[str]:
    """Return the json-unicode tarball filenames available on the BioC-PMC FTP."""
    s = settings or get_settings()
    html = httpx.get(s.biocpmc_ftp, timeout=60, follow_redirects=True).text
    pattern = rf"PMC\d+XXXXX_{re.escape(s.biocpmc_variant)}\.tar\.gz"
    return sorted(set(re.findall(pattern, html)))


def download_range(filename: str, settings: Settings | None = None) -> Path:
    """Download one range tarball to the raw layer (resumable)."""
    s = settings or get_settings()
    return download(s.biocpmc_ftp + filename, raw_dir("pmc", s) / filename)


def tar_to_ndjson(tar_path: Path, ndjson_path: Path) -> int:
    """Stream a BioC tarball → NDJSON (one compact BioC collection per line)."""
    n = 0
    with tarfile.open(tar_path, "r:gz") as tf, open(ndjson_path, "w") as out:
        for member in tf:
            if not member.isfile():
                continue
            fh = tf.extractfile(member)
            if fh is None:
                continue
            try:
                obj = json.loads(fh.read())
            except (ValueError, UnicodeDecodeError):
                continue  # skip a malformed file rather than abort the range
            out.write(json.dumps(obj, separators=(",", ":")) + "\n")
            n += 1
    return n


def materialize_raw(con: duckdb.DuckDBPyConnection, ndjson: Path | str) -> Path:
    """Bronze: compact Parquet ``(pmcid, record)`` from the NDJSON extract."""
    ndjson = Path(ndjson)
    raw = ndjson.with_name(ndjson.stem + ".parquet")
    con.execute(
        f"""
        COPY (
            SELECT json_extract_string(json, '{_DOC}.id') AS pmcid,
                   CAST(json AS VARCHAR) AS record
            FROM read_json_objects('{ndjson}')
            WHERE json_extract_string(json, '{_DOC}.id') IS NOT NULL
        ) TO '{raw}' (FORMAT parquet);
        """
    )
    return raw


def _sql_str(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _select_sql(src: str, snapshot: str, limit: int | None) -> str:
    limit_sql = f" LIMIT {int(limit)}" if limit else ""
    p0 = f"{_DOC}.passages[0]"
    return f"""
        SELECT
            pmcid,
            TRY_CAST(json_extract_string(record, '{p0}.infons."article-id_pmid"') AS BIGINT) AS pmid,
            json_extract_string(record, '{p0}.infons."article-id_doi"')  AS doi,
            json_extract_string(record, '{_DOC}.infons.license')         AS license,
            json_extract_string(record, '{p0}.text')                     AS title,
            json_array_length(json_extract(record, '{_DOC}.passages'))   AS n_passages,
            CAST({_sql_str(snapshot)} AS VARCHAR)                        AS snapshot_version,
            record
        FROM read_parquet({src}){limit_sql}
    """


def curate(
    con: duckdb.DuckDBPyConnection,
    raw_parquet: Path | str,
    *,
    schema: str = "pmc",
    snapshot: str = "bulk",
    limit: int | None = None,
) -> int:
    """Upsert ``pmc.fulltext`` from a bronze Parquet; MERGE on ``pmcid``."""
    src = csv_source(str(raw_parquet))
    return upsert(con, f"{LAKE}.{schema}.{_TABLE}", _select_sql(src, snapshot, limit), key="pmcid")


def ingest(
    *,
    ranges: list[str] | None = None,
    file: str | None = None,
    schema: str = "pmc",
    snapshot: str = "bulk",
    limit: int | None = None,
    keep_raw: bool = False,
    settings: Settings | None = None,
) -> dict:
    """Load BioC-PMC into ``pmc.fulltext``, one range at a time.

    ``ranges`` selects tarball filenames (default: all json-unicode ranges).
    ``file`` loads a single local NDJSON/Parquet instead (for tests/pilots).
    Local tarball + NDJSON are deleted after each range unless ``keep_raw``.
    """
    s = settings or get_settings()
    con = lake_connect(s)
    summary: dict = {"table": f"{LAKE}.{schema}.{_TABLE}", "ranges": [], "rows": 0}
    try:
        if file:
            raw = Path(file) if str(file).endswith(".parquet") else materialize_raw(con, file)
            summary["rows"] = curate(con, raw, schema=schema, snapshot=snapshot, limit=limit)
            summary["ranges"].append(Path(file).name)
            return summary

        for filename in ranges if ranges is not None else list_ranges(s):
            tar = download_range(filename, s)
            ndjson = raw_dir("pmc", s) / (filename.replace(".tar.gz", ".ndjson"))
            n_files = tar_to_ndjson(tar, ndjson)
            raw = materialize_raw(con, ndjson)
            curate(con, raw, schema=schema, snapshot=snapshot, limit=limit)
            summary["ranges"].append({"file": filename, "articles": n_files})
            if not keep_raw:
                tar.unlink(missing_ok=True)
                ndjson.unlink(missing_ok=True)
        summary["rows"] = con.execute(
            f"SELECT count(*) FROM {LAKE}.{schema}.{_TABLE}"
        ).fetchone()[0]
        return summary
    finally:
        con.close()
