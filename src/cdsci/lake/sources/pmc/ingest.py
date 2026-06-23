"""Ingest BioC-PMC full text into ``pmc.documents`` + ``pmc.passages``.

Bulk = per-PMCID-range tarballs at the BioC-PMC FTP (``PMC{range}XXXXX_json_unicode
.tar.gz``); each tarball holds one BioC *collection* JSON per article (the inner
files are named ``.xml`` but the content is JSON). The whole json-unicode set is
~130 GB / ~6M articles, so we process **one range at a time** to bound local disk.

Per range (medallion, ADR-0012):
1. **download** the tarball.
2. **stream → NDJSON** (Python ``tarfile``; one compact collection per line).
3. **materialize** a bronze Parquet ``(pmcid, record)`` — the faithful capture.
4. **curate** two normalized silver tables from it, both keyed off ``pmcid``:
   * ``pmc.documents`` — one row per article (pmid/doi/license/title/n_passages).
   * ``pmc.passages`` — one row per BioC passage (the full text, exploded out of
     the nested ``record`` into ``(pmcid, passage_index)`` rows). The one-to-many
     ``documents`` → ``passages`` relationship replaces the old flat ``fulltext``
     table that carried the whole BioC ``record`` as a nested blob.
Then delete the local tarball + NDJSON and move to the next range.

Incrementals use the per-article REST API (``biocpmc_api``) for PMCIDs newer than
the last bulk; the same MERGE-on-pmcid makes bulk and API top-ups idempotent.
"""

from __future__ import annotations

import gzip
import json
import re
import tarfile
from pathlib import Path

import duckdb
import httpx

from ...config import Settings, get_settings
from ...connect import LAKE, csv_source, lake_connect, raw_dir, upsert
from ...download import download

_DOCUMENTS = "documents"
_PASSAGES = "passages"
_DOC = "$.documents[0]"
_P0 = f"{_DOC}.passages[0]"


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
    """Stream a BioC tarball → gzipped NDJSON (one compact BioC collection per line).

    Gzip keeps the transient extract ~4 GB/range instead of ~20 GB; DuckDB reads
    ``.ndjson.gz`` directly.
    """
    n = 0
    opener = gzip.open if str(ndjson_path).endswith(".gz") else open
    with tarfile.open(tar_path, "r:gz") as tf, opener(ndjson_path, "wt") as out:
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
    base = ndjson.name
    for suffix in (".ndjson.gz", ".ndjson", ".gz"):
        if base.endswith(suffix):
            base = base[: -len(suffix)]
            break
    raw = ndjson.with_name(base + ".parquet")
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


def _documents_sql(src: str, snapshot: str, limit: int | None) -> str:
    """One row per article: crosswalk ids + document-level metadata (no passages)."""
    limit_sql = f" LIMIT {int(limit)}" if limit else ""
    return f"""
        SELECT
            pmcid,
            TRY_CAST(json_extract_string(record, '{_P0}.infons."article-id_pmid"') AS BIGINT) AS pmid,
            json_extract_string(record, '{_P0}.infons."article-id_doi"')  AS doi,
            json_extract_string(record, '{_DOC}.infons.license')          AS license,
            json_extract_string(record, '{_P0}.text')                     AS title,
            json_array_length(json_extract(record, '{_DOC}.passages'))    AS n_passages,
            CAST({_sql_str(snapshot)} AS VARCHAR)                         AS snapshot_version
        FROM read_parquet({src}){limit_sql}
    """


def _passages_sql(src: str, snapshot: str, limit: int | None) -> str:
    """One row per BioC passage, exploded from the nested ``record`` array.

    ``passages`` is cast to a list of JSON values, then zipped against its index
    range so each passage becomes ``(pmcid, passage_index)`` — the composite key.
    The full passage ``infons`` is kept as JSON so nothing passage-level is lost.
    """
    limit_sql = f" LIMIT {int(limit)}" if limit else ""
    return f"""
        WITH docs AS (
            SELECT
                pmcid,
                CAST(json_extract(record, '{_DOC}.passages') AS JSON[]) AS passages
            FROM read_parquet({src}){limit_sql}
        ),
        exploded AS (
            SELECT
                pmcid,
                unnest(range(0, len(passages))) AS passage_index,
                unnest(passages)                AS passage
            FROM docs
        )
        SELECT
            pmcid,
            passage_index,
            TRY_CAST(json_extract_string(passage, '$.offset') AS BIGINT) AS passage_offset,
            json_extract_string(passage, '$.infons.section_type')        AS section_type,
            json_extract_string(passage, '$.infons.type')                AS passage_type,
            json_extract_string(passage, '$.text')                       AS text,
            json_extract(passage, '$.infons')                            AS infons,
            CAST({_sql_str(snapshot)} AS VARCHAR)                        AS snapshot_version
        FROM exploded
    """


def _load(
    con: duckdb.DuckDBPyConnection,
    target: str,
    source_sql: str,
    key: list[str],
    *,
    mode: str,
) -> int:
    """Dispatch one table load to MERGE-upsert (idempotent) or append-INSERT (bulk).

    ``mode="merge"`` (default) MERGE-upserts on ``key`` — for incrementals/re-runs.
    ``mode="append"`` INSERTs without reading the target — for the initial bulk,
    whose PMCID ranges are disjoint, so a MERGE's whole-table read (growing to
    hundreds of GB by the last range) is pure waste. Append returns rows written
    this call (no full-table count, which would re-read the growing table from R2).
    """
    if mode == "merge":
        return upsert(con, target, source_sql, key, exclude_change_cols=["snapshot_version"])
    if mode != "append":
        raise ValueError(f"mode must be 'merge' or 'append', got {mode!r}")
    catalog, schema, _ = target.split(".")
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema};")
    con.execute(f"CREATE TABLE IF NOT EXISTS {target} AS SELECT * FROM ({source_sql}) WHERE false;")
    return con.execute(f"INSERT INTO {target} SELECT * FROM ({source_sql});").fetchone()[0]


def curate(
    con: duckdb.DuckDBPyConnection,
    raw_parquet: Path | str,
    *,
    schema: str = "pmc",
    snapshot: str = "bulk",
    limit: int | None = None,
    mode: str = "merge",
) -> dict:
    """Load ``pmc.documents`` + ``pmc.passages`` from one bronze Parquet.

    Both tables are derived from the same bronze ``(pmcid, record)``: ``documents``
    is one row per article (key ``pmcid``); ``passages`` explodes the nested BioC
    passages into one row each (key ``(pmcid, passage_index)``). Returns the row
    counts keyed by table. See :func:`_load` for ``mode`` semantics.
    """
    src = csv_source(str(raw_parquet))
    documents = _load(
        con,
        f"{LAKE}.{schema}.{_DOCUMENTS}",
        _documents_sql(src, snapshot, limit),
        ["pmcid"],
        mode=mode,
    )
    passages = _load(
        con,
        f"{LAKE}.{schema}.{_PASSAGES}",
        _passages_sql(src, snapshot, limit),
        ["pmcid", "passage_index"],
        mode=mode,
    )
    return {"documents": documents, "passages": passages}


def ingest(
    *,
    ranges: list[str] | None = None,
    file: str | None = None,
    schema: str = "pmc",
    snapshot: str = "bulk",
    limit: int | None = None,
    mode: str = "merge",
    keep_raw: bool = False,
    settings: Settings | None = None,
) -> dict:
    """Load BioC-PMC into ``pmc.documents`` + ``pmc.passages``, one range at a time.

    ``ranges`` selects tarball filenames (default: all json-unicode ranges).
    ``mode`` is ``"append"`` for the initial disjoint-range bulk (no target reads)
    or ``"merge"`` for incrementals. ``file`` loads a single local NDJSON/Parquet.
    Local transients are deleted after each range unless ``keep_raw``. Rows are
    summed per range and per table — no final whole-table ``count(*)`` (which would
    re-read the growing tables from R2).
    """
    s = settings or get_settings()
    con = lake_connect(s)
    summary: dict = {
        "schema": f"{LAKE}.{schema}",
        "ranges": [],
        "documents": 0,
        "passages": 0,
    }
    try:
        if file:
            raw = Path(file) if str(file).endswith(".parquet") else materialize_raw(con, file)
            counts = curate(con, raw, schema=schema, snapshot=snapshot, limit=limit, mode=mode)
            summary["documents"] += counts["documents"]
            summary["passages"] += counts["passages"]
            summary["ranges"].append(Path(file).name)
            return summary

        for filename in ranges if ranges is not None else list_ranges(s):
            tar = download_range(filename, s)
            ndjson = raw_dir("pmc", s) / (filename.replace(".tar.gz", ".ndjson.gz"))
            n_files = tar_to_ndjson(tar, ndjson)
            raw = materialize_raw(con, ndjson)
            counts = curate(con, raw, schema=schema, snapshot=snapshot, limit=limit, mode=mode)
            summary["documents"] += counts["documents"]
            summary["passages"] += counts["passages"]
            summary["ranges"].append(
                {"file": filename, "articles": n_files, **counts}
            )
            if not keep_raw:
                # The silver tables on R2 are the faithful copy, so the local
                # tarball / NDJSON / bronze Parquet are all transient — delete
                # them per range so disk stays ~one range, not 22 × 7 GB.
                tar.unlink(missing_ok=True)
                ndjson.unlink(missing_ok=True)
                raw.unlink(missing_ok=True)
        return summary
    finally:
        con.close()
