"""BEDbase snapshot -> ``lake.bedbase.{bed,bedset,bedset_membership}`` (EL only; bioc-on-ice#183).

BEDbase (bedbase.org) catalogs ~664k BED files across ~22k bedsets, mostly
GEO/ENCODE peak and region calls processed through its bedboss pipeline. Only
the catalog lands; the interval data is referenced, never ingested. Ported from
bioc-on-ice's ``src/bioconice/bedbase.py`` (its ``raw.bedbase__{bed,bedset,
bedset_membership}`` landing); bioc-on-ice derives ``resource.bedbase__{bedfile,
bedset}`` from these tables at release time (bioc-on-ice ADR-0015).

**Landing is BEDbase's own monthly Parquet snapshot, never an API crawl.** The
index ``/v1/exports`` lists each snapshot's three files with sha256 and record
count; the files sit on data2.bedbase.org, so pulling them costs the API and its
database nothing. Paging ``/v1/bed/list`` took the API down for ~95 minutes on
2026-09-18 (databio/bedhost#287). Do not bring the crawl back.

**Versioned by snapshot date**, ``bedbase_version``: the index's
``creation_date[:10]``, the label BEDbase publishes the files under. Dated files
are immutable and kept (monthly for 12 months, quarterly for good), so
``version`` fetches that snapshot. Raw has no version column (the date lived in
bioc-on-ice's provenance manifest); this one is added, last, and the release
SELECTs exclude it.

**The read is the lander's, verbatim**: the raw column names predate the
snapshot (the listing API's ``annotation_*`` prefix, ``bedset_source`` for
``source``), counts land INTEGER, the flags BOOLEAN, the two id lists
``|``-joined, the timestamps as the ISO 8601 text the API printed.
``annotation_description`` was listing-API only and lands NULL, as raw has it.
Columns are in raw's order, minus ``landed_in``.

**The shape is checked whole before anything lands**: each file's Parquet schema
must be exactly the declared one (a renamed, dropped, added or retyped column
fails), a file with no rows is refused, and a download must match the index's
sha256 and record count. Keys are raw's row identities: ``id`` (bed, bedset),
``(bedset_id, bedfile_id)`` (membership).
"""

from __future__ import annotations

import hashlib
import re
from pathlib import Path

import duckdb

from ... import ops
from ...config import Settings, get_settings
from ...connect import LAKE, csv_source, lake_connect, raw_dir, upsert
from ...download import download, get_json

_RAW = "bedbase"
# bedbase.org sits behind Cloudflare, which 403s some default user agents.
UA = {"User-Agent": "cdsci-lake (https://github.com/seandavi/cdsci-lake)"}

# Each snapshot file's Parquet schema (schema_version 1), as DESCRIBE prints it.
METADATA_SCHEMA = {
    "id": "VARCHAR", "name": "VARCHAR", "genome_alias": "VARCHAR", "genome_digest": "VARCHAR",
    "description": "VARCHAR", "bed_compliance": "VARCHAR", "data_format": "VARCHAR",
    "compliant_columns": "BIGINT", "non_compliant_columns": "BIGINT", "header": "VARCHAR",
    "indexed": "BOOLEAN", "file_indexed": "BOOLEAN",
    "submission_date": "TIMESTAMP WITH TIME ZONE", "last_update_date": "TIMESTAMP WITH TIME ZONE",
    "is_universe": "BOOLEAN", "license_id": "VARCHAR", "processed": "BOOLEAN",
    "species_name": "VARCHAR", "species_id": "VARCHAR", "genotype": "VARCHAR",
    "phenotype": "VARCHAR", "cell_type": "VARCHAR", "cell_line": "VARCHAR", "tissue": "VARCHAR",
    "library_source": "VARCHAR", "assay": "VARCHAR", "antibody": "VARCHAR", "target": "VARCHAR",
    "treatment": "VARCHAR", "original_file_name": "VARCHAR",
    "global_sample_id": "VARCHAR[]", "global_experiment_id": "VARCHAR[]",
}
BEDSETS_SCHEMA = {
    "id": "VARCHAR", "name": "VARCHAR", "description": "VARCHAR", "summary": "VARCHAR",
    "submission_date": "TIMESTAMP WITH TIME ZONE", "last_update_date": "TIMESTAMP WITH TIME ZONE",
    "md5sum": "VARCHAR", "bedset_means": "VARCHAR", "bedset_standard_deviation": "VARCHAR",
    "bedset_stats": "VARCHAR", "bedfile_count": "BIGINT", "author": "VARCHAR",
    "source": "VARCHAR", "processed": "BOOLEAN",
}
MEMBERSHIP_SCHEMA = {"bedset_id": "VARCHAR", "bedfile_id": "VARCHAR"}


def _iso(col: str) -> str:
    # The snapshot types these TIMESTAMPTZ; the raw columns are the text the API printed —
    # Python's isoformat(), which drops the fraction when the microseconds are zero.
    # AT TIME ZONE 'UTC' yields a plain TIMESTAMP, so the session TimeZone does not matter.
    return (f"replace(strftime({col} AT TIME ZONE 'UTC', '%Y-%m-%dT%H:%M:%S.%fZ'), "
            f"'.000000Z', 'Z') AS {col}")


def _read_bed(path: str) -> str:
    return f"""
        SELECT id, name, description, genome_alias, genome_digest, bed_compliance, data_format,
               compliant_columns::INTEGER AS compliant_columns,
               non_compliant_columns::INTEGER AS non_compliant_columns,
               is_universe, license_id, processed,
               {_iso('submission_date')}, {_iso('last_update_date')},
               species_name AS annotation_organism, species_id AS annotation_species_id,
               genotype AS annotation_genotype, phenotype AS annotation_phenotype,
               NULL::VARCHAR AS annotation_description,  -- listing-API only
               cell_type AS annotation_cell_type, cell_line AS annotation_cell_line,
               tissue AS annotation_tissue, library_source AS annotation_library_source,
               assay AS annotation_assay, antibody AS annotation_antibody,
               target AS annotation_target, treatment AS annotation_treatment,
               array_to_string(global_sample_id, '|') AS annotation_global_sample_id,
               array_to_string(global_experiment_id, '|') AS annotation_global_experiment_id,
               original_file_name AS annotation_original_file_name,
               header, indexed, file_indexed
        FROM read_parquet({path})"""


def _read_bedset(path: str) -> str:
    return f"""
        SELECT id, name, md5sum, {_iso('submission_date')}, {_iso('last_update_date')},
               description, bedfile_count::INTEGER AS bedfile_count, author,
               source AS bedset_source, summary, bedset_means, bedset_standard_deviation,
               bedset_stats, processed
        FROM read_parquet({path})"""


def _read_membership(path: str) -> str:
    return f"SELECT bedset_id, bedfile_id FROM read_parquet({path})"


# table -> (the index's file_type, its Parquet schema, reader, key)
TABLES = {
    "bed": ("metadata", METADATA_SCHEMA, _read_bed, "id"),
    "bedset": ("bedsets", BEDSETS_SCHEMA, _read_bedset, "id"),
    "bedset_membership": ("bedset_membership", MEMBERSHIP_SCHEMA, _read_membership,
                          ["bedset_id", "bedfile_id"]),
}


def file_name(file_type: str, version: str) -> str:
    """Upstream's name for one snapshot file, e.g. ``bedbase_metadata_2026_09_01.parquet``."""
    return f"bedbase_{file_type}_{version.replace('-', '_')}.parquet"


def snapshot(version: str | None = None, settings: Settings | None = None) -> dict[str, dict]:
    """One snapshot's index entries keyed by file_type: ``version``'s, or the newest."""
    s = settings or get_settings()
    file_types = {f for f, _, _, _ in TABLES.values()}
    entries = [e for e in get_json(s.bedbase_exports_url, headers=UA)["results"]
               if e["file_type"] in file_types]
    version = version or max(e["creation_date"] for e in entries)[:10]
    found = {e["file_type"]: e for e in entries if e["creation_date"][:10] == version}
    if set(found) != file_types:
        raise ValueError(f"bedbase: snapshot {version} lists {sorted(found)}, "
                         f"expected {sorted(file_types)}")
    return found


def download_snapshot(entries: dict[str, dict], version: str,
                      settings: Settings | None = None) -> dict[str, Path]:
    """Download one snapshot's files into the raw layer under ``version``, sha256-checked.

    A file whose digest is not the index's is removed, so a re-run fetches it again.
    """
    s = settings or get_settings()
    paths = {}
    for file_type, e in entries.items():
        path = download(e["file_path"], raw_dir(_RAW, s) / version / file_name(file_type, version),
                        headers=UA)
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        if digest != e["checksum"]:
            path.unlink()
            raise ValueError(f"bedbase: {e['file_path']} sha256 {digest} != index "
                             f"{e['checksum']}; removed, nothing landed")
        paths[file_type] = path
    return paths


def _check(path: Path, declared: dict[str, str], expected: int | None) -> None:
    """Fail unless ``path`` has exactly the declared schema and rows (``expected``, if given)."""
    read = f"read_parquet({csv_source([path])})"
    found = {r[0]: r[1] for r in duckdb.sql(f"DESCRIBE SELECT * FROM {read}").fetchall()}
    if found != declared:
        raise ValueError(f"bedbase: {path} schema is not the declared one; differs in "
                         f"{sorted(set(found.items()) ^ set(declared.items()))}")
    n = duckdb.sql(f"SELECT count(*) FROM {read}").fetchone()[0]
    if not n:
        raise ValueError(f"bedbase: {path} has zero rows")
    if expected is not None and n != expected:
        raise ValueError(f"bedbase: {path} has {n:,} rows, the index says {expected:,}")


def curate(
    con: duckdb.DuckDBPyConnection,
    paths: dict[str, Path],
    version: str,
    *,
    expected: dict[str, int] | None = None,
    schema: str = "bedbase",
    limit: int | None = None,
) -> dict[str, int]:
    """MERGE-upsert one snapshot's files (``paths`` by file_type); return each table's rows.

    All three files are checked whole before any lands; ``expected`` holds the
    index's record counts for a download. A full load prunes records gone from
    the snapshot; ``limit`` never prunes. ``bedbase_version`` is compared like any
    column, so a new snapshot restamps every row: each table is one snapshot.
    """
    # limit=0 is falsy: it would read every row and prune as a full load
    if limit is not None and limit < 1:
        raise ValueError(f"bedbase: limit must be positive, got {limit}")
    for file_type, declared, _, _ in TABLES.values():
        _check(paths[file_type], declared, (expected or {}).get(file_type))
    limit_sql = f" LIMIT {int(limit)}" if limit else ""
    counts = {}
    for table, (file_type, _, read, key) in TABLES.items():
        source = read(csv_source([paths[file_type]]))
        counts[table] = upsert(
            con, f"{LAKE}.{schema}.{table}",
            f"SELECT *, '{version}' AS bedbase_version FROM ({source}){limit_sql}",
            key=key, prune=None if limit else "true",
        )
    return counts


def ingest(
    *,
    directory: str | None = None,
    version: str | None = None,
    schema: str = "bedbase",
    limit: int | None = None,
    settings: Settings | None = None,
) -> dict:
    """End-to-end: download a snapshot (unless ``directory``) -> MERGE-upsert -> summary.

    ``directory`` holds local snapshot files under upstream's names; ``version``
    (default: the newest date among them) picks which, and nothing is checked
    against the index. Otherwise ``version`` (default: the newest in the index)
    is downloaded and verified.
    """
    s = settings or get_settings()
    entries: dict[str, dict] = {}
    if directory:
        d = Path(directory)
        dates = [m.group(1).replace("_", "-") for p in d.glob("bedbase_metadata_*.parquet")
                 if (m := re.fullmatch(r"bedbase_metadata_(\d{4}_\d{2}_\d{2})\.parquet", p.name))]
        version = version or (max(dates) if dates else None)
        if not version:
            raise ValueError(f"bedbase: no bedbase_metadata_<YYYY_MM_DD>.parquet in {d}")
        paths = {f: d / file_name(f, version) for f, _, _, _ in TABLES.values()}
        if missing := [p.name for p in paths.values() if not p.exists()]:
            raise FileNotFoundError(f"bedbase: {d} lacks {missing}")
    else:
        entries = snapshot(version, s)
        version = next(iter(entries.values()))["creation_date"][:10]
        paths = download_snapshot(entries, version, s)
    expected = {f: e["record_count"] for f, e in entries.items()}
    con = lake_connect(s)
    try:
        with ops.run(con, source="bedbase", target=f"{LAKE}.{schema}", version=version) as r:
            counts = curate(con, paths, version, expected=expected, schema=schema, limit=limit)
            r.rows = sum(counts.values())
    finally:
        con.close()
    return {**r.summary(), "schema": f"{LAKE}.{schema}",
            "snapshot_files": {f: p.name for f, p in paths.items()}, **counts}
