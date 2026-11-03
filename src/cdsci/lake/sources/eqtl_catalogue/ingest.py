"""eQTL Catalogue -> ``lake.eqtl_catalogue.{dataset,tabix_ftp_path}`` (EL only; bioc-on-ice#183).

The eQTL Catalogue (EMBL-EBI / University of Tartu, CC BY 4.0) reprocesses
public QTL studies uniformly and publishes, per dataset (one study x sample
group x quantification method), tabix-indexed summary statistics and SuSiE
fine-mapping files on the EBI FTP. Only the metadata lands here, two files of
github.com/eQTL-Catalogue/eQTL-Catalogue-resources, whole:

  data_tables/dataset_metadata_r<N>.tsv   the datasets of release N -> ``dataset``
  tabix/tabix_ftp_paths.tsv               the same datasets with their three FTP
                                          paths -> ``tabix_ftp_path``

Ported from bioc-on-ice's ``src/bioconice/eqtlcatalogue.py`` (its
``raw.eqtlcatalogue__{dataset,tabix_ftp_paths}`` landing); bioc-on-ice derives
``resource.eqtlcatalogue__dataset`` from these tables at release time
(bioc-on-ice ADR-0015).

**Versioned by release number.** The version is the N in the metadata file's
name, ``eqtlcatalogue_release``. The path table names no release, so it is
labelled with the metadata file's, which is only true if the two name exactly
the same datasets: that is checked over both whole files before either lands.
The files are fetched at a git tag (``Settings.eqtl_catalogue_url``) so the
bytes are immutable.

Both files are plain TSV: a header line, tab-delimited, no quoting (no quote
character occurs), LF, r7's metadata file with no final newline. 'NA' is R's
missing marker and reads as NULL, as the empty cell does. Every column lands
VARCHAR, as raw has it. Each file's header is checked whole and every line's
field count with it, before anything lands. Both tables are keyed
``dataset_id``, the natural key raw's docs name.

ponytail: ``RELEASE`` is bumped by hand when release 8 ships its paths (checked
2026-09-18: r8's 481 new datasets have no path rows yet). No "latest"
discovery: nothing upstream names the current release in a machine-readable place.
"""

from __future__ import annotations

from pathlib import Path

import duckdb

from ... import ops
from ...config import Settings, get_settings
from ...connect import LAKE, csv_source, lake_connect, raw_dir, upsert
from ...download import download

_RAW = "eqtl_catalogue"
RELEASE = "7"

# Each file's header, in file order.
DATASET_COLUMNS = (
    "study_id", "dataset_id", "study_label", "sample_group", "tissue_id", "tissue_label",
    "condition_label", "sample_size", "quant_method", "pmid", "study_type",
)
PATH_COLUMNS = (
    "study_id", "dataset_id", "study_label", "sample_group", "tissue_id", "tissue_label",
    "condition_label", "sample_size", "quant_method", "ftp_path", "ftp_cs_path", "ftp_lbf_path",
)


def _files(version: str) -> dict[str, str]:
    """Each table's file, relative to the resources repo root."""
    return {"dataset": f"data_tables/dataset_metadata_r{version}.tsv",
            "tabix_ftp_path": "tabix/tabix_ftp_paths.tsv"}


def download_release(version: str, settings: Settings | None = None) -> tuple[Path, Path]:
    """Download one release's two files into the raw layer: (dataset metadata, FTP paths)."""
    s = settings or get_settings()
    dest = raw_dir(_RAW, s) / version
    dataset, paths = (download(f"{s.eqtl_catalogue_url.rstrip('/')}/{f}", dest / Path(f).name)
                      for f in _files(version).values())
    return dataset, paths


def _check(path: Path, columns: tuple[str, ...]) -> set[str]:
    """The header whole, then every line's field count; return the file's dataset ids.

    No cell holds a tab or a line break, so a line is a record and a field count
    other than the header's is a malformed file, wherever it is (not just within
    a limited load's rows). A file with no rows is refused.
    """
    at = columns.index("dataset_id")
    ids = set()
    with open(path, "rb") as f:
        header = tuple(f.readline().decode("utf-8", "replace").rstrip("\r\n").split("\t"))
        if header != columns:
            raise ValueError(f"eqtl_catalogue: {path} header is not the declared one; "
                             f"differs in {sorted(set(header) ^ set(columns)) or 'column order'}")
        for n, line in enumerate(f, 1):
            fields = line.rstrip(b"\r\n").split(b"\t")
            if len(fields) != len(columns):
                raise ValueError(f"eqtl_catalogue: {path} data line {n} has {len(fields)} "
                                 f"fields, not {len(columns)}")
            ids.add(fields[at].decode("utf-8", "replace"))
    if not ids:
        raise ValueError(f"eqtl_catalogue: {path} has zero rows")
    return ids


def _select_sql(path: Path, columns: tuple[str, ...], version: str, limit: int | None) -> str:
    select = ", ".join(f'"{c}"' for c in columns)
    limit_sql = f" LIMIT {int(limit)}" if limit else ""
    # The lander's dialect, verbatim: stated rather than sniffed, no quoting,
    # 'NA' and the empty cell are missing.
    return f"""
        SELECT {select}, '{version}' AS eqtlcatalogue_release
        FROM read_csv({csv_source([path])}, header=true, all_varchar=true, delim='\\t',
                      quote='', escape='', nullstr=['', 'NA'])
        {limit_sql}
    """


def curate(
    con: duckdb.DuckDBPyConnection,
    dataset: Path,
    paths: Path,
    version: str,
    *,
    schema: str = "eqtl_catalogue",
    limit: int | None = None,
) -> dict[str, int]:
    """MERGE-upsert one release into ``dataset`` and ``tabix_ftp_path``; return each table's rows.

    Both files are checked whole, and checked to name the same datasets, before
    either lands. A full load prunes datasets the release no longer has;
    ``limit`` never prunes. ``eqtlcatalogue_release`` is compared like any
    column, so a new release restamps every row: each table is one release.
    """
    unpaired = _check(dataset, DATASET_COLUMNS) ^ _check(paths, PATH_COLUMNS)
    if unpaired:
        raise ValueError(f"eqtl_catalogue: {len(unpaired):,} datasets are in only one of the "
                         f"release {version} metadata and tabix_ftp_paths.tsv (e.g. "
                         f"{sorted(unpaired)[0]}); the path table is for another release")
    prune = None if limit else "true"
    return {
        table: upsert(con, f"{LAKE}.{schema}.{table}",
                      _select_sql(path, columns, version, limit),
                      key="dataset_id", prune=prune)
        for table, path, columns in (("dataset", dataset, DATASET_COLUMNS),
                                     ("tabix_ftp_path", paths, PATH_COLUMNS))
    }


def ingest(
    *,
    directory: str | None = None,
    version: str | None = None,
    schema: str = "eqtl_catalogue",
    limit: int | None = None,
    settings: Settings | None = None,
) -> dict:
    """End-to-end: download a release (unless ``directory``) -> MERGE-upsert -> summary.

    ``directory`` is the root of a local copy of the resources repo (holding
    ``data_tables/`` and ``tabix/``). ``version`` is the release number,
    default :data:`RELEASE`.
    """
    s = settings or get_settings()
    version = version or RELEASE
    if directory:
        dataset, paths = (Path(directory) / f for f in _files(version).values())
    else:
        dataset, paths = download_release(version, s)
    con = lake_connect(s)
    try:
        with ops.run(con, source="eqtl_catalogue", target=f"{LAKE}.{schema}",
                     version=version) as r:
            counts = curate(con, dataset, paths, version, schema=schema, limit=limit)
            r.rows = sum(counts.values())
    finally:
        con.close()
    return {**r.summary(), "schema": f"{LAKE}.{schema}", **counts}
