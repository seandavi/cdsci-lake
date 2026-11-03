"""ENCODE portal metadata -> ``lake.encode.{experiment,file}`` (EL only; bioc-on-ice#183).

The ENCODE portal (encodeproject.org) catalogs 28,642 experiments and 1,659,000
files visible anonymously (2026-09-18; released, archived and revoked alike).
Only the metadata lands; the files stay in ENCODE's public bucket. Ported from
bioc-on-ice's ``src/bioconice/encode.py`` (its ``raw.encode__{experiment,file}``
landing); bioc-on-ice derives ``resource.encode__{experiment,file}`` from these
tables at release time (bioc-on-ice ADR-0015).

**One ``report.tsv`` per type, with an explicit ``field=`` list.** The report
streams the whole inventory from a server-side scan (it rewrites any ``limit``
to ``limit=all``) and takes dotted paths into embedded objects
(``biosample_ontology.term_id``). Without ``field=`` it carries only the display
columns, so the columns are a declared list (:data:`EXPERIMENT_FIELDS`,
:data:`FILE_FIELDS`): whole in rows, a declared subset of each object's
properties. Column names are the fields with ``.`` -> ``_``, every one VARCHAR,
as raw has them.

**The read is positional, so the request is checked whole first.** The first
line is ``<UTC timestamp>\\t<request URL>`` and the second the display titles,
which the portal can reword. The URL's ``field=`` list must echo ours, in
order, and DuckDB counts the whole file with a strict column count, so a row
with the wrong number of cells fails anywhere in the file, not just within a
limited load's rows. A report with no rows is refused, and a download with
fewer rows than the search API's ``total`` is a short download: it is removed
and nothing lands.

The dialect is the lander's, verbatim: tab-delimited, CRLF, no quoting at all
(the portal collapses whitespace runs inside a value to one space, and a quote
character is data), the empty cell is NULL. List-valued fields stay
``,``-joined as the portal writes them.

**Versioned by retrieval date.** The portal has no release number and no dated
copies; the inventory mutates in place. Each table's ``retrieval_date`` is its
own report's timestamp date, true even for a report downloaded earlier, and the
two may differ when a download straddles UTC midnight. Keyed ``accession``
(experiment) and ``title`` (file): raw's identity columns, unique and never
NULL (the 2026-09-18 crawl: 28,642 and 1,659,000 distinct). A file is keyed on
``title`` because 1,283 files have only an ``external_accession``.
"""

from __future__ import annotations

import urllib.parse
from datetime import UTC, datetime
from pathlib import Path

import duckdb

from ... import ops
from ...config import Settings, get_settings
from ...connect import LAKE, csv_source, lake_connect, raw_dir, upsert
from ...download import download, get_json

_RAW = "encode"
UA = {"User-Agent": "cdsci-lake (https://github.com/seandavi/cdsci-lake)"}

# report.tsv `field=` paths, in request (= column) order.
EXPERIMENT_FIELDS = (
    "accession", "uuid", "status", "date_created", "date_submitted", "date_released",
    "description", "assay_term_id", "assay_term_name", "assay_title", "assay_slims",
    "biosample_ontology.term_id", "biosample_ontology.term_name",
    "biosample_ontology.classification", "biosample_ontology.organ_slims",
    "biosample_ontology.cell_slims", "biosample_summary",
    "replicates.library.biosample.organism.scientific_name",
    "replicates.library.biosample.organism.taxon_id",
    "target.name", "target.label", "target.investigated_as", "target.genes.geneid",
    "target.genes.symbol", "control_type", "perturbed", "lab.title", "award.name",
    "award.project", "award.rfa", "assembly", "replication_type", "bio_replicate_count",
    "tech_replicate_count", "dbxrefs", "doi", "internal_tags", "alternate_accessions",
    "supersedes", "superseded_by", "possible_controls",
)
FILE_FIELDS = (
    "accession", "external_accession", "title", "uuid", "status", "dataset", "file_format",
    "file_format_type", "file_type", "output_type", "output_category", "assembly",
    "genome_annotation", "file_size", "md5sum",
    "content_md5sum", "href", "cloud_metadata.url", "s3_uri", "no_file_available", "restricted",
    "derived_from", "biological_replicates", "technical_replicates", "preferred_default",
    "processed", "run_type", "read_length", "paired_end", "paired_with", "date_created",
    "lab.title", "award.project", "award.rfa", "alternate_accessions", "superseded_by",
)
# table -> (portal type, fields, key)
TABLES = {
    "experiment": ("Experiment", EXPERIMENT_FIELDS, "accession"),
    "file": ("File", FILE_FIELDS, "title"),
}


def report_url(type_: str, fields: tuple[str, ...], settings: Settings | None = None) -> str:
    s = settings or get_settings()
    return f"{s.encode_base_url}/report.tsv?type={type_}&" + "&".join(f"field={f}" for f in fields)


def total(type_: str, settings: Settings | None = None) -> int:
    """The portal's own count of ``type_`` objects, from one zero-row search."""
    s = settings or get_settings()
    return get_json(f"{s.encode_base_url}/search/?type={type_}&limit=0&format=json",
                    headers=UA)["total"]


def download_report(table: str, version: str, settings: Settings | None = None) -> Path:
    """Download one table's report into the raw layer, keyed by ``version`` (the UTC date)."""
    s = settings or get_settings()
    type_, fields, _ = TABLES[table]
    return download(report_url(type_, fields, s), raw_dir(_RAW, s) / f"{version}-{table}.tsv",
                    headers=UA)


def _read(path: Path, fields: tuple[str, ...]) -> str:
    """The lander's read: positional, every column VARCHAR, no quoting.

    skip=2 drops the comment line and the display-title header. A row with the
    wrong number of cells fails in DuckDB's reader rather than shifting columns.
    """
    columns = "{" + ", ".join(f"'{f.replace('.', '_')}': 'VARCHAR'" for f in fields) + "}"
    return (f"read_csv({csv_source([path])}, delim='\\t', quote='', escape='', header=false, "
            f"skip=2, auto_detect=false, nullstr='', columns={columns})")


def _comment(path: Path) -> tuple[str, str]:
    """The report's first line: (its UTC date, the request URL). Only that line is read."""
    with open(path, encoding="utf-8") as f:
        stamp, _, url = f.readline().rstrip("\r\n").partition("\t")
    return datetime.strptime(stamp[:10], "%Y-%m-%d").date().isoformat(), url


def _verify(path: Path, fields: tuple[str, ...], expected: int | None = None) -> str:
    """The report's own date, after failing if it is not the report asked for, or is short.

    ``expected`` is the search API's count, given for a download only: fewer
    rows is a truncated download, which is removed so a re-run fetches it again.
    (More is a record released mid-download.)
    """
    retrieval_date, url = _comment(path)
    echoed = tuple(urllib.parse.parse_qs(urllib.parse.urlparse(url).query).get("field", ()))
    if echoed != fields:
        raise ValueError(f"encode: {path} does not echo the requested fields; differs in "
                         f"{sorted(set(echoed) ^ set(fields)) or 'order'}")
    n = duckdb.sql(f"SELECT count(*) FROM {_read(path, fields)}").fetchone()[0]
    if not n:
        raise ValueError(f"encode: {path} has zero rows")
    if expected is not None and n < expected:
        path.unlink()
        raise ValueError(f"encode: {path} has {n:,} rows but the portal reports {expected:,}; "
                         "short download, removed, nothing landed")
    return retrieval_date


def _select_sql(path: Path, fields: tuple[str, ...], version: str, limit: int | None) -> str:
    limit_sql = f" LIMIT {int(limit)}" if limit else ""
    return f"SELECT *, '{version}' AS retrieval_date FROM {_read(path, fields)}{limit_sql}"


def curate(
    con: duckdb.DuckDBPyConnection,
    experiments: Path,
    files: Path,
    *,
    expected: dict[str, int] | None = None,
    schema: str = "encode",
    limit: int | None = None,
) -> dict[str, int]:
    """MERGE-upsert both reports into ``experiment`` and ``file``; return each table's rows.

    Both reports are checked whole before either lands; each table is stamped
    with its own report's date. ``expected`` holds the search API's counts for
    downloaded reports. A full load prunes objects gone from the portal;
    ``limit`` never prunes. ``retrieval_date`` is compared like any column, so a
    new crawl restamps every row: each table is one crawl, never a mix.
    """
    # limit=0 is falsy: it would read every row and prune as a full load
    if limit is not None and limit < 1:
        raise ValueError(f"encode: limit must be positive, got {limit}")
    paths = {"experiment": experiments, "file": files}
    versions = {t: _verify(paths[t], fields, (expected or {}).get(t))
                for t, (_, fields, _) in TABLES.items()}
    return {
        t: upsert(con, f"{LAKE}.{schema}.{t}",
                  _select_sql(paths[t], fields, versions[t], limit),
                  key=key, prune=None if limit else "true")
        for t, (_, fields, key) in TABLES.items()
    }


def ingest(
    *,
    experiments: str | None = None,
    files: str | None = None,
    schema: str = "encode",
    limit: int | None = None,
    settings: Settings | None = None,
) -> dict:
    """End-to-end: download each report (unless given locally) -> MERGE-upsert -> summary.

    A local report skips the download and the search API's ``total`` check (how
    offline tests and re-exports stay offline). The run's version is the
    experiment report's date; the summary carries each table's.
    """
    s = settings or get_settings()
    today = datetime.now(UTC).date().isoformat()
    local = {"experiment": experiments, "file": files}
    paths, expected = {}, {}
    for t, (type_, _, _) in TABLES.items():
        if local[t]:
            paths[t] = Path(local[t])
        else:
            expected[t] = total(type_, s)
            paths[t] = download_report(t, today, s)
    version = _comment(paths["experiment"])[0]
    con = lake_connect(s)
    try:
        with ops.run(con, source="encode", target=f"{LAKE}.{schema}", version=version) as r:
            counts = curate(con, paths["experiment"], paths["file"], expected=expected,
                            schema=schema, limit=limit)
            r.rows = sum(counts.values())
    finally:
        con.close()
    return {**r.summary(), "schema": f"{LAKE}.{schema}", **counts}
