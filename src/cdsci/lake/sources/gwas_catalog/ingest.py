"""NHGRI-EBI GWAS Catalog -> ``lake.gwas_catalog.{association,study}`` (EL only; bioc-on-ice#183).

Two files from one dated release directory: the ontology-annotated full
associations download (a zip holding one TSV; 1,192,604 rows at 2026-09-15) and
the v1.0.3.1 studies download (230,057 rows). Ported from bioc-on-ice's
``src/bioconice/gwas_catalog.py`` (its ``raw.gwas_catalog__{associations,studies}``
landing); bioc-on-ice derives ``clinical.gwas_catalog__{association,study}`` from
these tables at release time (bioc-on-ice ADR-0015). EMBL-EBI Terms of Use.

**Versioned by release directory.** Releases live under ``releases/YYYY/MM/DD/``;
``releases/latest/`` is a copy of the newest that says nowhere which one it is,
so the default walks the index to the newest dated directory. The date
(``YYYY-MM-DD``) is the Catalog's own release label and is ``gwas_catalog_release``.

Every column lands VARCHAR under the snake_cased name bioc-on-ice's raw tables
use, so ``SELECT * EXCLUDE (line_number)`` reproduces them. Each file's header is
checked whole (the file names carry a format version because the Catalog does
change it), and every line's field count with it, before anything lands.

**An association has no identifier in the download.** The file holds whole-row
duplicates (132 surplus rows at 2026-09-15), kept as published, so no set of its
columns can be a key: ``association`` is keyed by ``line_number``, the 1-based
data line in the TSV. ``study`` is keyed by ``study_accession``.

ponytail: the ancestry, unpublished-studies and efo-trait-mappings files are not
landed; each is its own table when something needs it.
"""

from __future__ import annotations

import re
import zipfile
from pathlib import Path

import duckdb
import httpx

from ... import ops
from ...config import Settings, get_settings
from ...connect import LAKE, csv_source, lake_connect, raw_dir, upsert
from ...download import download, unzip

_RAW = "gwas_catalog"
ASSOCIATIONS_ZIP = "gwas-catalog-associations_ontology-annotated-full.zip"
STUDIES_FILE = "gwas-catalog-download-studies-v1.0.3.1.txt"

# Upstream header -> column name, in file order. The two files spell the PubMed
# column differently ('PUBMEDID', 'PUBMED ID'); the lake spells it one way.
ASSOCIATIONS = {
    "DATE ADDED TO CATALOG": "date_added_to_catalog",
    "PUBMEDID": "pubmed_id",
    "FIRST AUTHOR": "first_author",
    "DATE": "date",
    "JOURNAL": "journal",
    "LINK": "link",
    "STUDY": "study",
    "DISEASE/TRAIT": "disease_trait",
    "INITIAL SAMPLE SIZE": "initial_sample_size",
    "REPLICATION SAMPLE SIZE": "replication_sample_size",
    "REGION": "region",
    "CHR_ID": "chr_id",
    "CHR_POS": "chr_pos",
    "REPORTED GENE(S)": "reported_genes",
    "MAPPED_GENE": "mapped_gene",
    "UPSTREAM_GENE_ID": "upstream_gene_id",
    "DOWNSTREAM_GENE_ID": "downstream_gene_id",
    "SNP_GENE_IDS": "snp_gene_ids",
    "UPSTREAM_GENE_DISTANCE": "upstream_gene_distance",
    "DOWNSTREAM_GENE_DISTANCE": "downstream_gene_distance",
    "STRONGEST SNP-RISK ALLELE": "strongest_snp_risk_allele",
    "SNPS": "snps",
    "MERGED": "merged",
    "SNP_ID_CURRENT": "snp_id_current",
    "CONTEXT": "context",
    "INTERGENIC": "intergenic",
    "RISK ALLELE FREQUENCY": "risk_allele_frequency",
    "P-VALUE": "p_value",
    "PVALUE_MLOG": "pvalue_mlog",
    "P-VALUE (TEXT)": "p_value_text",
    "OR or BETA": "or_beta",
    "95% CI (TEXT)": "ci_95_text",
    "PLATFORM [SNPS PASSING QC]": "platform",
    "CNV": "cnv",
    "MAPPED_TRAIT": "mapped_trait",
    "MAPPED_TRAIT_URI": "mapped_trait_uri",
    "STUDY ACCESSION": "study_accession",
    "GENOTYPING TECHNOLOGY": "genotyping_technology",
}
STUDIES = {
    "DATE ADDED TO CATALOG": "date_added_to_catalog",
    "PUBMED ID": "pubmed_id",
    "FIRST AUTHOR": "first_author",
    "DATE": "date",
    "JOURNAL": "journal",
    "LINK": "link",
    "STUDY": "study",
    "DISEASE/TRAIT": "disease_trait",
    "INITIAL SAMPLE SIZE": "initial_sample_size",
    "REPLICATION SAMPLE SIZE": "replication_sample_size",
    "PLATFORM [SNPS PASSING QC]": "platform",
    "ASSOCIATION COUNT": "association_count",
    "MAPPED_TRAIT": "mapped_trait",
    "MAPPED_TRAIT_URI": "mapped_trait_uri",
    "STUDY ACCESSION": "study_accession",
    "GENOTYPING TECHNOLOGY": "genotyping_technology",
    "SUBMISSION DATE": "submission_date",
    "STATISTICAL MODEL": "statistical_model",
    "BACKGROUND TRAIT": "background_trait",
    "MAPPED BACKGROUND TRAIT": "mapped_background_trait",
    "MAPPED BACKGROUND TRAIT URI": "mapped_background_trait_uri",
    "COHORT": "cohort",
    "FULL SUMMARY STATISTICS": "full_summary_statistics",
    "SUMMARY STATS LOCATION": "summary_stats_location",
    "GXE": "gxe",
}


def _dated(url: str) -> list[str]:
    """The all-digit subdirectories an FTP-over-HTTPS listing links to."""
    resp = httpx.get(url, timeout=60, follow_redirects=True)
    resp.raise_for_status()
    return re.findall(r'href="(\d+)/"', resp.text)


def latest(settings: Settings | None = None) -> str:
    """The newest dated release, ``YYYY-MM-DD``: the max YYYY/, then MM/, then DD/.

    Names are zero-padded, so the string max is the newest.
    """
    s = settings or get_settings()
    url, parts = s.gwas_catalog_releases_url, []
    for _ in range(3):
        if not (names := _dated(url)):
            raise RuntimeError(f"gwas_catalog: no dated directories under {url}")
        parts.append(max(names))
        url = f"{url}{parts[-1]}/"
    return "-".join(parts)


def _extract(archive: Path, dest: Path) -> Path:
    """The one TSV the associations zip holds, extracted (DuckDB does not read zips)."""
    with zipfile.ZipFile(archive) as z:
        if len(names := z.namelist()) != 1:
            raise ValueError(f"gwas_catalog: {archive} holds {names}, expected one TSV")
    return unzip(archive, dest)[0]


def download_release(version: str, settings: Settings | None = None) -> tuple[Path, Path]:
    """Download one release's two files into the raw layer: (associations TSV, studies)."""
    s = settings or get_settings()
    url = f"{s.gwas_catalog_releases_url}{version.replace('-', '/')}/"
    dest = raw_dir(_RAW, s) / version
    archive = download(url + ASSOCIATIONS_ZIP, dest / ASSOCIATIONS_ZIP)
    return _extract(archive, dest), download(url + STUDIES_FILE, dest / STUDIES_FILE)


def _check(path: Path, columns: dict[str, str]) -> None:
    """The header whole, then every line's field count; a file with no rows is refused.

    No cell holds a tab or a line break, so a line is a record and a field count
    other than the header's is a malformed file, wherever it is (not just within
    a limited load's rows).
    """
    with open(path, "rb") as f:
        header = tuple(f.readline().decode("utf-8", "replace").rstrip("\r\n").split("\t"))
        if header != tuple(columns):
            raise ValueError(f"gwas_catalog: {path} header is not the declared one; "
                             f"differs in {sorted(set(header) ^ set(columns))}")
        n = 0
        for n, line in enumerate(f, 1):
            if (fields := line.count(b"\t") + 1) != len(columns):
                raise ValueError(f"gwas_catalog: {path} data line {n} has {fields} fields, "
                                 f"not {len(columns)}")
    if not n:
        raise ValueError(f"gwas_catalog: {path} has zero rows")


def _select_sql(path: Path, columns: dict[str, str], version: str, limit: int | None,
                *, numbered: bool = False) -> str:
    select = ", ".join(f'"{h}" AS {c}' for h, c in columns.items())
    # The CSV scan is order-preserving and row_number() OVER () streams in that
    # order, so the n-th row is the n-th data line (checked above to be one record).
    line = "row_number() OVER ()::INTEGER AS line_number, " if numbered else ""
    limit_sql = f" LIMIT {int(limit)}" if limit else ""
    # The dialect is stated rather than sniffed, and it has NO quoting: a '"' is an
    # ordinary character here (3,182 study titles quote a phrase, and one risk
    # allele carries a stray unbalanced one), so any quote setting would swallow
    # tabs and newlines up to the next '"'. An empty cell is the only missing
    # marker and reads as NULL; 'NA' and 'NR' are the curators' "not applicable" /
    # "not reported" and are data, so they stay.
    return f"""
        SELECT {line}{select}, '{version}' AS gwas_catalog_release
        FROM read_csv({csv_source([path])}, header=true, all_varchar=true, delim='\\t',
                      quote='', escape='', comment='', nullstr='')
        {limit_sql}
    """


def curate(
    con: duckdb.DuckDBPyConnection,
    associations: Path,
    studies: Path,
    version: str,
    *,
    schema: str = "gwas_catalog",
    limit: int | None = None,
) -> dict[str, int]:
    """MERGE-upsert one release into ``study`` and ``association``; return each table's rows.

    Both files are checked whole before either lands. A full load prunes what the
    release no longer has; ``limit`` never prunes. ``gwas_catalog_release`` is
    compared like any column, so a new release restamps every row: each table is
    one release, never a mix.
    """
    _check(associations, ASSOCIATIONS)
    _check(studies, STUDIES)
    prune = None if limit else "true"
    return {
        "study": upsert(
            con, f"{LAKE}.{schema}.study", _select_sql(studies, STUDIES, version, limit),
            key="study_accession", prune=prune,
        ),
        "association": upsert(
            con, f"{LAKE}.{schema}.association",
            _select_sql(associations, ASSOCIATIONS, version, limit, numbered=True),
            key="line_number", prune=prune,
        ),
    }


def ingest(
    *,
    directory: str | None = None,
    version: str | None = None,
    schema: str = "gwas_catalog",
    limit: int | None = None,
    settings: Settings | None = None,
) -> dict:
    """End-to-end: download a release (unless ``directory``) -> MERGE-upsert -> summary.

    ``directory`` is a local copy of a release directory holding both files under
    their upstream names; a dated ``YYYY/MM/DD`` path names the version unless
    ``version`` is given. Otherwise ``version`` (default: the newest release,
    :func:`latest`) is downloaded.
    """
    s = settings or get_settings()
    if directory:
        d = Path(directory)
        dated = re.search(r"(\d{4})/(\d{2})/(\d{2})$", d.resolve().as_posix())
        version = version or (dated and "-".join(dated.groups()))
        if not version:
            raise ValueError(f"gwas_catalog: {d} is not <YYYY>/<MM>/<DD>/; pass version")
        associations = _extract(d / ASSOCIATIONS_ZIP, raw_dir(_RAW, s) / version)
        studies = d / STUDIES_FILE
    else:
        version = version or latest(s)
        associations, studies = download_release(version, s)
    con = lake_connect(s)
    try:
        with ops.run(con, source="gwas_catalog", target=f"{LAKE}.{schema}",
                     version=version) as r:
            counts = curate(con, associations, studies, version, schema=schema, limit=limit)
            r.rows = sum(counts.values())
    finally:
        con.close()
    return {**r.summary(), "schema": f"{LAKE}.{schema}", **counts}
