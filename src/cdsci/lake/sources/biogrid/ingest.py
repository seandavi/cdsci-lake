"""BioGRID tab3 -> ``lake.biogrid.interaction`` (EL only; bioc-on-ice#183).

BioGRID curates physical and genetic interactions for every organism, MIT
licensed. ``BIOGRID-ALL-x.y.zzz.tab3.zip`` holds one tab-separated text file,
one row per curated interaction evidence (2,936,093 in 5.0.261; 0.18 GB zipped,
1.56 GB unzipped). Ported from bioc-on-ice's ``src/bioconice/biogrid.py`` (its
``raw.biogrid__interactions`` landing); bioc-on-ice derives its
``annotation.interaction`` rows from this table at release time (bioc-on-ice
ADR-0015).

**Versioned by release number** (``5.0.261``), monthly. Every release stays
under BioGRID's Release-Archive, so ``version`` fetches that release and with
none the newest listed there is fetched. A local zip names its release
(``BIOGRID-ALL-5.0.261.tab3.zip``) unless ``version`` is given.

**The read is positional, so the file is checked whole first.** The header
must be exactly :data:`COLUMNS`, then DuckDB counts every row with a strict
column count, so a row with the wrong number of cells fails anywhere in the
file, not just within a limited load's rows. A file with no rows is refused.

The dialect is the lander's, verbatim: tab-delimited, quoting off (no ``"`` at
all in 5.0.261, so a future one is data rather than a quote), ``-`` is
BioGRID's missing marker and reads as NULL. Every column lands VARCHAR under
raw's names, so ``SELECT *`` reproduces ``raw.biogrid__interactions`` without
``landed_in``. The key is BioGRID's interaction id: unique on every row of a
release and stable across releases (bioc-on-ice's ``biogrid.py`` has the
measurement).
"""

from __future__ import annotations

import re
from pathlib import Path

import duckdb
import httpx

from ... import ops
from ...config import Settings, get_settings
from ...connect import LAKE, csv_source, lake_connect, raw_dir, upsert
from ...download import download, unzip

_RAW = "biogrid"
# BioGRID sits behind Cloudflare, which 403s urllib's default User-Agent.
UA = {"User-Agent": "cdsci-lake (https://github.com/seandavi/cdsci-lake)"}
RELEASE = r"\d+\.\d+\.\d+"

# The file's header -> column name, in file order.
COLUMNS = {
    "#BioGRID Interaction ID": "biogrid_interaction_id",
    "Entrez Gene Interactor A": "entrez_gene_a",
    "Entrez Gene Interactor B": "entrez_gene_b",
    "BioGRID ID Interactor A": "biogrid_id_a",
    "BioGRID ID Interactor B": "biogrid_id_b",
    "Systematic Name Interactor A": "systematic_name_a",
    "Systematic Name Interactor B": "systematic_name_b",
    "Official Symbol Interactor A": "official_symbol_a",
    "Official Symbol Interactor B": "official_symbol_b",
    "Synonyms Interactor A": "synonyms_a",
    "Synonyms Interactor B": "synonyms_b",
    "Experimental System": "experimental_system",
    "Experimental System Type": "experimental_system_type",
    "Author": "author",
    "Publication Source": "publication_source",
    "Organism ID Interactor A": "organism_id_a",
    "Organism ID Interactor B": "organism_id_b",
    "Throughput": "throughput",
    "Score": "score",
    "Modification": "modification",
    "Qualifications": "qualifications",
    "Tags": "tags",
    "Source Database": "source_database",
    "SWISS-PROT Accessions Interactor A": "swissprot_a",
    "TREMBL Accessions Interactor A": "trembl_a",
    "REFSEQ Accessions Interactor A": "refseq_a",
    "SWISS-PROT Accessions Interactor B": "swissprot_b",
    "TREMBL Accessions Interactor B": "trembl_b",
    "REFSEQ Accessions Interactor B": "refseq_b",
    "Ontology Term IDs": "ontology_term_ids",
    "Ontology Term Names": "ontology_term_names",
    "Ontology Term Categories": "ontology_term_categories",
    "Ontology Term Qualifier IDs": "ontology_term_qualifier_ids",
    "Ontology Term Qualifier Names": "ontology_term_qualifier_names",
    "Ontology Term Types": "ontology_term_types",
    "Organism Name Interactor A": "organism_name_a",
    "Organism Name Interactor B": "organism_name_b",
}


def file_version(path: Path) -> str | None:
    """The release a tab3 zip names (``BIOGRID-ALL-5.0.261.tab3.zip`` -> ``5.0.261``)."""
    m = re.search(rf"BIOGRID-ALL-({RELEASE})\.tab3", path.name)
    return m.group(1) if m else None


def latest(settings: Settings | None = None) -> str:
    """The newest release number in the Release-Archive index, e.g. ``5.0.263``."""
    s = settings or get_settings()
    url = f"{s.biogrid_base_url}BioGRID/Release-Archive/"
    resp = httpx.get(url, headers=UA, timeout=60, follow_redirects=True)
    resp.raise_for_status()
    found = set(re.findall(rf"BIOGRID-({RELEASE})", resp.text))
    if not found:
        raise RuntimeError(f"biogrid: no BIOGRID-x.y.zzz release listed at {url}")
    return max(found, key=lambda v: tuple(map(int, v.split("."))))


def download_release(version: str, settings: Settings | None = None) -> Path:
    """Download one release's ALL tab3 zip into the raw layer, keyed by ``version``."""
    s = settings or get_settings()
    name = f"BIOGRID-ALL-{version}.tab3.zip"
    url = f"{s.biogrid_base_url}Download/BioGRID/Release-Archive/BIOGRID-{version}/{name}"
    return download(url, raw_dir(_RAW, s) / version / name, headers=UA)


def extract(archive: Path, version: str, settings: Settings | None = None) -> Path:
    """Unzip the release's one text file into the raw layer beside the download."""
    members = unzip(archive, raw_dir(_RAW, settings) / version)
    if len(members) != 1:
        raise ValueError(f"biogrid: {archive} should hold one text file, "
                         f"found {[m.name for m in members]}")
    return members[0]


def _read(path: Path) -> str:
    columns = "{" + ", ".join(f"'{c}': 'VARCHAR'" for c in COLUMNS.values()) + "}"
    return (f"read_csv({csv_source([path])}, delim='\\t', header=true, auto_detect=false, "
            f"columns={columns}, quote='', escape='', nullstr='-')")


def _check(con: duckdb.DuckDBPyConnection, path: Path) -> None:
    """The header whole, then every row's cell count; a file with no rows is refused."""
    with open(path, encoding="utf-8", errors="replace") as f:
        header = f.readline().rstrip("\r\n").split("\t")
    if header != list(COLUMNS):
        raise ValueError(f"biogrid: {path} header is not the declared one; differs "
                         f"in {sorted(set(header) ^ set(COLUMNS)) or 'column order'}")
    if not con.execute(f"SELECT count(*) FROM {_read(path)}").fetchone()[0]:
        raise ValueError(f"biogrid: {path} has zero rows")


def curate(
    con: duckdb.DuckDBPyConnection,
    path: Path,
    version: str,
    *,
    target: str | None = None,
    limit: int | None = None,
) -> int:
    """MERGE-upsert one tab3 file into ``interaction`` on its id; return the row count.

    The file is checked whole before it lands. A full load prunes interactions
    the release withdrew; ``limit`` never prunes. ``biogrid_version`` is
    compared like any column, so a new release restamps every row: the table
    is one release, never a mix.
    """
    # limit=0 is falsy: it would read every row and prune as a full load
    if limit is not None and limit < 1:
        raise ValueError(f"biogrid: limit must be positive, got {limit}")
    _check(con, path)
    limit_sql = f" LIMIT {int(limit)}" if limit else ""
    target = target or f"{LAKE}.biogrid.interaction"
    return upsert(
        con, target, f"SELECT *, '{version}' AS biogrid_version FROM {_read(path)}{limit_sql}",
        key="biogrid_interaction_id", prune=None if limit else "true",
    )


def ingest(
    *,
    file: str | None = None,
    version: str | None = None,
    schema: str = "biogrid",
    limit: int | None = None,
    settings: Settings | None = None,
) -> dict:
    """End-to-end: download a release (unless ``file``) -> unzip -> MERGE-upsert -> summary.

    ``file`` is a local tab3 zip; its name gives the version unless ``version``
    is given. Otherwise ``version`` (default: :func:`latest`) is downloaded.
    """
    s = settings or get_settings()
    archive = Path(file) if file else None
    if archive:
        version = version or file_version(archive)
        if not version:
            raise ValueError(f"biogrid: {archive.name} does not name its release "
                             "(BIOGRID-ALL-x.y.zzz.tab3.zip); pass version")
    else:
        version = version or latest(s)
    # the version is interpolated into a URL, a path and the SQL
    if not re.fullmatch(RELEASE, version):
        raise ValueError(f"biogrid: version {version!r} is not a release number x.y.zzz")
    archive = archive or download_release(version, s)
    path = extract(archive, version, s)
    target = f"{LAKE}.{schema}.interaction"
    con = lake_connect(s)
    try:
        with ops.run(con, source="biogrid", target=target, version=version) as r:
            r.rows = curate(con, path, version, target=target, limit=limit)
    finally:
        con.close()
    return r.summary()
