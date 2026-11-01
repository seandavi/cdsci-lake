"""HGNC complete set -> ``lake.hgnc.gene`` (EL only; bioc-on-ice#183 step 7).

HGNC is the authority for human gene symbols and names: one TSV, one row per
approved HGNC id (45,083 on 2026-09-18), CC0. Small enough that none of the
streaming machinery the NCBI dumps need is warranted. Ported from bioc-on-ice's
``src/bioconice/hgnc.py`` (its ``raw.hgnc__complete_set`` landing); bioc-on-ice
now derives ``annotation.hgnc__gene`` and its ``identifier_mapping`` rows from
this table at release time (bioc-on-ice ADR-0015).

**Versioned by date.** The rolling ``hgnc_complete_set.txt`` is regenerated in
place and carries no version in-band (no banner line; only Last-Modified and an
etag, which move with every regeneration), so by default the version is the
retrieval date. HGNC also freezes dated copies under
``archive/{monthly,quarterly}/tsv/hgnc_complete_set_YYYY-MM-DD.txt``; landing
one of those through ``file`` takes the date from the file name.

**The header is checked whole.** HGNC does change it: the archives through
2026-08-07 carry a ``location_sortable`` column that the 2026-09-04 archive and
the rolling file no longer have. A header that is not exactly :data:`COLUMNS`
fails before anything is read, rather than landing a file with a column
silently missing.

Every column lands VARCHAR, as published: the dates and ids are only ever
compared as text downstream, and ``omim_id``/``uniprot_ids`` etc. are
``|``-lists. Two upstream names are not valid bare column names
(``pseudogene.org``, ``mamit-trnadb``); they land with underscores.

ponytail: ``withdrawn.txt`` is not landed. The complete set holds approved
records only, so withdrawn and merged ids live in that second file, with their
own columns. Land it as ``hgnc.withdrawn_gene`` when something needs symbol
succession.
"""

from __future__ import annotations

import re
from datetime import date
from pathlib import Path

import duckdb

from ... import ops
from ...config import Settings, get_settings
from ...connect import LAKE, csv_source, lake_connect, raw_dir, upsert
from ...download import download

_RAW = "hgnc"

# The file's header, in file order.
COLUMNS = (
    "hgnc_id", "symbol", "name", "locus_group", "locus_type", "status", "location",
    "alias_symbol", "alias_name", "prev_symbol", "prev_name", "gene_group", "gene_group_id",
    "date_approved_reserved", "date_symbol_changed", "date_name_changed", "date_modified",
    "entrez_id", "ensembl_gene_id", "vega_id", "ucsc_id", "ena", "refseq_accession",
    "ccds_id", "uniprot_ids", "pubmed_id", "mgd_id", "rgd_id", "lsdb", "cosmic", "omim_id",
    "mirbase", "homeodb", "snornabase", "bioparadigms_slc", "orphanet", "pseudogene.org",
    "horde_id", "merops", "imgt", "iuphar", "kznf_gene_catalog", "mamit-trnadb", "cd",
    "lncrnadb", "enzyme_id", "intermediate_filament_db", "rna_central_id", "lncipedia",
    "gtrnadb", "agr", "mane_select", "gencc",
)


def _header(path: Path) -> tuple[str, ...]:
    """The file's first line, split. Only that line is read."""
    with open(path, encoding="utf-8", errors="replace") as f:
        return tuple(f.readline().rstrip("\r\n").split("\t"))


def file_version(path: Path) -> str | None:
    """The date a dated archive file names (``hgnc_complete_set_2026-09-04.txt``)."""
    m = re.search(r"hgnc_complete_set_(\d{4}-\d{2}-\d{2})\.", path.name)
    return m.group(1) if m else None


def download_tsv(version: str, settings: Settings | None = None) -> Path:
    """Download the rolling complete set into the raw layer, keyed by ``version``."""
    s = settings or get_settings()
    return download(s.hgnc_url, raw_dir(_RAW, s) / f"{version}-hgnc_complete_set.txt")


def _select_sql(path: Path, version: str, limit: int | None) -> str:
    select = ", ".join(f'"{c}" AS {c.replace(".", "_").replace("-", "_")}' for c in COLUMNS)
    limit_sql = f" LIMIT {int(limit)}" if limit else ""
    # The dialect is stated rather than sniffed. HGNC quotes the cells that hold
    # a '|'-separated list and nothing else; the quotes are the dialect, the
    # pipes are the value and stay. An empty cell is HGNC's missing marker.
    return f"""
        SELECT {select}, '{version}' AS hgnc_version
        FROM read_csv({csv_source([path])}, header=true, all_varchar=true, delim='\\t',
                      quote='"', escape='"', nullstr='')
        {limit_sql}
    """


def curate(
    con: duckdb.DuckDBPyConnection,
    path: Path,
    version: str,
    *,
    target: str | None = None,
    limit: int | None = None,
) -> int:
    """MERGE-upsert one complete set into ``gene`` on ``hgnc_id``; return the row count.

    A full load prunes ids dropped from the file (withdrawn or merged); ``limit``
    never prunes. ``hgnc_version`` is compared like any column, so a new version
    restamps every row: the table is one version, never a mix.
    """
    if (header := _header(path)) != COLUMNS:
        raise ValueError(
            f"hgnc: {path} header is not the declared one; "
            f"differs in {sorted(set(header) ^ set(COLUMNS))}"
        )
    target = target or f"{LAKE}.hgnc.gene"
    return upsert(
        con, target, _select_sql(path, version, limit),
        key="hgnc_id", prune=None if limit else "true",
    )


def ingest(
    *,
    file: str | None = None,
    version: str | None = None,
    schema: str = "hgnc",
    limit: int | None = None,
    settings: Settings | None = None,
) -> dict:
    """End-to-end: download (unless ``file``) -> MERGE-upsert -> summary.

    Version precedence: ``version``, then the date a dated archive ``file`` names,
    then today's date (the rolling file has none).
    """
    s = settings or get_settings()
    path = Path(file) if file else None
    version = version or (path and file_version(path)) or date.today().isoformat()
    path = path or download_tsv(version, s)
    target = f"{LAKE}.{schema}.gene"
    con = lake_connect(s)
    try:
        with ops.run(con, source="hgnc", target=target, version=version) as r:
            r.rows = curate(con, path, version, target=target, limit=limit)
    finally:
        con.close()
    return r.summary()
