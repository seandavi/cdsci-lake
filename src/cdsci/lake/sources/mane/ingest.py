"""MANE summary -> ``lake.mane.transcript`` (EL only; bioc-on-ice#183).

MANE (Matched Annotation from NCBI and EMBL-EBI) names, per human protein-coding
gene, one transcript that RefSeq and Ensembl annotate identically (MANE Select),
plus a few MANE Plus Clinical ones. The summary file is one row per matched
RefSeq/Ensembl transcript pair (19,437 in v1.5). Ported from bioc-on-ice's
``src/bioconice/mane.py`` (its ``raw.ncbi__mane_summary`` landing); bioc-on-ice
derives ``annotation.mane__transcript`` and its ``identifier_mapping`` rows from
this table at release time (bioc-on-ice ADR-0015).

**Versioned by release number**, read from the file name
(``MANE.GRCh38.v1.5.summary.txt.gz`` -> ``1.5``), the form MANE itself cites.
Every release stays under ``release_X.Y/``; with no ``version`` the newest is
read off the ``current/`` index and fetched from its own release directory. A
file whose name carries no version needs ``version`` rather than an invented one.

The dialect is stated, not sniffed: tab-separated, no quoting, no comment
character (the header itself starts with ``#``), the empty cell for missing.
``-`` is only ever the minus strand, so it is data, not NULL. The header is
checked whole and every line's field count with it, before anything lands.

Every column lands VARCHAR under raw's names (lowercased, ``#`` dropped), so
``SELECT *`` reproduces ``raw.ncbi__mane_summary`` without ``landed_in``. The key
is the matched pair, ``(refseq_nuc, ensembl_nuc)``.

ponytail: only the summary is landed; the release's GFF/GTF/FASTA and the small
``changed_select_accessions`` / ``protein_coding_genes_not_in_mane`` TSVs are
their own tables when something needs them.
"""

from __future__ import annotations

import gzip
import re
from pathlib import Path

import duckdb
import httpx

from ... import ops
from ...config import Settings, get_settings
from ...connect import LAKE, csv_source, lake_connect, raw_dir, upsert
from ...download import download

_RAW = "mane"
SUMMARY = r"MANE\.GRCh38\.v(\d+(?:\.\d+)*)\.summary\.txt"

# The file's header, in file order. The lake lowercases these and drops the '#'.
COLUMNS = (
    "#NCBI_GeneID", "Ensembl_Gene", "HGNC_ID", "symbol", "name", "RefSeq_nuc", "RefSeq_prot",
    "Ensembl_nuc", "Ensembl_prot", "MANE_status", "GRCh38_chr", "chr_start", "chr_end",
    "chr_strand",
)


def _index(url: str) -> str:
    """An FTP-over-HTTPS directory listing's HTML."""
    resp = httpx.get(url, timeout=60, follow_redirects=True)
    resp.raise_for_status()
    return resp.text


def file_version(path: Path) -> str | None:
    """The release a summary file names (``MANE.GRCh38.v1.5.summary.txt.gz`` -> ``1.5``)."""
    m = re.search(SUMMARY, path.name)
    return m.group(1) if m else None


def latest(settings: Settings | None = None) -> str:
    """The release the ``current/`` index's summary file names, e.g. ``1.5``."""
    s = settings or get_settings()
    if not (m := re.search(SUMMARY, _index(f"{s.mane_base_url}current/"))):
        raise RuntimeError(f"mane: no summary file listed under {s.mane_base_url}current/")
    return m.group(1)


def download_summary(version: str, settings: Settings | None = None) -> Path:
    """Download one release's summary from its ``release_X.Y/`` directory into the raw layer."""
    s = settings or get_settings()
    name = f"MANE.GRCh38.v{version}.summary.txt.gz"
    return download(f"{s.mane_base_url}release_{version}/{name}", raw_dir(_RAW, s) / name)


def _check(path: Path) -> None:
    """The header whole, then every line's field count; a file with no rows is refused.

    No cell is quoted, so a line is a record and a field count other than the
    header's is a malformed file, wherever it is (not just within a limited load).
    """
    with (gzip.open if path.suffix == ".gz" else open)(path, "rb") as f:
        header = tuple(f.readline().decode("utf-8", "replace").rstrip("\r\n").split("\t"))
        if header != COLUMNS:
            raise ValueError(f"mane: {path} header is not the declared one; "
                             f"differs in {sorted(set(header) ^ set(COLUMNS))}")
        n = 0
        for n, line in enumerate(f, 1):
            if (fields := line.count(b"\t") + 1) != len(COLUMNS):
                raise ValueError(f"mane: {path} data line {n} has {fields} fields, "
                                 f"not {len(COLUMNS)}")
    if not n:
        raise ValueError(f"mane: {path} has zero rows")


def _select_sql(path: Path, version: str, limit: int | None) -> str:
    select = ", ".join(f'"{c}" AS {c.lstrip("#").lower()}' for c in COLUMNS)
    limit_sql = f" LIMIT {int(limit)}" if limit else ""
    return f"""
        SELECT {select}, '{version}' AS mane_version
        FROM read_csv({csv_source([path])}, header=true, all_varchar=true, delim='\\t',
                      quote='', escape='', comment='', nullstr='')
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
    """MERGE-upsert one summary into ``transcript`` on the matched pair; return the row count.

    The file is checked whole before it lands. A full load prunes pairs the
    release no longer has; ``limit`` never prunes. ``mane_version`` is compared
    like any column, so a new release restamps every row: the table is one
    release, never a mix.
    """
    _check(path)
    target = target or f"{LAKE}.mane.transcript"
    return upsert(
        con, target, _select_sql(path, version, limit),
        key=["refseq_nuc", "ensembl_nuc"], prune=None if limit else "true",
    )


def ingest(
    *,
    file: str | None = None,
    version: str | None = None,
    schema: str = "mane",
    limit: int | None = None,
    settings: Settings | None = None,
) -> dict:
    """End-to-end: download a release (unless ``file``) -> MERGE-upsert -> summary.

    ``file`` is a local summary; its name gives the version unless ``version`` is
    given. Otherwise ``version`` (default: :func:`latest`) is downloaded.
    """
    s = settings or get_settings()
    if file:
        path = Path(file)
        version = version or file_version(path)
        if not version:
            raise ValueError(f"mane: {path.name} does not name its release "
                             "(MANE.GRCh38.vX.Y.summary.txt); pass version")
    else:
        version = version or latest(s)
        path = download_summary(version, s)
    target = f"{LAKE}.{schema}.transcript"
    con = lake_connect(s)
    try:
        with ops.run(con, source="mane", target=target, version=version) as r:
            r.rows = curate(con, path, version, target=target, limit=limit)
    finally:
        con.close()
    return r.summary()
