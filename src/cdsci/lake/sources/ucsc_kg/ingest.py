"""Ingest UCSC Known Gene cross-references into ``lake.ucsc.known_gene_xref``.

UCSC's Genome Browser publishes its MySQL tables as per-assembly dumps under
``hgdownload.soe.ucsc.edu/goldenPath/<build>/database/``. Two of them carry the
``UCSCKG`` identifier bioc-on-ice's OrgDb-parity column list needs (issue #55):

* ``kgXref`` — one row per Known Gene transcript, with its mRNA / UniProt /
  symbol / RefSeq cross-references and description.
* ``knownToLocusLink`` — the **direct** ``UCSCKG -> Entrez gene_id`` mapping
  (``name`` = kgID, ``value`` = Entrez GeneID). No intermediate crosswalk.

Both are raw MySQL table dumps: tab-delimited, gzipped, **no header**. The
column order comes from the ``<table>.sql`` file sitting next to each ``.txt.gz``
in the same directory (verified against the real hg38/hg19/mm39 dumps,
2026-08-11) — never from guessing.

``knownToLocusLink`` is ``UNIQUE KEY (name)`` and its ids are a strict subset of
``kgXref``'s (verified on hg38: all 584,958 of its kgIDs appear among kgXref's
670,670), so the Entrez id folds into ``kgXref`` as a LEFT JOIN — one row per
transcript, one table, no second table to join at read time. A kgID with no
Entrez mapping keeps its row with ``gene_id IS NULL`` (85,712 of them on hg38);
that's the "land raw whole" convention, not a filter.

``kgID`` is declared as a *non-unique* MySQL ``KEY``, but is in fact unique in
every real dump checked (hg38 670,670/670,670, hg19 639,904/639,904, mm39
481,956/481,956), so ``(genome_build, kg_id)`` is the upsert key.

Genome build is a **column**, not a hardcode: ``--builds hg38,mm39`` loads any
number of assemblies into the same table. The default is ``hg38`` alone —
UCSC has hundreds of assemblies and most have no ``knownGene``/``kgXref`` at
all, so "whole" here means "every column of every row of the builds asked for",
not "every assembly UCSC hosts".

The files are small (hg38: 15 MB + 2.8 MB gzipped) — no batching machinery.

License: **no license needed for UCSC's table data**. genome.ucsc.edu/license
(confirmed 2026-08-11): "the raw table data and binary files used to create the
graphics by the browser is freely available for both public and commercial use.
This applies to data that is downloaded as files via http, https, ftp or rsync".
The stated exceptions are liftOver chain files (non-commercial) and certain
restricted clinical/GISAID tracks (HGMD, LOVD, OMIM, COSMIC, …, which UCSC does
not redistribute at all) — ``kgXref``/``knownToLocusLink`` are neither.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb

from ... import ops
from ...config import Settings, get_settings
from ...connect import LAKE, csv_source, lake_connect, raw_dir, upsert
from ...download import download

_TABLE = "known_gene_xref"
_RAW = "ucsc_kg"  # raw-download subdir name

# kgXref column order, per https://hgdownload.soe.ucsc.edu/goldenPath/<build>/
# database/kgXref.sql (kgID, mRNA, spID, spDisplayID, geneSymbol, refseq,
# protAcc, description, rfamAcc, tRnaName) -> snake_case.
KGXREF_COLUMNS = (
    "kg_id", "mrna", "sp_id", "sp_display_id", "gene_symbol", "refseq",
    "prot_acc", "description", "rfam_acc", "trna_name",
)
# knownToLocusLink.sql: `name` (the kgID) + `value` (the Entrez GeneID).
LOCUSLINK_COLUMNS = ("kg_id", "gene_id")


def _sql_str(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _today_version() -> str:
    """Default snapshot label — the pull date (the dumps carry no version tag)."""
    return date.today().isoformat()


def _read_sql(path: Path, columns: tuple[str, ...]) -> str:
    """SQL: read one headerless UCSC MySQL dump with its schema-file column names.

    Dialect stated, not sniffed: ``description`` is free text that really does
    contain quote characters (7 such rows in mm39's kgXref), so ``quote=''`` /
    ``escape=''`` — a sniffed ``"`` would swallow tabs. Every dialect option is
    given, so ``auto_detect=false`` turns the sniffer off outright: on an older
    assembly whose ``kgXref`` predates the trailing rfamAcc/tRnaName columns
    (hg18's really is 8 columns, per its own ``.sql``) the sniffer otherwise
    refuses the file for having fewer columns than declared, where
    ``null_padding`` just fills the missing ones with NULL.
    """
    cols = ", ".join(f"'{c}': 'VARCHAR'" for c in columns)
    return (
        f"read_csv({csv_source([path])}, delim = '\\t', header = false, "
        f"quote = '', escape = '', columns = {{{cols}}}, "
        f"null_padding = true, auto_detect = false)"
    )


def download_table(
    build: str, table: str, version: str, settings: Settings | None = None
) -> Path:
    """Download one UCSC ``<build>/database/<table>.txt.gz`` into the raw layer."""
    s = settings or get_settings()
    dest = raw_dir(_RAW, s) / f"{build}-{version}-{table}.txt.gz"
    return download(f"{s.ucsc_goldenpath_base}/{build}/database/{table}.txt.gz", dest)


def _select_sql(
    build: str, kgxref: Path, locuslink: Path, version: str, limit: int | None
) -> str:
    limit_sql = f" LIMIT {int(limit)}" if limit else ""
    # Empty string is MySQL's "no value" here (the columns are NOT NULL), so it
    # lands as NULL rather than '' — a consumer joining on refseq/sp_id should
    # not have to know that.
    passthrough = ",\n            ".join(
        f"nullif(trim(x.{c}), '') AS {c}" for c in KGXREF_COLUMNS if c != "kg_id"
    )
    return f"""
        SELECT
            CAST({_sql_str(build)} AS VARCHAR) AS genome_build,
            x.kg_id,
            {passthrough},
            TRY_CAST(l.gene_id AS BIGINT) AS gene_id,
            CAST({_sql_str(version)} AS VARCHAR) AS snapshot_version
        FROM {_read_sql(kgxref, KGXREF_COLUMNS)} AS x
        LEFT JOIN {_read_sql(locuslink, LOCUSLINK_COLUMNS)} AS l USING (kg_id)
        WHERE nullif(trim(x.kg_id), '') IS NOT NULL{limit_sql}
    """


def curate(
    con: duckdb.DuckDBPyConnection,
    build: str,
    kgxref: Path,
    locuslink: Path,
    version: str,
    *,
    target: str | None = None,
    limit: int | None = None,
) -> int:
    """MERGE-upsert one build's dumps into ``known_gene_xref`` on ``(genome_build, kg_id)``."""
    target = target or f"{LAKE}.ucsc.{_TABLE}"
    return upsert(
        con, target, _select_sql(build, kgxref, locuslink, version, limit),
        key=["genome_build", "kg_id"], exclude_change_cols=["snapshot_version"],
    )


def ingest(
    *,
    builds: str = "hg38",
    kgxref_file: str | None = None,
    locuslink_file: str | None = None,
    version: str | None = None,
    schema: str = "ucsc",
    limit: int | None = None,
    settings: Settings | None = None,
) -> dict:
    """End-to-end: download each build's two dumps (unless given) → upsert → summary.

    ``builds`` is a comma-separated assembly list (default ``hg38``).
    ``kgxref_file``/``locuslink_file`` load local dumps instead of downloading,
    and so apply to a single build only.
    """
    s = settings or get_settings()
    version = version or _today_version()
    names = [b.strip() for b in builds.split(",") if b.strip()]
    if not names:
        raise ValueError("builds must name at least one genome assembly")
    if (kgxref_file or locuslink_file) and len(names) != 1:
        raise ValueError("kgxref_file/locuslink_file apply to a single build")

    target = f"{LAKE}.{schema}.{_TABLE}"
    con = lake_connect(s)
    try:
        with ops.run(con, source="ucsc_kg", target=target, version=version) as r:
            for build in names:
                kx = (
                    Path(kgxref_file) if kgxref_file
                    else download_table(build, "kgXref", version, s)
                )
                ll = (
                    Path(locuslink_file) if locuslink_file
                    else download_table(build, "knownToLocusLink", version, s)
                )
                r.rows = curate(con, build, kx, ll, version, target=target, limit=limit)
    finally:
        con.close()
    return r.summary()
