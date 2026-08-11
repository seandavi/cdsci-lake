"""Ingest UniProt's organism-scoped ID-mapping dumps into ``lake.uniprot.idmapping``.

UniProt publishes one ``<ORGANISM>_idmapping_selected.tab.gz`` per organism under
``idmapping/by_organism/`` — tab-delimited, **no header**, 22 columns per
UniProt's own README:

    UniProtKB-AC, UniProtKB-ID, GeneID (EntrezGene), RefSeq, GI, PDB, GO,
    UniRef100, UniRef90, UniRef50, UniParc, PIR, NCBI-taxon, MIM, UniGene,
    PubMed, EMBL, EMBL-CDS, Ensembl, Ensembl_TRS, Ensembl_PRO, Additional PubMed

Only column 1 (accession) and column 3 (GeneID) feed the mapping this source
exists for — the rest land unused, per the "land raw whole" convention (issue
#32): don't drop columns just because nothing reads them yet.

GeneID is itself ``;``-separated when one UniProt entry maps to more than one
Entrez gene, and a GeneID can just as easily map back to more than one
accession — so neither column alone is a key. Each row is exploded to one
``(accession, gene_id)`` pair, which **is** the natural key (MERGE-upsert,
ADR-0003). ``current_release`` carries no version in the URL (like
retractionwatch's rolling CSV), so the snapshot is tagged by pull date.

License: CC BY 4.0 (confirmed at https://www.uniprot.org/help/license,
2026-08-11) — carried forward via ``ops.SOURCES``.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb

from ... import ops
from ...config import Settings, get_settings
from ...connect import LAKE, csv_source, lake_connect, raw_dir, upsert
from ...download import download

_TABLE = "uniprot.idmapping"  # lake table (schema.table)
_RAW = "uniprot"  # raw-download subdir name

# Column order per UniProt's idmapping_selected README — see module docstring.
# Everything but `accession`/`gene_id_raw` lands unused (raw VARCHAR, untouched).
_COLUMNS = (
    "accession", "uniprotkb_id", "gene_id_raw", "refseq", "gi", "pdb", "go",
    "uniref100", "uniref90", "uniref50", "uniparc", "pir", "ncbi_taxon", "mim",
    "unigene", "pubmed", "embl", "embl_cds", "ensembl", "ensembl_trs",
    "ensembl_pro", "additional_pubmed",
)


def _sql_str(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _today_version() -> str:
    """Default snapshot label — the pull date (``current_release`` has no version)."""
    return date.today().isoformat()


def _gene_ids(col: str) -> str:
    """SQL: a ``;``-separated GeneID list → trimmed, non-empty ``VARCHAR[]`` to UNNEST."""
    return (
        f'list_transform(list_filter(string_split(coalesce("{col}", \'\'), \';\'), '
        f"x -> trim(x) <> ''), x -> trim(x))"
    )


def download_idmapping(organism: str, version: str, settings: Settings | None = None) -> Path:
    """Download one organism's ID-mapping file into the raw layer (kept as bronze)."""
    s = settings or get_settings()
    filename = f"{organism}_idmapping_selected.tab.gz"
    dest = raw_dir(_RAW, s) / f"{version}-{filename}"
    return download(f"{s.uniprot_idmapping_base_url}{filename}", dest)


def _select_sql(path: Path, organism: str, version: str, limit: int | None) -> str:
    limit_sql = f" LIMIT {int(limit)}" if limit else ""
    columns_sql = ", ".join(f"'{c}': 'VARCHAR'" for c in _COLUMNS)
    passthrough = ", ".join(
        f'trim("{c}") AS {c}' for c in _COLUMNS if c not in ("accession", "gene_id_raw")
    )
    return f"""
        WITH raw AS (
            SELECT * FROM read_csv(
                {csv_source([path])}, delim = '\\t', header = false,
                quote = '', escape = '',
                columns = {{{columns_sql}}}, all_varchar = true,
                sample_size = -1, ignore_errors = true
            )
        )
        SELECT
            trim(accession)                       AS accession,
            TRY_CAST(gid AS BIGINT)                AS gene_id,
            {passthrough},
            CAST({_sql_str(organism)} AS VARCHAR)  AS organism,
            CAST({_sql_str(version)} AS VARCHAR)   AS snapshot_version
        FROM raw, UNNEST({_gene_ids("gene_id_raw")}) AS t(gid)
        WHERE trim(accession) <> '' AND TRY_CAST(gid AS BIGINT) IS NOT NULL{limit_sql}
    """


def curate(
    con: duckdb.DuckDBPyConnection,
    path: Path,
    organism: str,
    version: str,
    *,
    target: str | None = None,
    limit: int | None = None,
) -> int:
    """MERGE-upsert one organism's file into ``idmapping``, keyed ``(accession, gene_id)``."""
    target = target or f"{LAKE}.{_TABLE}"
    return upsert(
        con, target, _select_sql(path, organism, version, limit),
        key=["accession", "gene_id"], exclude_change_cols=["snapshot_version"],
    )


def ingest(
    *,
    organisms: list[str] | None = None,
    file: str | None = None,
    version: str | None = None,
    schema: str = "uniprot",
    limit: int | None = None,
    settings: Settings | None = None,
) -> dict:
    """End-to-end: download each organism's file (unless ``file``) → MERGE-upsert → summary.

    ``organisms`` defaults to :attr:`Settings.uniprot_organisms` (human only).
    ``file`` overrides the download for every organism in the loop with one
    local path — the single-organism/local-fixture case (see ``census_geo``,
    ``reliance`` for the same idiom).
    """
    s = settings or get_settings()
    version = version or _today_version()
    orgs = organisms or list(s.uniprot_organisms)
    target = f"{LAKE}.{schema}.idmapping"

    con = lake_connect(s)
    try:
        counts: dict[str, int] = {}
        with ops.run(con, source="uniprot", target=target, version=version) as r:
            for organism in orgs:
                path = Path(file) if file else download_idmapping(organism, version, s)
                counts[organism] = curate(con, path, organism, version, target=target, limit=limit)
            r.rows = con.execute(f"SELECT count(*) FROM {target}").fetchone()[0]
    finally:
        con.close()
    return {**r.summary(), "counts": counts}
