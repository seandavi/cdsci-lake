"""Ingest UniProt's whole-of-UniProt ID-mapping dump into ``lake.uniprot.idmapping``.

UniProt publishes one un-split ``idmapping_selected.tab.gz`` covering **every**
UniProtKB entry regardless of organism — tab-delimited, **no header**, 22 columns
per UniProt's own README:

    UniProtKB-AC, UniProtKB-ID, GeneID (EntrezGene), RefSeq, GI, PDB, GO,
    UniRef100, UniRef90, UniRef50, UniParc, PIR, NCBI-taxon, MIM, UniGene,
    PubMed, EMBL, EMBL-CDS, Ensembl, Ensembl_TRS, Ensembl_PRO, Additional PubMed

An earlier version of this source downloaded per-organism files under
``idmapping/by_organism/`` instead. That was a scoping mistake (issue #32
follow-up): the lake's standing convention is "land raw whole," and
``by_organism/`` only covers a curated reference set anyway — not UniProt's full
breadth — so even looping over every file there wouldn't have been "whole."
``ncbi_taxon`` is already a per-row column in the un-split file, so nothing about
per-organism filtering is lost by dropping the split; it's just done downstream
(a view / WHERE clause) instead of at ingest time.

Only column 1 (accession) and column 3 (GeneID) feed the mapping this source
exists for — the rest land unused, per the "land raw whole" convention: don't
drop columns just because nothing reads them yet.

GeneID is itself ``;``-separated when one UniProt entry maps to more than one
Entrez gene, and a GeneID can just as easily map back to more than one
accession — so neither column alone is a key. Each row is exploded to one
``(accession, gene_id)`` pair, which **is** the natural key (MERGE-upsert,
ADR-0003). ``current_release`` carries no version in the URL (like
retractionwatch's rolling CSV), so the snapshot is tagged by pull date.

The un-split dump is ~6.6 GiB gzipped (confirmed via HEAD against the
ftp.expasy.org / ftp.ebi.ac.uk mirrors, 2026-08-11 — ftp.uniprot.org itself
times out intermittently for large GETs in this environment) — multi-GB, same
order of magnitude as the PMC bulk load. Since it's one un-splittable gzip
stream (no random access), ``ingest(batches=...)`` shards the *load* (not the
download — that still happens once) by ``hash(accession) % batches`` so no
single pass materializes the whole exploded result set at once, the same
memory-bounding lever ``openalex`` uses for its part-file batches and ``pmc``
uses for its passage-shard batches. ``mode="append"`` (vs the default
``"merge"``) additionally skips each batch's MERGE-read of the growing target —
valid because batches partition disjointly by accession, so it's safe for the
initial bulk load the same way it is for openalex/pmc.

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


def download_idmapping(version: str, settings: Settings | None = None) -> Path:
    """Download the whole-of-UniProt ID-mapping dump into the raw layer (kept as bronze)."""
    s = settings or get_settings()
    dest = raw_dir(_RAW, s) / f"{version}-idmapping_selected.tab.gz"
    return download(s.uniprot_idmapping_url, dest)


def _select_sql(
    path: Path, version: str, limit: int | None, batch: tuple[int, int] | None = None
) -> str:
    """Render the curate SELECT; ``batch=(i, n)`` keeps only the ``i``-th of ``n``
    ``hash(accession)``-mod shards — the memory-bounding lever (see module docstring).
    """
    columns_sql = ", ".join(f"'{c}': 'VARCHAR'" for c in _COLUMNS)
    passthrough = ", ".join(
        f'trim("{c}") AS {c}' for c in _COLUMNS if c not in ("accession", "gene_id_raw")
    )
    where = ["trim(accession) <> ''", "TRY_CAST(gid AS BIGINT) IS NOT NULL"]
    if batch is not None:
        i, n = batch
        where.append(f"(hash(accession) % {int(n)}) = {int(i)}")
    where_sql = " AND ".join(where)
    limit_sql = f" LIMIT {int(limit)}" if limit else ""
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
            CAST({_sql_str(version)} AS VARCHAR)   AS snapshot_version
        FROM raw, UNNEST({_gene_ids("gene_id_raw")}) AS t(gid)
        WHERE {where_sql}{limit_sql}
    """


def _load(
    con: duckdb.DuckDBPyConnection,
    target: str,
    source_sql: str,
    key: list[str],
    *,
    mode: str,
) -> int:
    """Dispatch one batch to MERGE-upsert (idempotent) or append-INSERT (disjoint bulk).

    ``mode="merge"`` (default) MERGE-upserts on ``key`` — safe for reruns/top-ups.
    ``mode="append"`` INSERTs without reading the target — for the initial bulk,
    whose accession-hash batches are disjoint, so a MERGE's whole-table read
    (growing every batch) is pure waste (the lesson from ``openalex``/``pmc``).
    """
    if mode == "merge":
        return upsert(con, target, source_sql, key, exclude_change_cols=["snapshot_version"])
    if mode != "append":
        raise ValueError(f"mode must be 'merge' or 'append', got {mode!r}")
    catalog, schema, _ = target.split(".")
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema};")
    con.execute(f"CREATE OR REPLACE TEMP TABLE _up_stage AS {source_sql};")
    con.execute(f"CREATE TABLE IF NOT EXISTS {target} AS SELECT * FROM _up_stage WHERE false;")
    con.execute(f"INSERT INTO {target} SELECT * FROM _up_stage;")
    return con.execute(f"SELECT count(*) FROM {target}").fetchone()[0]


def curate(
    con: duckdb.DuckDBPyConnection,
    path: Path,
    version: str,
    *,
    target: str | None = None,
    limit: int | None = None,
    batch: tuple[int, int] | None = None,
    mode: str = "merge",
) -> int:
    """Load one batch of the idmapping file, keyed ``(accession, gene_id)``.

    Returns the target's current full row count (see :func:`_load`).
    """
    target = target or f"{LAKE}.{_TABLE}"
    return _load(
        con, target, _select_sql(path, version, limit, batch),
        key=["accession", "gene_id"], mode=mode,
    )


def ingest(
    *,
    file: str | None = None,
    version: str | None = None,
    schema: str = "uniprot",
    limit: int | None = None,
    batches: int = 20,
    mode: str = "merge",
    settings: Settings | None = None,
) -> dict:
    """End-to-end: download the whole-of-UniProt dump (unless ``file``) → batched load → summary.

    ``batches`` shards the load by ``hash(accession) % batches`` to bound peak
    memory on the ~6.6 GiB gzip source (see module docstring); 1 disables sharding.
    ``mode="append"`` is valid (and faster) for the initial bulk load since batches
    are disjoint by accession — see :func:`_load`. ``file`` overrides the download
    with one local path — the local-fixture case (see ``census_geo``, ``reliance``
    for the same idiom).
    """
    s = settings or get_settings()
    version = version or _today_version()
    target = f"{LAKE}.{schema}.idmapping"

    con = lake_connect(s)
    try:
        with ops.run(con, source="uniprot", target=target, version=version) as r:
            path = Path(file) if file else download_idmapping(version, s)
            rows = 0
            for i in range(batches):
                shard = (i, batches) if batches > 1 else None
                rows = curate(
                    con, path, version, target=target, limit=limit, batch=shard, mode=mode
                )
            r.rows = rows
    finally:
        con.close()
    return r.summary()
