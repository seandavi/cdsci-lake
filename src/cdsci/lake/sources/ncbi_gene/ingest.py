"""Land NCBI Gene's bulk dumps into ``lake.ncbi_gene.*`` (issue #36).

NCBI Gene is the lake's **unversioned** case. There is no release number: the
dumps under ``ftp.ncbi.nlm.nih.gov/gene/DATA/`` are regenerated nightly, so
``Last-Modified`` and any published checksum change daily on identical content.
The snapshot label is therefore the *retrieval date* -- never a fabricated
release number -- the same treatment ``retractionwatch`` already gets here.

Raw is landed **whole**: every organism NCBI knows, not just the couple of
species anything currently derives annotation for. An earlier version of this
ingest (in bioc-on-ice, where it came from) filtered ``tax_id`` on the way in to
avoid landing rows nothing read; that made raw a function of what happens to be
derived today, so a third species cost a full re-fetch. Per-species scoping
lives downstream in a ``WHERE`` clause, where it's free.

``gene_info`` is ~71.5M rows / 1.56 GiB gzipped (confirmed via HEAD, 2026-08-11)
-- one un-splittable gzip stream, so ``ingest(batches=...)`` shards the *load*
(not the download) by ``hash(gene_id) % batches``, exactly the memory-bounding
lever ``uniprot`` and ``pmc`` use. ``mode="append"`` additionally skips each
batch's MERGE-read of the growing target: valid for the initial bulk load
because gene_id-hash batches partition disjointly.

``auto_detect`` is off against these files and the column spec is stated here in
file order, so a column NCBI inserts or reorders fails loudly instead of quietly
shifting every value one to the left. ``nullstr='-'`` is NCBI's own absent
marker. Quoting is disabled (``quote=''``): these are plain TSVs whose
description fields contain bare ``"`` characters that would otherwise swallow
the rest of the row.

:func:`download_dump` and :func:`land` are deliberately dump-agnostic (name +
column spec + key in, rows out) because the gene2accession / gene2pubmed /
gene2go migrations (#37/#38/#39) differ from this module only in URL, column
spec and key -- they should import these two rather than restate them three
times.

License: US government work (NCBI/NLM) -- public domain. Carried forward via
``ops.SOURCES``.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb

from ... import ops
from ...config import Settings, get_settings
from ...connect import LAKE, csv_source, lake_connect, raw_dir, upsert
from ...download import download
from ...log import logger

_RAW = "ncbi_gene"  # raw-download subdir name
_log = logger.bind(ctx="ncbi_gene")

# One column spec per dump, in file order (see module docstring on auto_detect).
# Upstream's '#tax_id' is named `taxon_id` here: with an explicit spec the names
# come from this table, not from the header line.
DUMP_COLUMNS: dict[str, dict[str, str]] = {
    "gene_info": {
        "taxon_id": "INTEGER", "gene_id": "BIGINT", "symbol": "VARCHAR",
        "locus_tag": "VARCHAR", "synonyms": "VARCHAR", "dbxrefs": "VARCHAR",
        "chromosome": "VARCHAR", "map_location": "VARCHAR", "description": "VARCHAR",
        "type_of_gene": "VARCHAR", "symbol_authority": "VARCHAR",
        "full_name_authority": "VARCHAR", "nomenclature_status": "VARCHAR",
        "other_designations": "VARCHAR", "modification_date": "VARCHAR",
        "feature_type": "VARCHAR",
    },
    "gene2ensembl": {
        "taxon_id": "INTEGER", "gene_id": "BIGINT", "ensembl_gene_id": "VARCHAR",
        "rna_accession": "VARCHAR", "ensembl_rna_id": "VARCHAR",
        "protein_accession": "VARCHAR", "ensembl_protein_id": "VARCHAR",
    },
}

# Natural key per dump. `gene_info` is one row per GeneID. `gene2ensembl` has no
# single-column key -- one GeneID has a row per (transcript, protein) pair -- so
# every identifier column is the key; the MERGE then degenerates to
# insert-if-absent, which is correct because there is no non-key column to update.
DUMP_KEYS: dict[str, list[str]] = {
    "gene_info": ["gene_id"],
    "gene2ensembl": [
        "gene_id", "ensembl_gene_id", "rna_accession", "ensembl_rna_id",
        "protein_accession", "ensembl_protein_id",
    ],
}


def _sql_str(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _today_version() -> str:
    """Default snapshot label -- the retrieval date (the dumps carry no release)."""
    return date.today().isoformat()


def download_dump(name: str, version: str, settings: Settings | None = None) -> Path:
    """Download one ``gene/DATA/<name>.gz`` dump into the raw layer (kept as bronze).

    Dump-agnostic on purpose -- see the module docstring: #37/#38/#39 pass their
    own ``name`` here rather than restating the URL join.
    """
    s = settings or get_settings()
    dest = raw_dir(_RAW, s) / f"{version}-{name}.gz"
    return download(f"{s.ncbi_gene_base_url}{name}.gz", dest)


def _select_sql(
    path: Path,
    version: str,
    columns: dict[str, str],
    limit: int | None,
    batch: tuple[int, int] | None,
) -> str:
    """Render the landing SELECT; ``batch=(i, n)`` keeps the ``i``-th of ``n``
    ``hash(gene_id)``-mod shards -- the memory-bounding lever (module docstring).
    """
    columns_sql = ", ".join(f"'{c}': '{t}'" for c, t in columns.items())
    where = ["gene_id IS NOT NULL"]
    if batch is not None:
        i, n = batch
        where.append(f"(hash(gene_id) % {int(n)}) = {int(i)}")
    limit_sql = f" LIMIT {int(limit)}" if limit else ""
    return f"""
        SELECT *, CAST({_sql_str(version)} AS VARCHAR) AS snapshot_version
        FROM read_csv(
            {csv_source([path])}, delim = '\\t', header = true, auto_detect = false,
            columns = {{{columns_sql}}}, nullstr = '-', quote = '', escape = ''
        )
        WHERE {" AND ".join(where)}{limit_sql}
    """


def _load(
    con: duckdb.DuckDBPyConnection,
    target: str,
    source_sql: str,
    key: list[str],
    *,
    mode: str,
) -> int:
    """MERGE-upsert (idempotent) or append-INSERT (disjoint bulk) one batch.

    ``mode="append"`` skips the MERGE's read of a target that grows with every
    batch -- safe only because the shards partition disjointly by ``gene_id``
    (the same lesson ``uniprot``/``openalex``/``pmc`` landed on).
    """
    if mode == "merge":
        return upsert(con, target, source_sql, key, exclude_change_cols=["snapshot_version"])
    if mode != "append":
        raise ValueError(f"mode must be 'merge' or 'append', got {mode!r}")
    catalog, schema, _ = target.split(".")
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema};")
    con.execute(f"CREATE OR REPLACE TEMP TABLE _ncbi_stage AS {source_sql};")
    con.execute(f"CREATE TABLE IF NOT EXISTS {target} AS SELECT * FROM _ncbi_stage WHERE false;")
    con.execute(f"INSERT INTO {target} SELECT * FROM _ncbi_stage;")
    return con.execute(f"SELECT count(*) FROM {target}").fetchone()[0]


def land(
    con: duckdb.DuckDBPyConnection,
    name: str,
    path: Path,
    version: str,
    *,
    schema: str = "ncbi_gene",
    columns: dict[str, str] | None = None,
    key: list[str] | None = None,
    limit: int | None = None,
    batch: tuple[int, int] | None = None,
    mode: str = "merge",
) -> int:
    """Land one NCBI Gene dump verbatim into ``lake.<schema>.<name>``; return its row count.

    ``columns``/``key`` default to this module's :data:`DUMP_COLUMNS` /
    :data:`DUMP_KEYS`; the gene2* siblings (#37/#38/#39) pass their own and
    otherwise reuse this whole function unchanged.
    """
    target = f"{LAKE}.{schema}.{name}"
    sql = _select_sql(path, version, columns or DUMP_COLUMNS[name], limit, batch)
    return _load(con, target, sql, key or DUMP_KEYS[name], mode=mode)


def ingest(
    *,
    dump: str | None = None,
    file: str | None = None,
    version: str | None = None,
    schema: str = "ncbi_gene",
    limit: int | None = None,
    batches: int = 20,
    mode: str = "merge",
    settings: Settings | None = None,
) -> dict:
    """End-to-end: download each dump (unless ``file``) → batched land → summary.

    ``dump`` lands only one of ``gene_info`` / ``gene2ensembl``; omit it for both.
    ``file`` overrides the download with one local path (and then requires
    ``dump``) -- the local-fixture idiom ``reliance``/``census_geo`` use.
    ``batches`` shards the load by ``hash(gene_id) % batches``; 1 disables it.
    """
    s = settings or get_settings()
    version = version or _today_version()
    names = [dump] if dump else list(DUMP_COLUMNS)
    if file and len(names) != 1:
        raise ValueError("file=... loads a single dump — pass dump='gene_info' too")

    con = lake_connect(s)
    try:
        counts: dict[str, int] = {}
        with ops.run(
            con, source="ncbi_gene", target=f"{LAKE}.{schema}", version=version
        ) as r:
            for name in names:
                path = Path(file) if file else download_dump(name, version, s)
                for i in range(batches):
                    counts[name] = land(
                        con, name, path, version, schema=schema, limit=limit,
                        batch=(i, batches) if batches > 1 else None, mode=mode,
                    )
                _log.info("{} <- {:,} rows", name, counts[name])
            r.rows = sum(counts.values())
    finally:
        con.close()
    return {**r.summary(), "schema": schema, "counts": counts}
