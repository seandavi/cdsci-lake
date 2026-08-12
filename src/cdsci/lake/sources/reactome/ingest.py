"""Land Reactome's pathway TSVs into ``lake.reactome.*`` (issue #34, EL only).

Reactome is the lake's non-KEGG pathway source, chosen because its license is
unambiguous where KEGG's is not (#32/#33). Three flat, **headerless** TSVs from
``reactome.org/download/current/`` — small enough (98 MB / 1.6 MB / 0.6 MB,
2026-08-11) that none of the batching machinery ``uniprot``/``ncbi_gene`` need
is warranted here:

* ``NCBI2Reactome_All_Levels.txt`` → ``gene_pathway`` (788,194 rows). Gene →
  pathway at **every** level of the hierarchy, not the lowest-level-only
  ``NCBI2Reactome.txt``: land raw whole and let a consumer pick a level.
* ``ReactomePathways.txt`` → ``pathway`` (23,603 rows). Pathway id → name →
  species, independent of any gene.
* ``ReactomePathwaysRelation.txt`` → ``pathway_relation`` (23,717 rows).
  Parent/child edges, for an ancestor closure if one is ever wanted.

Note the filename: the issue says ``NCBI2Reactome_All_Level.txt`` (singular),
which 404s. The real file is ``_All_Levels``.

Verified against the real 2026-08-11 files, not assumed:

* Keys hold exactly. ``(gene_id, pathway_id, evidence_code)`` is unique across
  all 788,194 rows; ``pathway_id`` across all 23,603; ``(parent, child)`` across
  all 23,717. The issue's suggested ``taxon_id`` component is **redundant**: a
  Reactome stable id is species-scoped (``R-HSA-`` = human, ``R-MMU-`` = mouse,
  …) and 0 of the 16 species prefixes map to more than one species.
* **Species is a name, not a taxon id.** The column holds ``Homo sapiens``, not
  ``9606``. It lands verbatim as ``species``; resolving it to an NCBI taxon id
  is a downstream/transform concern (``lake.ncbi_gene`` or a taxonomy source is
  the authority), not something to invent a 16-row mapping for here.
* **``gene_id`` is VARCHAR, deliberately.** 232 rows carry a GenBank/RefSeq
  accession rather than an Entrez id (``MN908947.3`` = SARS-CoV-2,
  ``NC_012920`` = the mitochondrial genome, …). A ``BIGINT`` column would NULL
  those — and a NULL key column silently breaks the MERGE (``NULL = NULL`` never
  matches, so every load re-inserts). Landed whole as text; a numeric join to
  ``lake.ncbi_gene.gene_info`` is ``TRY_CAST(gene_id AS BIGINT)`` downstream.
* ``pathway_url`` is ``https://reactome.org/PathwayBrowser/#/<pathway_id>`` for
  every one of the 788,194 rows — kept because raw lands whole, but derivable
  and safely ignorable.
* No null sentinels of any kind (no ``-``, ``\\N``, or empty field in any of the
  three files), so no ``nullstr`` — unlike NCBI's dumps, a bare ``-`` here would
  be a real value. 4,946 names carry padding whitespace, which is trimmed.

``auto_detect=false`` with the column spec stated in file order, so a column
Reactome inserts or reorders fails loudly instead of shifting every value one to
the left. ``quote=''``/``escape=''``: pathway names are free text (the current
release happens to contain no ``"``, but a sniffed quote char would silently
swallow tabs the day one appears).

Releases are quarterly and the files carry no version tag, so the snapshot label
is the retrieval date — the ``retractionwatch``/``ncbi_gene`` treatment.

License: **CC0**. reactome.org/license §1(c) (confirmed 2026-08-11): "All data
in the Reactome database and files derived from that data are licensed under the
Creative Commons Public Domain Dedication (CC0). User may copy, modify, and
distribute these data, even for commercial purposes, without asking for
permission." (§1(a) CC BY 4.0 and §1(b) Apache 2.0 cover the *illustrations* and
the *software* respectively — neither applies to these TSVs.)

Scope: EL only. Reverse-ETL to bioc-on-ice stays blocked on the schema decision
issue #34 itself names (a Reactome-specific table vs. a general ``pathway`` table
stacking across sources), and deferred per #63.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import NamedTuple

import duckdb

from ... import ops
from ...config import Settings, get_settings
from ...connect import LAKE, csv_source, lake_connect, raw_dir, upsert
from ...download import download
from ...log import logger

_RAW = "reactome"  # raw-download subdir name
_log = logger.bind(ctx="reactome")


class Dump(NamedTuple):
    """One upstream TSV: its filename, its columns *in file order*, its key."""

    file: str
    columns: tuple[str, ...]
    key: tuple[str, ...]


# Keyed by lake table name. All columns land VARCHAR: these are raw tables, and
# the one numeric-looking column (gene_id) genuinely isn't always numeric.
DUMPS: dict[str, Dump] = {
    "gene_pathway": Dump(
        "NCBI2Reactome_All_Levels.txt",
        ("gene_id", "pathway_id", "pathway_url", "pathway_name", "evidence_code", "species"),
        ("gene_id", "pathway_id", "evidence_code"),
    ),
    "pathway": Dump(
        "ReactomePathways.txt",
        ("pathway_id", "pathway_name", "species"),
        ("pathway_id",),
    ),
    "pathway_relation": Dump(
        "ReactomePathwaysRelation.txt",
        ("parent_pathway_id", "child_pathway_id"),
        ("parent_pathway_id", "child_pathway_id"),
    ),
}


def _sql_str(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _today_version() -> str:
    """Default snapshot label — the retrieval date (the files carry no release)."""
    return date.today().isoformat()


def download_dump(name: str, version: str, settings: Settings | None = None) -> Path:
    """Download one Reactome TSV into the raw layer (kept as bronze)."""
    s = settings or get_settings()
    dump = DUMPS[name]
    dest = raw_dir(_RAW, s) / f"{version}-{dump.file}"
    return download(f"{s.reactome_base_url}{dump.file}", dest)


def _select_sql(path: Path, dump: Dump, version: str, limit: int | None) -> str:
    """Render the landing SELECT for one headerless Reactome TSV."""
    columns_sql = ", ".join(f"'{c}': 'VARCHAR'" for c in dump.columns)
    # Names arrive padded (4,946 of them); trim every column rather than
    # remember which. A blank key column would break the MERGE (NULL never
    # matches NULL, so every load would re-insert), so drop such rows outright —
    # none exist today, and this is what keeps that true.
    projection = ",\n            ".join(f"trim({c}) AS {c}" for c in dump.columns)
    where = " AND ".join(f"nullif(trim({k}), '') IS NOT NULL" for k in dump.key)
    limit_sql = f" LIMIT {int(limit)}" if limit else ""
    return f"""
        SELECT
            {projection},
            CAST({_sql_str(version)} AS VARCHAR) AS snapshot_version
        FROM read_csv(
            {csv_source([path])}, delim = '\\t', header = false, auto_detect = false,
            columns = {{{columns_sql}}}, quote = '', escape = ''
        )
        WHERE {where}{limit_sql}
    """


def land(
    con: duckdb.DuckDBPyConnection,
    name: str,
    path: Path,
    version: str,
    *,
    schema: str = "reactome",
    limit: int | None = None,
) -> int:
    """MERGE-upsert one Reactome TSV into ``lake.<schema>.<name>``; return its row count."""
    dump = DUMPS[name]
    target = f"{LAKE}.{schema}.{name}"
    return upsert(
        con, target, _select_sql(path, dump, version, limit),
        key=list(dump.key), exclude_change_cols=["snapshot_version"],
    )


def ingest(
    *,
    dump: str | None = None,
    file: str | None = None,
    version: str | None = None,
    schema: str = "reactome",
    limit: int | None = None,
    settings: Settings | None = None,
) -> dict:
    """End-to-end: download each TSV (unless ``file``) → MERGE-upsert → summary.

    ``dump`` lands only one of ``gene_pathway`` / ``pathway`` / ``pathway_relation``;
    omit it for all three. ``file`` loads a local TSV instead of downloading, and
    so requires ``dump``.
    """
    s = settings or get_settings()
    version = version or _today_version()
    names = [dump] if dump else list(DUMPS)
    unknown = [n for n in names if n not in DUMPS]
    if unknown:
        raise ValueError(f"unknown dump(s) {unknown} — pick from {list(DUMPS)}")
    if file and len(names) != 1:
        raise ValueError("file=... loads a single dump — pass dump='gene_pathway' too")

    con = lake_connect(s)
    try:
        counts: dict[str, int] = {}
        with ops.run(
            con, source="reactome", target=f"{LAKE}.{schema}", version=version
        ) as r:
            for name in names:
                path = Path(file) if file else download_dump(name, version, s)
                counts[name] = land(con, name, path, version, schema=schema, limit=limit)
                _log.info("{} <- {:,} rows", name, counts[name])
            r.rows = sum(counts.values())
    finally:
        con.close()
    return {**r.summary(), "schema": schema, "counts": counts}
