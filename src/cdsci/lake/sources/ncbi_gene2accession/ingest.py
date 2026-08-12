"""Land NCBI's ``gene2accession`` dump into ``lake.ncbi_gene2accession`` (issue #37).

Same file family, same unversioned-nightly/retrieval-date treatment and the same
landing mechanics as ``ncbi_gene`` (#36) — so the download and the load are that
module's :func:`~cdsci.lake.sources.ncbi_gene.download_dump` /
:func:`~cdsci.lake.sources.ncbi_gene.land`, imported rather than restated. Like
``ncbi_gene2pubmed`` (#38), this module is only the three things that genuinely
differ: the column spec, the key, and the scale.

**Scale — the part that is actually different.** Measured, not assumed (a full
streaming pass over the real file, 2026-08-11):

===========================  ==============================================
``gene2accession.gz``        4,598,626,972 B (4.28 GiB), HEAD
uncompressed TSV             31,301,943,449 B (29.2 GiB), 6.8x ratio
rows                         **284,846,230** (~110 B/row)
columns                      16 on every row (no ragged lines)
ordering                     tax_id ascending, monotone over the whole file
===========================  ==============================================

Worth stating plainly because the issue plans around a different number: the
issue says "~1e9 rows upstream". The real file is **285M**, ~3.5x smaller. It is
still the lake's largest landing (4x ``gene_info`` by rows, 19x by bytes), but it
is an overnight job, not a multi-day one. Two levers, both already paid for:

* **Spill goes to ``/data``, never ``/home``.** Nothing source-specific is needed
  — every ingest connects through :func:`cdsci.lake.connect.lake_connect`, whose
  ``_apply_limits`` points DuckDB's ``temp_directory`` at
  ``Settings.duckdb_temp_directory``. Set that (the deployment does) before
  running this source: PMC's passages explosion spilled 100+ GB, and exhausting
  the catalog disk aborts a multi-hour load with an IO error.
* **``batches`` shards the load** by ``hash(gene_id) % batches``. The default is
  **40**, not ``ncbi_gene``'s 20, which puts ~7.1M rows / ~780 MB of TSV in each
  batch — the same per-batch working set ``gene_info`` runs with, so the peak
  temp footprint is a known quantity rather than a new one. The cost of more
  batches is that each one re-scans the whole file; measured, DuckDB reads this
  gzip at ~450 MB/s of uncompressed CSV (~70 s/pass warm), so 40 passes is
  ~1 h of scanning, and the parquet write dominates instead. Expect an
  overnight-shaped job and roughly 6-12 GB of parquet for the raw table
  (extrapolated from 20 B/row measured on a real 2.4M-row slice).
  Operator lever if a pass ever is the bottleneck: ``zcat`` the dump onto
  ``/data`` once and pass ``--file`` — a plain TSV is seekable and DuckDB scans
  it ~4x faster (measured 2 GB/s) with every thread. Deliberately not automated:
  one line of shell, and it costs 29 GiB of disk most runs don't want.

For the initial bulk load pass ``mode="append"``: the ``gene_id``-hash shards
partition disjointly, so the MERGE's read of an ever-growing target buys nothing.
``mode="merge"`` stays the default because idempotency is the safe default, but
note what it costs *here* — one full read of a ~300M-row target per batch.

``auto_detect`` is off and the column spec is stated below in file order, so a
column NCBI inserts or reorders fails loudly instead of shifting every value one
to the left. ``nullstr='-'`` is NCBI's own absent marker (it is what makes the
accession columns nullable at all). ``quote=''`` because these NCBI TSVs carry
bare ``"`` in free-text fields.

**One sentinel collision, unique to this dump**: ``orientation`` uses ``-`` for
the minus strand, which is the same token as NCBI's null marker, so a
minus-strand row lands with ``orientation IS NULL``. That is recoverable, not
lost: NCBI writes ``?`` (never ``-``) when the orientation is unknown — verified
on real bytes, the column is only ever ``+``/``-``/``?`` across 2.4M bacterial
rows and the human rows in the fixture — so ``coalesce(orientation, '-')``
reconstructs the file value exactly, and ``tests/test_ncbi_gene2accession.py``
pins that premise. Not worked around at read time on purpose: DuckDB's
``nullstr`` is per-``read_csv``, not per-column, so the alternative is dropping
it for all sixteen columns and pushing NCBI's ``-`` sentinel onto every consumer.

Accession **versions are kept exactly as the file gives them** (``NM_000546.6``,
not ``NM_000546``): the versioned form is what the record asserts, and dropping
the version is interpretation a reader can do with ``split_part``.

Reverse-ETL to bioc-on-ice (the issue's third section, including its question
about full-refresh publish cost at this volume) is deliberately **not** here —
see cdsci-lake#63; build in the lake first, publish later if at all. This module
therefore does not close #37.

License: US government work (NCBI/NLM) — public domain.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

from ... import ops
from ...config import Settings, get_settings
from ...connect import LAKE, lake_connect
from ...log import logger
from ..ncbi_gene import download_dump, land

DUMP = "gene2accession"

# File order, `auto_detect = false` (see the module docstring). Upstream's
# '#tax_id'/'GeneID'/'RNA_nucleotide_accession.version'/... are renamed here --
# with an explicit spec the names come from this table, not the header line, and
# 'accession.version' is not a legal column name anyway. `_gi` columns are NCBI
# GI numbers; positions are 1-based inclusive on the genomic accession.
COLUMNS: dict[str, str] = {
    "taxon_id": "INTEGER", "gene_id": "BIGINT", "status": "VARCHAR",
    "rna_accession": "VARCHAR", "rna_gi": "BIGINT",
    "protein_accession": "VARCHAR", "protein_gi": "BIGINT",
    "genomic_accession": "VARCHAR", "genomic_gi": "BIGINT",
    "start_position": "BIGINT", "end_position": "BIGINT", "orientation": "VARCHAR",
    "assembly": "VARCHAR", "mature_peptide_accession": "VARCHAR",
    "mature_peptide_gi": "BIGINT", "symbol": "VARCHAR",
}

# Every identifying + placement column. The accession triple alone is NOT unique
# (verified on a real 2.4M-row slice: 19 duplicate groups -- one gene's RNA/protein
# pair recurs at several genomic locations), so the coordinates and the assembly
# are part of the key. `symbol` and the `_gi` columns are the only non-key
# payload, which is right: those are the columns that can change for a row that
# is otherwise the same assertion.
KEY: list[str] = [
    "taxon_id", "gene_id", "status", "rna_accession", "protein_accession",
    "genomic_accession", "start_position", "end_position", "orientation",
    "assembly", "mature_peptide_accession",
]

_log = logger.bind(ctx="ncbi_gene2accession")


def ingest(
    *,
    file: str | None = None,
    version: str | None = None,
    schema: str = "ncbi_gene2accession",
    limit: int | None = None,
    batches: int = 40,
    mode: str = "merge",
    settings: Settings | None = None,
) -> dict:
    """Download ``gene2accession.gz`` (unless ``file``) → batched land → summary.

    ``version`` defaults to the retrieval date: the dump regenerates nightly and
    carries no release number, so a fabricated one would be a lie (#36's rule).
    ``file`` loads a local path instead — the fixture idiom the tests use, and
    the ``zcat``-to-``/data`` operator lever in the module docstring.
    ``batches`` shards the load by ``hash(gene_id) % batches``; 1 disables it.
    """
    s = settings or get_settings()
    version = version or date.today().isoformat()
    path = Path(file) if file else download_dump(DUMP, version, s)

    con = lake_connect(s)
    try:
        with ops.run(
            con, source="ncbi_gene2accession", target=f"{LAKE}.{schema}", version=version
        ) as r:
            for i in range(batches):
                r.rows = land(
                    con, DUMP, path, version, schema=schema, columns=COLUMNS, key=KEY,
                    limit=limit, batch=(i, batches) if batches > 1 else None, mode=mode,
                )
                _log.debug("batch {}/{} done — {:,} rows in target", i + 1, batches, r.rows)
            _log.info("{} <- {:,} rows", DUMP, r.rows)
    finally:
        con.close()
    return {**r.summary(), "table": f"{LAKE}.{schema}.{DUMP}"}
