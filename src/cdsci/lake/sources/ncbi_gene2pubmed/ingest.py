"""Land NCBI's ``gene2pubmed`` dump into ``lake.ncbi_gene2pubmed`` (issue #38).

Same file family, same unversioned-nightly treatment and same landing mechanics
as ``ncbi_gene`` (#36) -- so the download and the load are that module's
:func:`~cdsci.lake.sources.ncbi_gene.download_dump` /
:func:`~cdsci.lake.sources.ncbi_gene.land`, imported rather than restated. This
module is only the two things that genuinely differ: the column spec and the key.

Raw is landed **whole** (every organism, ~40M rows / 269 MiB gzipped, confirmed
via HEAD 2026-08-11) -- per-species scoping is a downstream ``WHERE`` clause,
not a filter baked into raw. The load is sharded by ``hash(gene_id) % batches``
by the shared helper, the usual memory-bounding lever.

**Target schema decision** (the issue asks for this before any schema is
committed to): gene<->PubMed links get their **own** ``lake.ncbi_gene2pubmed``
schema; they are **not** an alias set keyed one-row-per-PMID. (The argument was
originally made against ``ref.id_crosswalk``, retired unused 2026-08-15 -- see
``docs/ROADMAP.md``; the grain reasoning stands on its own.) A per-PMID alias
row translates one publication identifier into its siblings (doi/pmcid/grant/
nct), all functionally dependent on the pmid. gene2pubmed is a genuine
many-to-many edge: one PMID cites many
genes (PMID 25750732 carries 32,224 gene rows in the first 8 MiB of the real
dump alone) and one gene is cited by many PMIDs. Folding it in would either
explode the crosswalk's grain
(every pmid row multiplied by its genes, breaking every existing consumer's
one-row-per-pmid assumption) or bury an array column whose cardinality dwarfs
the row itself. It is an edge table between two entities, not an alias set for
one -- so it stays a separate table, keyed on (gene_id, pmid), and a consumer
joins it on ``pmid`` directly to whichever source carries the identifier they
want (``icite.metadata`` for doi, ``pmc.documents`` for pmcid,
``reporter.publink`` for grants, ``ctgov.references`` for trials).

Reverse-ETL to bioc-on-ice (the issue's third section) is deliberately **not**
here -- see cdsci-lake#63; build in the lake first, publish later if at all.

License: US government work (NCBI/NLM) -- public domain.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

from ... import ops
from ...config import Settings, get_settings
from ...connect import LAKE, lake_connect
from ...log import logger
from ..ncbi_gene import download_dump, land

DUMP = "gene2pubmed"

# In file order, `auto_detect = false` (see ncbi_gene's module docstring): a
# column NCBI inserts or reorders must fail loudly, not shift values silently.
# Upstream's '#tax_id'/'GeneID'/'PubMed_ID' are renamed here -- with an explicit
# spec the names come from this table, not the header line.
COLUMNS: dict[str, str] = {"taxon_id": "INTEGER", "gene_id": "BIGINT", "pmid": "BIGINT"}

# The whole row is the key: there is no non-key column, so the MERGE degenerates
# to insert-if-absent, which is exactly right for an edge table.
KEY: list[str] = ["taxon_id", "gene_id", "pmid"]

_log = logger.bind(ctx="ncbi_gene2pubmed")


def ingest(
    *,
    file: str | None = None,
    version: str | None = None,
    schema: str = "ncbi_gene2pubmed",
    limit: int | None = None,
    batches: int = 20,
    mode: str = "merge",
    settings: Settings | None = None,
) -> dict:
    """Download ``gene2pubmed.gz`` (unless ``file``) → batched land → summary.

    ``version`` defaults to the retrieval date: the dump regenerates nightly and
    carries no release number, so a fabricated one would be a lie (#36's rule).
    ``file`` loads a local path instead -- the fixture idiom the tests use.
    ``batches`` shards the load by ``hash(gene_id) % batches``; 1 disables it.
    """
    s = settings or get_settings()
    version = version or date.today().isoformat()
    path = Path(file) if file else download_dump(DUMP, version, s)

    con = lake_connect(s)
    try:
        with ops.run(
            con, source="ncbi_gene2pubmed", target=f"{LAKE}.{schema}", version=version
        ) as r:
            for i in range(batches):
                r.rows = land(
                    con, DUMP, path, version, schema=schema, columns=COLUMNS, key=KEY,
                    limit=limit, batch=(i, batches) if batches > 1 else None, mode=mode,
                )
            _log.info("{} <- {:,} rows", DUMP, r.rows)
    finally:
        con.close()
    return {**r.summary(), "table": f"{LAKE}.{schema}.{DUMP}"}
