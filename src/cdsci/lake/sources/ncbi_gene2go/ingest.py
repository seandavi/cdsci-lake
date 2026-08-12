"""Land NCBI's ``gene2go`` dump into ``lake.ncbi_gene2go.gene2go`` (issue #39).

Everything mechanical here is :mod:`cdsci.lake.sources.ncbi_gene`'s: the same
FTP directory, the same unversioned-nightly treatment (the snapshot label is the
*retrieval date* -- these dumps regenerate nightly, so ``Last-Modified`` and any
checksum move daily on identical content, and there is no release number to
quote), and the same ``auto_detect=false`` / ``nullstr='-'`` / ``quote=''``
landing contract. This module therefore imports
:func:`~cdsci.lake.sources.ncbi_gene.download_dump` and
:func:`~cdsci.lake.sources.ncbi_gene.land` rather than restating them, and adds
only what is genuinely gene2go's: its column spec, its key, and its own
``ops`` source name.

Its **own** source key (``ncbi_gene2go``, own lake schema) on purpose: gene2go
and gene_info/gene2ensembl ingest independently, and one overwriting the other's
ledger row would misreport what either was built from.

Raw is landed whole -- ~48M rows, every organism NCBI annotates (1.37 GiB
gzipped, confirmed via HEAD 2026-08-11). Per-species scoping is a ``WHERE``
clause downstream, not a filter on the way in; see ncbi_gene's module docstring
for why raw must not be a function of what happens to be derived today.

``PubMed`` is kept as the raw pipe-separated PMID list -- splitting it is
interpretation, and it is the one column that legitimately changes for an
otherwise-identical annotation, which is exactly why it is *not* in
:data:`KEY`: a re-land with new citations UPDATEs its row instead of inserting a
second one.

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

_DUMP = "gene2go"
_log = logger.bind(ctx="ncbi_gene2go")

# File order (see ncbi_gene on why auto_detect is off). Upstream's '#tax_id' /
# 'GeneID' are named to match `lake.ncbi_gene.*` so the two join without casts.
COLUMNS: dict[str, str] = {
    "taxon_id": "INTEGER", "gene_id": "BIGINT", "go_id": "VARCHAR",
    "evidence": "VARCHAR", "qualifier": "VARCHAR", "go_term": "VARCHAR",
    "pubmed": "VARCHAR", "category": "VARCHAR",
}

# The natural key: one annotation is a (gene, term, evidence code, relation,
# aspect) tuple. Verified unique over 1.17M real rows / 20 taxa sliced out of
# the live dump. `pubmed` and `go_term` are the updatable attributes.
#
# bioc-on-ice's version COALESCE'd evidence/qualifier to '' before merging,
# because a NULL key column never equals itself and the row would retire and
# reappear on every merge. That normalization is not ported: `-` (NCBI's null
# marker) does not occur in any of the eight columns in the real slice, so the
# COALESCE would only obscure the day it does start occurring. If NCBI ever
# ships one, `test_land_is_idempotent` is what catches it.
KEY: list[str] = ["taxon_id", "gene_id", "go_id", "evidence", "qualifier", "category"]


def ingest(
    *,
    file: str | None = None,
    version: str | None = None,
    schema: str = "ncbi_gene2go",
    limit: int | None = None,
    batches: int = 20,
    mode: str = "merge",
    settings: Settings | None = None,
) -> dict:
    """Download gene2go (unless ``file``) → batched land → summary.

    ``batches`` shards the load by ``hash(gene_id) % batches`` -- the same
    memory-bounding lever ncbi_gene uses on a single un-splittable gzip stream;
    1 disables it. ``mode="append"`` skips the MERGE's read of the growing
    target, valid for an initial bulk load because gene_id-hash shards partition
    disjointly.
    """
    s = settings or get_settings()
    version = version or date.today().isoformat()

    con = lake_connect(s)
    try:
        with ops.run(
            con, source="ncbi_gene2go", target=f"{LAKE}.{schema}", version=version
        ) as r:
            path = Path(file) if file else download_dump(_DUMP, version, s)
            for i in range(batches):
                r.rows = land(
                    con, _DUMP, path, version, schema=schema, columns=COLUMNS, key=KEY,
                    limit=limit, batch=(i, batches) if batches > 1 else None, mode=mode,
                )
            _log.info("{} <- {:,} rows", _DUMP, r.rows)
    finally:
        con.close()
    return {**r.summary(), "schema": schema}
