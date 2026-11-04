"""RNAcentral ``id_mapping.tsv.gz`` -> ``lake.rnacentral.id_mapping`` (EL only; bioc-on-ice#183).

RNAcentral (EMBL-EBI; CC0 from release 20) is the non-coding RNA sequence
aggregator: one ``URS`` id per distinct sequence, cross-referenced to the
records of its member databases (ENA, Rfam, Ensembl, RefSeq, miRBase, HGNC, ...).
``id_mapping.tsv.gz`` is that whole cross-reference set as one headerless TSV:
264,333,345 rows over 56 databases in release 27, 2.66 GB gzipped, sorted by
URS. Ported from bioc-on-ice's ``src/bioconice/rnacentral.py`` (its
``raw.rnacentral__id_mapping`` landing); bioc-on-ice derives
``annotation.rnacentral__rna`` from this table at release time (bioc-on-ice
ADR-0015). ENA's ``accession:range:feature`` rows (86%) land like any other.

**Versioned by release number.** ``current_release/`` is a moving alias, so it
is only asked which release it is (the ``RNAcentral Release NN`` banner of its
``release_notes.txt``); the file is read from ``releases/NN.0/``, which stays
put. ``rnacentral_release`` stamps every row.

**A row has no identifier.** No column or set the file declares is a key, so
the table is keyed by ``line_number``, the 1-based line, and ``SELECT * EXCLUDE
(line_number)`` reproduces ``raw.rnacentral__id_mapping`` without ``landed_in``.

**The read is positional, so the file is checked whole first**: every line must
have its 6 fields, so a malformed line (a blank one included) fails anywhere in
the file, not just within a limited load's rows. A file with no rows is refused.

The dialect is the lander's, verbatim: tab-delimited, CRLF, no header, an empty cell
is the only missing marker (``gene_name``, 86% of rows), quoting off (the file
quotes nothing, and a stray ``"`` in a gene name must not swallow the lines
after it).

**Scale.** The load is sharded by ``line_number % batches`` (default 20, ~13M
rows a batch), as ``ncbi_gene2accession`` shards its 285M rows: each batch
re-reads the file and MERGEs and prunes only its own shard.
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
from ...log import logger

_RAW = "rnacentral"
NUMBER = r"\d+"

# The file has no header, so the names are bioc-on-ice's and the order is the readme's.
COLUMNS = {"urs": "VARCHAR", "database": "VARCHAR", "external_id": "VARCHAR",
           "taxon_id": "INTEGER", "rna_type": "VARCHAR", "gene_name": "VARCHAR"}

_log = logger.bind(ctx="rnacentral")


def latest(settings: Settings | None = None) -> str:
    """The number of the release ``current_release/`` names, e.g. ``28``."""
    s = settings or get_settings()
    resp = httpx.get(f"{s.rnacentral_base_url}current_release/release_notes.txt",
                     timeout=60, follow_redirects=True)
    resp.raise_for_status()
    banner = re.search(r"RNAcentral Release (\d+)", resp.text[:1000])
    if not banner:
        raise RuntimeError("rnacentral: no 'RNAcentral Release NN' banner in "
                           "current_release/release_notes.txt")
    return banner.group(1)


def download_release(version: str, settings: Settings | None = None) -> Path:
    """Download one release's ``id_mapping.tsv.gz`` into the raw layer, keyed by ``version``."""
    s = settings or get_settings()
    url = f"{s.rnacentral_base_url}releases/{version}.0/id_mapping/id_mapping.tsv.gz"
    return download(url, raw_dir(_RAW, s) / version / "id_mapping.tsv.gz")


def _check(path: Path) -> int:
    """Every line's field count, then the line count; a file with no rows is refused.

    No cell holds a tab or a line break, so a line is a record and a field count
    other than 6 is a malformed file, wherever it is.
    """
    opener = gzip.open if path.suffix == ".gz" else open
    n = 0
    with opener(path, "rb") as f:
        for n, line in enumerate(f, 1):
            if (fields := line.count(b"\t") + 1) != len(COLUMNS):
                raise ValueError(f"rnacentral: {path} line {n} has {fields} fields, "
                                 f"not {len(COLUMNS)}")
    if not n:
        raise ValueError(f"rnacentral: {path} has zero rows")
    return n


def _read(path: Path) -> str:
    columns = "{" + ", ".join(f"'{c}': '{t}'" for c, t in COLUMNS.items()) + "}"
    # The CSV scan is order-preserving and row_number() OVER () streams in that
    # order, so the n-th row is the n-th line (checked to be one record each);
    # checked under 8 threads, batched, by tests/test_rnacentral.py, as intact's is.
    return (f"SELECT row_number() OVER ()::INTEGER AS line_number, * "
            f"FROM read_csv({csv_source([path])}, delim='\\t', header=false, auto_detect=false, "
            f"columns={columns}, nullstr='', quote='', escape='')")


def curate(
    con: duckdb.DuckDBPyConnection,
    path: Path,
    version: str,
    *,
    target: str | None = None,
    limit: int | None = None,
    batches: int = 20,
) -> int:
    """MERGE-upsert one release's id_mapping into ``id_mapping``; return the row count.

    Keyed ``line_number``. The file is checked whole before any of it lands. A
    full load prunes lines the release no longer has, each batch within its own
    shard; ``limit`` never prunes and is one batch. ``rnacentral_release`` is
    compared like any column, so a new release restamps every row: the table is
    one release, never a mix.
    """
    # limit=0 is falsy: it would read every row and prune as a full load
    if limit is not None and limit < 1:
        raise ValueError(f"rnacentral: limit must be positive, got {limit}")
    if batches < 1:
        raise ValueError(f"rnacentral: batches must be positive, got {batches}")
    lines = _check(path)
    # no shard may be empty: an empty load refuses to prune a shard that holds rows
    batches = min(batches, lines)
    target = target or f"{LAKE}.rnacentral.id_mapping"
    select = f"SELECT *, '{version}' AS rnacentral_release FROM ({_read(path)})"
    if limit:
        return upsert(con, target, f"{select} LIMIT {int(limit)}", key="line_number")
    n = 0
    for i in range(batches):
        shard = f"(line_number % {batches}) = {i}"
        n = upsert(con, target, f"SELECT * FROM ({select}) WHERE {shard}",
                   key="line_number", prune=shard)
        _log.debug("batch {}/{} done — {:,} rows in target", i + 1, batches, n)
    return n


def ingest(
    *,
    file: str | None = None,
    version: str | None = None,
    schema: str = "rnacentral",
    limit: int | None = None,
    batches: int = 20,
    settings: Settings | None = None,
) -> dict:
    """End-to-end: download a release (unless ``file``) -> MERGE-upsert -> summary.

    ``file`` is a local ``id_mapping.tsv.gz``; a ``releases/NN.0/id_mapping/``
    path names the version unless ``version`` is given. Otherwise ``version``
    (default: :func:`latest`) is downloaded.
    """
    s = settings or get_settings()
    path = Path(file) if file else None
    if path:
        numbered = re.search(r"/releases/(\d+)\.0/id_mapping/", str(path.resolve()))
        version = version or (numbered and numbered.group(1))
        if not version:
            raise ValueError(f"rnacentral: {path} is not under releases/NN.0/id_mapping/; "
                             "pass version")
    else:
        version = version or latest(s)
    # the version is interpolated into a URL, a path and the SQL
    if not re.fullmatch(NUMBER, version):
        raise ValueError(f"rnacentral: version {version!r} is not a release number")
    path = path or download_release(version, s)
    target = f"{LAKE}.{schema}.id_mapping"
    con = lake_connect(s)
    try:
        with ops.run(con, source="rnacentral", target=target, version=version) as r:
            r.rows = curate(con, path, version, target=target, limit=limit, batches=batches)
    finally:
        con.close()
    return r.summary()
