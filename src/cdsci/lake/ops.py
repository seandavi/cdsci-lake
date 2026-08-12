"""``cdsci.lake.ops`` — the operational ledger (ADR-0006).

The questions DuckLake snapshots can't answer — *when did we last load a source,
did the run change/error, which snapshot did it produce, where do incrementals
resume* — are answered here. The ledger is **catalog-adjacent native state**, not
DuckLake data: a second attachment ``ops`` (the Postgres ``lake`` DB in
production, a sibling ``ops.duckdb`` locally) holding plain mutable tables.
:func:`cdsci.lake.connect.lake_connect` attaches it on the write path and calls
:func:`bootstrap`; read-only consumers never see it.

Ingestors don't touch SQL here — they wrap a curate in :func:`run` (a context
manager that records one ``lake_ops.run`` row, bracketing the upsert with the
before/after snapshot ids every ingestor used to hand-roll) and, for
incrementals, read/write a cursor via :func:`get_watermark` / :func:`set_watermark`.

Portability note: ``ops`` may be a real Postgres database reached through DuckDB's
``postgres`` extension, whose DDL surface is narrow. So the tables carry **no**
``SERIAL``/``DEFAULT``/``PRIMARY KEY``/foreign-key constraints — ``run_id`` is a
client-generated UUID, timestamps are written with ``current_timestamp``, the
watermark ``value`` is JSON text, and uniqueness is enforced in code (a registry
refresh and watermark set are delete-then-insert).
"""

from __future__ import annotations

import json
import socket
import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any

import duckdb

from .connect import LAKE
from .log import logger

OPS = "ops"  # the ATTACH alias for the ledger database
OPS_SCHEMA = "lake_ops"

# The run currently executing on this context, so :func:`cdsci.lake.connect.upsert`
# can self-attribute its snapshot (ADR-0009) without every curate threading the
# Run through. Set for the duration of a :func:`run` block.
_ACTIVE_RUN: ContextVar[Run | None] = ContextVar("active_run", default=None)


def active_run() -> Run | None:
    """The :class:`Run` for the enclosing :func:`run` block, or None outside one."""
    return _ACTIVE_RUN.get()


def _t(table: str) -> str:
    """Fully-qualified ledger table name, e.g. ``ops.lake_ops.run``."""
    return f"{OPS}.{OPS_SCHEMA}.{table}"


# --- The source registry (declared in code; materialized into lake_ops.source) ---


@dataclass(frozen=True)
class Source:
    """A registered source: its lake schema and how it refreshes."""

    name: str
    lake_schema: str
    description: str
    cadence: str
    distribution: str
    license: str
    watermark_strategy: str | None = None
    # The producer that owns this source (ADR-0011 §4). The shared ledger is
    # multi-producer, so each row records which writer registered it (`cdsci`,
    # `omicidx`), making "show me all of <producer>'s sources/loads" one query.
    writer: str = "cdsci"
    # The source's own entrypoint (issue #52): `ingest(**kwargs) -> dict`, self-
    # connecting and self-bracketed in `run()`. Left unset here -- `SOURCES`
    # below is imported by the base `cdsci.lake` package (the read-client
    # surface, no ingest deps installed) and importing all 14 `sources/*/ingest`
    # modules here would both violate that packaging boundary and cycle back
    # into this module (each imports `from ... import ops`). Instead
    # `sources/_cli.py` looks a source up by name and attaches its already
    # locally-imported `ingest` via `dataclasses.replace` at CLI-build time --
    # so a name absent from `SOURCES` still can't get a CLI, which is the
    # structural fix this field exists for.
    ingest: Callable[..., dict] | None = None


SOURCES: tuple[Source, ...] = (
    Source("reporter", "reporter", "NIH RePORTER ExPORTER (projects/abstracts/pubs/publink)",
           "per-fiscal-year", "nih-exporter", "us-public-domain"),
    Source("icite", "icite", "iCite article-level metrics (RCR) monthly snapshot",
           "monthly", "figshare", "us-public-domain"),
    Source("ctgov", "ctgov", "ClinicalTrials.gov v2-API full study records + nct↔pmid refs",
           "daily", "ctgov-api", "us-public-domain", watermark_strategy="page_token"),
    Source("scp", "scp", "State Cancer Profiles burden/risk/demographics",
           "monthly", "github-release", "us-public-domain"),
    Source("pmc", "pmc", "BioC-PMC full-text documents + passages",
           "on-rebuild", "biocpmc-bulk", "mixed-oa", watermark_strategy="max_range"),
    Source("openalex", "openalex", "OpenAlex works (Life+Health domains) + edge tables",
           "monthly", "s3-snapshot", "cc0", watermark_strategy="updated_date"),
    Source("census_geo", "ref", "US Census cartographic FIPS + boundaries (ref.geo_*)",
           "annual", "census-cartographic", "us-public-domain"),
    Source("europepmc", "europepmc", "Europe PMC text-mined annotations (PMCID↔term)",
           "monthly", "europepmc-bulk", "europepmc-terms"),
    Source("mesh", "mesh", "NLM MeSH controlled vocabulary: descriptors + tree + qualifiers",
           "annual", "nlm-xml", "us-public-domain"),
    Source("retractionwatch", "retractionwatch", "Retraction Watch retraction/correction notices",
           "weekday-daily", "crossref-gitlab-csv", "cc0", watermark_strategy="full"),
    Source("bugsigdb", "bugsigdb", "BugSigDB curated microbial signatures (per-study taxon "
           "contrasts)", "on-release", "github-release", "cc-by-4.0"),
    # CC BY-NC 4.0: internal non-commercial use only, do NOT redistribute. The
    # license string is the machine-readable carry-forward for consumers.
    Source("reliance", "reliance", "Reliance on Science (Marx): patent↔paper links [NC]",
           "annual", "zenodo", "cc-by-nc-4.0"),
    Source("bioregistry", "ref", "Bioregistry: canonical identifier prefixes, patterns, synonyms",
           "weekly", "github-tsv", "cc0"),
    Source("uniprot", "uniprot", "UniProt accession<->EntrezGene ID mapping (whole dump)",
           "~8-weekly", "uniprot-ftp", "cc-by-4.0"),
    Source("ncbi_gene", "ncbi_gene", "NCBI Gene bulk dumps: gene_info + gene2ensembl (all taxa)",
           "nightly", "ncbi-ftp", "us-public-domain"),
    Source("ncbi_gene2pubmed", "ncbi_gene2pubmed", "NCBI gene2pubmed: gene↔PMID links (all taxa)",
           "nightly", "ncbi-ftp", "us-public-domain"),
    Source("ncbi_gene2go", "ncbi_gene2go", "NCBI gene2go: GO annotations per Entrez gene (all "
           "taxa)", "nightly", "ncbi-ftp", "us-public-domain"),
    Source("ncbi_gene2accession", "ncbi_gene2accession",
           "NCBI gene2accession: gene↔RNA/protein/genomic accessions (all taxa)",
           "nightly", "ncbi-ftp", "us-public-domain"),
    Source("ontology", "ontology", "OBO semantic-sql builds: terms/synonyms/xrefs/edges",
           "on-release", "semsql-s3", "mixed"),
    # "ucsc-free": genome.ucsc.edu/license (2026-08-11) grants no-license-needed
    # public *and* commercial use of the browser's raw table data; the stated
    # exceptions (liftOver chains, restricted clinical/GISAID tracks) don't apply
    # to kgXref/knownToLocusLink. Not a standard SPDX/CC identifier, hence its own
    # string. Cite a UCSC publication when used in published work.
    Source("ucsc_kg", "ucsc", "UCSC Known Gene xrefs + UCSCKG<->Entrez mapping, per build",
           "on-assembly-update", "ucsc-goldenpath", "ucsc-free"),
    # `ensembl-no-restrictions`, not `cc0`: Ensembl names no license instrument, only
    # "imposes no restrictions on access to, or use of, the data" + a third-party-
    # constraints caveat (verified 2026-08-11 -- see sources/ensembl/ingest.py).
    Source("ensembl", "ensembl", "Ensembl per-species GTF gene annotation (raw GTF; "
           "genome/gene/transcript/exon models)", "per-release", "ensembl-ftp",
           "ensembl-no-restrictions"),
    Source("reactome", "reactome", "Reactome pathways: gene->pathway (all levels) + hierarchy",
           "quarterly", "reactome-download", "cc0"),
)


# --- Bootstrap (idempotent; run on every write-mode connect) ---


def bootstrap(con: duckdb.DuckDBPyConnection) -> None:
    """Create the ``lake_ops`` schema + tables (if absent). Schema only.

    Idempotent and cheap: ``CREATE … IF NOT EXISTS``. Assumes the ``ops`` database
    is already attached. Does **not** seed the source registry — each producer
    registers its own sources via :func:`register_sources` at its load entrypoint
    (ADR-0011 §4), so the shared ledger stays per-producer and the dependency arrow
    stays correct (a producer's source list lives with the producer).
    """
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {OPS}.{OPS_SCHEMA};")
    con.execute(
        f"""CREATE TABLE IF NOT EXISTS {_t("source")} (
            name TEXT, lake_schema TEXT, description TEXT, cadence TEXT,
            distribution TEXT, license TEXT, watermark_strategy TEXT,
            writer TEXT, registered_at TIMESTAMPTZ
        );"""
    )
    # Migrate a pre-PR `source` table (created before the writer column, so the
    # CREATE IF NOT EXISTS above is a no-op on it) — else register_sources INSERTs
    # into a missing column and crashes on the first real run (ADR-0011 §4).
    con.execute(f"ALTER TABLE {_t('source')} ADD COLUMN IF NOT EXISTS writer TEXT;")
    con.execute(f"UPDATE {_t('source')} SET writer = 'cdsci' WHERE writer IS NULL;")
    con.execute(
        f"""CREATE TABLE IF NOT EXISTS {_t("run")} (
            run_id TEXT, source TEXT, target TEXT, version TEXT, status TEXT,
            snapshot_before BIGINT, snapshot_after BIGINT, rows_after BIGINT,
            started_at TIMESTAMPTZ, finished_at TIMESTAMPTZ, error TEXT, host TEXT
        );"""
    )
    con.execute(
        f"""CREATE TABLE IF NOT EXISTS {_t("watermark")} (
            source TEXT, name TEXT, value TEXT, updated_at TIMESTAMPTZ, set_by_run TEXT
        );"""
    )
    con.execute(
        f"""CREATE TABLE IF NOT EXISTS {_t("dataset_contract")} (
            lake_schema TEXT, view_name TEXT, contract_version INTEGER, columns TEXT,
            backing_table TEXT, status TEXT, published_at TIMESTAMPTZ
        );"""
    )


def register_sources(
    con: duckdb.DuckDBPyConnection,
    *,
    writer: str,
    sources: tuple[Source, ...],
) -> None:
    """Register ``sources`` under producer ``writer`` (delete-then-insert; ADR-0011 §4).

    Each producer calls this once at its load entrypoint with its own source list —
    ``bootstrap`` no longer seeds, so the registry is per-producer. Idempotent and
    self-healing: the delete-then-insert is scoped to ``(name, writer)`` so a
    re-register refreshes a producer's rows without touching another producer's.
    """
    for s in sources:
        con.execute(
            f"DELETE FROM {_t('source')} WHERE name = ? AND writer = ?",
            [s.name, writer],
        )
        con.execute(
            f"INSERT INTO {_t('source')} "
            "(name, lake_schema, description, cadence, distribution, license, "
            " watermark_strategy, writer, registered_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, current_timestamp)",
            [s.name, s.lake_schema, s.description, s.cadence, s.distribution,
             s.license, s.watermark_strategy, writer],
        )


# --- The run ledger ---


@dataclass
class Run:
    """A live run handle. Set :attr:`rows` inside the :func:`run` block."""

    con: duckdb.DuckDBPyConnection
    run_id: str
    source: str
    target: str
    version: str | None
    snapshot_before: int | None
    writer: str = "cdsci"  # producer id; derived from the registry in run()
    rows: int | None = None
    snapshot_after: int | None = None
    status: str | None = None
    extra: dict | None = None  # per-producer commit_extra_info keys (ADR-0011 §5)
    _txn_depth: int = 0  # re-entrancy guard for nested attribute() blocks

    @property
    def changed(self) -> bool:
        """True when the upsert produced a new snapshot (not idempotent)."""
        return self.snapshot_after != self.snapshot_before

    def summary(self) -> dict:
        """The dict an ``ingest()`` returns — superset of the old hand-rolled one."""
        return {
            "table": self.target,
            "version": self.version,
            "rows": self.rows,
            "changed": self.changed,
            "snapshot": self.snapshot_after,
            "run_id": self.run_id,
            "status": self.status,
        }

    @contextmanager
    def attribute(self, op: str, *, message: str | None = None) -> Iterator[None]:
        """Wrap the enclosed write(s) in one **self-describing** DuckLake snapshot.

        Opens a transaction, stamps the snapshot it will produce with
        ``author='<writer>:<source>'``, a ``commit_message``, and a JSON
        ``commit_extra_info`` (canonical ``{writer, source, target, version,
        run_id, op}`` plus any per-producer :attr:`extra` keys, ADR-0011 §5)
        via ``set_commit_message``, runs the block, and commits. So the catalog
        itself attributes the snapshot — no ledger join, no table-id resolution
        (ADR-0007). One snapshot per block; the block rolls back on error.

        ``op`` is the sub-step label (e.g. ``"documents"``, ``"passages.shard2"``).
        Each block is its own transaction, so do **not** wrap an unbounded write in
        one block expecting it to stay small — keep the per-block write bounded
        (the PMC passages shards are sized for exactly this).

        **Re-entrant:** a nested ``attribute`` (e.g. PMC ``curate`` wrapping a
        ``_load`` that itself calls the now-self-attributing :func:`upsert`) joins
        the outer transaction — the outermost block owns the BEGIN/COMMIT and the
        commit message; inner blocks just run. So one snapshot, no nested BEGIN.
        """
        if self._txn_depth > 0:
            self._txn_depth += 1
            try:
                yield
            finally:
                self._txn_depth -= 1
            return

        canonical = {
            "writer": self.writer, "source": self.source, "target": self.target,
            "version": self.version, "run_id": self.run_id, "op": op,
        }
        # Per-producer keys (e.g. omicidx's prefect_run_id) merge in, but the
        # canonical keys are authoritative — a colliding extra key can't override.
        extra = json.dumps({**(self.extra or {}), **canonical})
        self.con.execute("BEGIN;")
        self.con.execute(
            f"CALL {LAKE}.set_commit_message(?, ?, extra_info => ?);",
            [f"{self.writer}:{self.source}", message or f"{self.source}: {op}", extra],
        )
        self._txn_depth = 1
        try:
            yield
        except BaseException:
            self.con.execute("ROLLBACK;")
            raise
        else:
            self.con.execute("COMMIT;")
        finally:
            self._txn_depth = 0


def _max_snapshot(con: duckdb.DuckDBPyConnection) -> int | None:
    """Current max DuckLake snapshot id, or None on an empty lake."""
    return con.execute(f"SELECT max(snapshot_id) FROM {LAKE}.snapshots()").fetchone()[0]


def _is_view(con: duckdb.DuckDBPyConnection, catalog: str, schema: str, name: str) -> bool:
    """True if ``name`` is a view, not a table -- ``duckdb_views()``/``duckdb_tables()``
    are disjoint catalogs, and ``COMMENT ON`` needs the right keyword for each
    (DuckDB rejects ``COMMENT ON TABLE`` for a view and vice versa)."""
    return (
        con.execute(
            "SELECT 1 FROM duckdb_views() "
            "WHERE database_name = ? AND schema_name = ? AND view_name = ?",
            [catalog, schema, name],
        ).fetchone()
        is not None
    )


def _escape(literal: str) -> str:
    # COMMENT ON doesn't accept a bound parameter for the literal (verified:
    # DuckDB's parser rejects `IS ?`) -- manually escape, same as csv_source().
    return literal.replace("'", "''")


def _ensure_table_comment(con: duckdb.DuckDBPyConnection, target: str, comment: str) -> None:
    """``COMMENT ON TABLE``/``COMMENT ON VIEW`` (auto-detected), only when it would
    actually change the stored comment.

    ``COMMENT ON`` writes a DuckLake snapshot **unconditionally**, even when the
    text is identical to what's already there (verified 2026-08-10) -- calling
    it on every run would silently break ADR-0003's "an unchanged re-run adds no
    snapshot" guarantee for every source, not just this one call site. Read the
    current comment first; write only on first-set or an actual change (e.g. the
    registered ``Source.description``/``license`` was edited).
    """
    catalog, schema, table = target.split(".", 2)
    view = _is_view(con, catalog, schema, table)
    entries = "duckdb_views()" if view else "duckdb_tables()"
    name_col = "view_name" if view else "table_name"
    current = con.execute(
        f"SELECT comment FROM {entries} "
        f"WHERE database_name = ? AND schema_name = ? AND {name_col} = ?",
        [catalog, schema, table],
    ).fetchone()
    if current is not None and current[0] == comment:
        return
    kind = "VIEW" if view else "TABLE"
    con.execute(f"COMMENT ON {kind} {target} IS '{_escape(comment)}';")


def ensure_column_comments(
    con: duckdb.DuckDBPyConnection, target: str, comments: dict[str, str]
) -> None:
    """``COMMENT ON COLUMN`` for each ``{column: comment}``, table-materialized targets only.

    DuckDB flatly rejects column comments on a view ("Cannot comment on columns
    for entry v - it is not a table", verified 2026-08-10) -- skip with a log
    line rather than crash a run whose model just happens to be a view with
    ``-- column:`` directives left over from before it became one.
    """
    if not comments:
        return
    catalog, schema, table = target.split(".", 2)
    if _is_view(con, catalog, schema, table):
        logger.warning(
            "ops: {} column comments declared but {} is a view -- DuckDB doesn't "
            "support COMMENT ON COLUMN for views, skipping", len(comments), target,
        )
        return
    current = dict(
        con.execute(
            "SELECT column_name, comment FROM duckdb_columns() "
            "WHERE database_name = ? AND schema_name = ? AND table_name = ?",
            [catalog, schema, table],
        ).fetchall()
    )
    for column, comment in comments.items():
        if current.get(column) == comment:
            continue
        con.execute(f"COMMENT ON COLUMN {target}.{column} IS '{_escape(comment)}';")


_SOURCES_BY_NAME: dict[str, Source] = {s.name: s for s in SOURCES}


def _self_register(con: duckdb.DuckDBPyConnection, source: str) -> None:
    """Lazily register a **built-in** ``source`` on first run, if not already present.

    Gives all cdsci ingestors correct attribution with no per-ingestor edits and
    without the substrate force-seeding on connect: a source name in the built-in
    :data:`SOURCES` self-registers (under its own ``writer``) the first time it
    runs. A foreign producer's source (not in ``SOURCES``) is left untouched — it
    registers itself explicitly via :func:`register_sources`. The write lands in the
    ``ops`` attachment (never a lake snapshot), like the run-row INSERT beside it.
    """
    src = _SOURCES_BY_NAME.get(source)
    if src is None:
        return
    exists = con.execute(
        f"SELECT 1 FROM {_t('source')} WHERE name = ? AND writer = ? LIMIT 1",
        [source, src.writer],
    ).fetchone()
    if exists is None:
        register_sources(con, writer=src.writer, sources=(src,))


def _writer_for(con: duckdb.DuckDBPyConnection, source: str) -> str:
    """The producer that registered ``source`` (ADR-0011 §5); the source name if unregistered.

    ``run`` derives ``writer`` from the registry rather than taking it as a param, so
    call sites stay stable. A source not yet registered (``bootstrap`` no longer
    seeds) falls back to its own name with a warning — attribution degrades to
    ``<source>:<source>`` but never crashes a load.
    """
    writers = [
        w for (w,) in con.execute(
            f"SELECT DISTINCT writer FROM {_t('source')} WHERE name = ?", [source]
        ).fetchall() if w
    ]
    if len(writers) == 1:
        return writers[0]
    if len(writers) > 1:
        raise ValueError(
            f"source {source!r} is registered under multiple writers "
            f"{sorted(writers)}; attribution is ambiguous — a producer sharing a "
            "source name must register/disambiguate explicitly"
        )
    logger.warning(
        "source {!r} not registered in lake_ops.source; defaulting writer to the "
        "source name (call ops.register_sources at your load entrypoint)", source,
    )
    return source


@contextmanager
def run(
    con: duckdb.DuckDBPyConnection,
    *,
    source: str,
    target: str,
    version: str | None = None,
    host: str | None = None,
    extra: dict | None = None,
) -> Iterator[Run]:
    """Record one ``lake_ops.run`` row around a curate/upsert.

    On enter: capture ``snapshot_before`` and insert a ``running`` row. Inside the
    block set ``r.rows`` to the upsert's row count. On exit: capture
    ``snapshot_after`` and finalize status — ``error`` if the block raised, else
    ``idempotent`` when no snapshot was added, else ``success``.

    ``writer`` is **derived** from the source registry (not a param, so existing
    call sites don't change); ``extra`` is an optional per-producer dict merged into
    the snapshot ``commit_extra_info`` on top of the canonical keys (ADR-0011 §5).

        with ops.run(con, source="icite", target=target, version=version) as r:
            r.rows = curate(con, paths, version, target=target, limit=limit)
        return r.summary()
    """
    rid = str(uuid.uuid4())
    host = host or socket.gethostname()
    _self_register(con, source)  # built-in sources self-register on first run
    writer = _writer_for(con, source)
    before = _max_snapshot(con)
    con.execute(
        f"INSERT INTO {_t('run')} "
        "(run_id, source, target, version, status, snapshot_before, started_at, host) "
        "VALUES (?, ?, ?, ?, 'running', ?, current_timestamp, ?)",
        [rid, source, target, version, before, host],
    )
    bound = logger.bind(ctx=f"run:{source}")
    bound.info(
        "start → {} (version={}, snapshot_before={}, run_id={})",
        target, version, before, rid,
    )
    r = Run(con, rid, source, target, version, before, writer=writer, extra=extra)
    token = _ACTIVE_RUN.set(r)
    try:
        try:
            yield r
        except Exception as exc:  # noqa: BLE001 — record then re-raise
            after = _max_snapshot(con)
            con.execute(
                f"UPDATE {_t('run')} SET status='error', snapshot_after=?, rows_after=?, "
                "finished_at=current_timestamp, error=? WHERE run_id=?",
                [after, r.rows, str(exc)[:2000], rid],
            )
            r.snapshot_after, r.status = after, "error"
            bound.error(
                "ERROR after {} rows (snapshot {}→{}, run_id={}): {}",
                r.rows, before, after, rid, exc,
            )
            raise
        else:
            after = _max_snapshot(con)
            status = "idempotent" if after == before else "success"
            con.execute(
                f"UPDATE {_t('run')} SET status=?, snapshot_after=?, rows_after=?, "
                "finished_at=current_timestamp WHERE run_id=?",
                [status, after, r.rows, rid],
            )
            r.snapshot_after, r.status = after, status
            src = con.execute(
                f"SELECT description, license FROM {_t('source')} "
                "WHERE name = ? AND writer = ?",
                [source, writer],
            ).fetchone()
            # Not every run's `target` is one table -- a multi-table source (pmc:
            # documents + passages under nested `attribute()` blocks) passes its
            # *schema* as the outer target, catalog.schema with no third part.
            # Nothing to comment on at that granularity; skip rather than guess
            # which of several tables the description/license would apply to.
            if src is not None and target.count(".") == 2:
                description, license_ = src
                _ensure_table_comment(con, target, f"{description} License: {license_}.")
            bound.success(
                "{} → {} (rows={}, snapshot {}→{}, run_id={})",
                status, target, r.rows, before, after, rid,
            )
    finally:
        _ACTIVE_RUN.reset(token)


def last_run(
    con: duckdb.DuckDBPyConnection, source: str, *, status: str | None = None
) -> dict | None:
    """The most recent run for ``source`` (optionally filtered to a ``status``)."""
    where = "source = ?"
    params: list[Any] = [source]
    if status is not None:
        where += " AND status = ?"
        params.append(status)
    row = con.execute(
        f"SELECT run_id, source, target, version, status, snapshot_before, "
        f"snapshot_after, rows_after, started_at::VARCHAR, finished_at::VARCHAR, error "
        f"FROM {_t('run')} WHERE {where} ORDER BY started_at DESC LIMIT 1",
        params,
    ).fetchone()
    if row is None:
        return None
    cols = ("run_id", "source", "target", "version", "status", "snapshot_before",
            "snapshot_after", "rows_after", "started_at", "finished_at", "error")
    return dict(zip(cols, row, strict=True))


_RUN_COLS = (
    "run_id", "source", "target", "version", "status", "snapshot_before",
    "snapshot_after", "rows_after", "started_at", "finished_at", "error", "host",
)
_RUN_SELECT = (
    "SELECT run_id, source, target, version, status, snapshot_before, "
    "snapshot_after, rows_after, started_at::VARCHAR, finished_at::VARCHAR, error, host"
)


def list_runs(con: duckdb.DuckDBPyConnection, *, limit: int = 50) -> list[dict]:
    """The most recent runs across all sources/producers (newest first).

    The read surface an ops dashboard queries instead of touching the ledger
    tables directly (a read-only consumer attaches via
    ``lake_connect(..., read_only=True, with_ops=True)``).
    """
    rows = con.execute(
        f"{_RUN_SELECT} FROM {_t('run')} ORDER BY started_at DESC LIMIT ?", [limit]
    ).fetchall()
    return [dict(zip(_RUN_COLS, r, strict=True)) for r in rows]


def get_run(con: duckdb.DuckDBPyConnection, run_id: str) -> dict | None:
    """One run row by ``run_id``, or None if unknown."""
    row = con.execute(
        f"{_RUN_SELECT} FROM {_t('run')} WHERE run_id = ?", [run_id]
    ).fetchone()
    return dict(zip(_RUN_COLS, row, strict=True)) if row else None


def list_sources(con: duckdb.DuckDBPyConnection) -> list[dict]:
    """The registered sources (name, schema, description, cadence, writer)."""
    rows = con.execute(
        f"SELECT name, lake_schema, description, cadence, writer "
        f"FROM {_t('source')} ORDER BY name"
    ).fetchall()
    cols = ("name", "lake_schema", "description", "cadence", "writer")
    return [dict(zip(cols, r, strict=True)) for r in rows]


# --- Watermarks (incremental cursors; in-place) ---


def get_watermark(con: duckdb.DuckDBPyConnection, source: str, name: str) -> Any | None:
    """The cursor value for ``(source, name)``, JSON-decoded, or None if unset."""
    row = con.execute(
        f"SELECT value FROM {_t('watermark')} WHERE source = ? AND name = ?",
        [source, name],
    ).fetchone()
    return json.loads(row[0]) if row else None


def set_watermark(
    con: duckdb.DuckDBPyConnection,
    source: str,
    name: str,
    value: Any,
    *,
    run_id: str | None = None,
) -> None:
    """Set the ``(source, name)`` cursor to ``value`` (JSON-encoded; in-place)."""
    payload = json.dumps(value)
    con.execute(
        f"DELETE FROM {_t('watermark')} WHERE source = ? AND name = ?", [source, name]
    )
    con.execute(
        f"INSERT INTO {_t('watermark')} (source, name, value, updated_at, set_by_run) "
        "VALUES (?, ?, ?, current_timestamp, ?)",
        [source, name, payload, run_id],
    )
