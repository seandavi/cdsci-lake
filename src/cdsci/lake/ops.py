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
import time
import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import duckdb

from .connect import LAKE
from .contracts import check_asset_ref
from .log import event, logger

if TYPE_CHECKING:
    from .publish.release import PublicationReceipt

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
    Source("ror", "ror", "ROR: canonical institution identity (names, locations, relationships)",
           "on-release", "zenodo", "cc0"),
    # Not the annual bulk file: demand-driven fetch of caller-supplied iDs from
    # the public API (issue #56 -- see sources/orcid/ingest.py for the numbers).
    Source("orcid", "orcid", "ORCID: canonical researcher identity (names + affiliations), "
           "scoped to requested iDs", "on-demand", "orcid-public-api", "cc0"),
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
    # ADR-0014 §5 / docs/design/metadata_lineage.md: the asset + lineage skeleton.
    # No SERIAL/PK/FK (ADR-0006 portability note) -- uniqueness enforced in code.
    con.execute(
        f"""CREATE TABLE IF NOT EXISTS {_t("asset")} (
            ref TEXT, writer TEXT, asset_type TEXT, name TEXT,
            first_seen TIMESTAMPTZ, last_run_id TEXT, current_version TEXT
        );"""
    )
    con.execute(
        f"""CREATE TABLE IF NOT EXISTS {_t("lineage")} (
            src_ref TEXT, dst_ref TEXT, edge_type TEXT, run_id TEXT,
            discovered_at TIMESTAMPTZ
        );"""
    )
    # ADR-0014 Amendment 2026-09-22 / cdsci-lake#100: lands `PublicationReceipt.to_json()`
    # keyed by (release_id, dataset_id, asset_ref), attributed to run_id.
    con.execute(
        f"""CREATE TABLE IF NOT EXISTS {_t("publication_receipt")} (
            receipt_id TEXT, release_id TEXT, dataset_id TEXT, asset_ref TEXT,
            spec_version TEXT, status TEXT, receipt TEXT, run_id TEXT,
            recorded_at TIMESTAMPTZ
        );"""
    )
    # ADR-0008 Amendment 2026-09-22 / cdsci-lake#89: SQLMesh writes DuckLake
    # directly (no `run()`/`attribute()` wrapper), so its snapshots carry no
    # `commit_extra_info` and ADR-0008's in-catalog guarantee doesn't reach them.
    # This side table restores attribution from `lake_ops` instead of the commit
    # metadata -- see `sync_sqlmesh_snapshot_attribution`.
    con.execute(
        f"""CREATE TABLE IF NOT EXISTS {_t("snapshot_attribution")} (
            snapshot_id BIGINT, run_id TEXT, source TEXT
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
    event("run_started", run_id=rid, writer=writer, job=source, status="running")
    t0 = time.monotonic()
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
            event(
                "run_error", run_id=rid, writer=writer, job=source, rows=r.rows,
                status="error", duration_ms=(time.monotonic() - t0) * 1000,
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
            event(
                "run_succeeded" if status == "success" else "run_idempotent",
                run_id=rid, writer=writer, job=source, rows=r.rows,
                status=status, duration_ms=(time.monotonic() - t0) * 1000,
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


# --- Assets + lineage (ADR-0014 §5; docs/design/metadata_lineage.md) ---


def register_asset(
    con: duckdb.DuckDBPyConnection,
    *,
    ref: str,
    writer: str,
    asset_type: str,
    name: str,
    current_version: str | None = None,
) -> None:
    """Register/refresh an asset (delete-then-insert on ``(writer, ref)``; ADR-0006 style).

    ``first_seen`` is preserved across re-registration; ``last_run_id`` is taken from
    the active :func:`run`, if any (ADR-0008's attribution convention, applied to the
    ledger rather than a DuckLake snapshot).
    """
    check_asset_ref(ref)
    active = active_run()
    run_id = active.run_id if active else None
    existing = con.execute(
        f"SELECT first_seen FROM {_t('asset')} WHERE writer = ? AND ref = ?", [writer, ref]
    ).fetchone()
    first_seen = existing[0] if existing else None
    con.execute(f"DELETE FROM {_t('asset')} WHERE writer = ? AND ref = ?", [writer, ref])
    con.execute(
        f"INSERT INTO {_t('asset')} "
        "(ref, writer, asset_type, name, first_seen, last_run_id, current_version) "
        "VALUES (?, ?, ?, ?, COALESCE(?, current_timestamp), ?, ?)",
        [ref, writer, asset_type, name, first_seen, run_id, current_version],
    )


def record_lineage(
    con: duckdb.DuckDBPyConnection, *, src_ref: str, dst_ref: str, edge_type: str
) -> None:
    """Record a lineage edge (``dst_ref`` built from ``src_ref``); idempotent.

    Uniqueness is ``(src_ref, dst_ref)`` (metadata_lineage.md) -- a second call for
    the same pair is a no-op, it does not update ``edge_type``/``run_id``.
    """
    check_asset_ref(src_ref)
    check_asset_ref(dst_ref)
    exists = con.execute(
        f"SELECT 1 FROM {_t('lineage')} WHERE src_ref = ? AND dst_ref = ?", [src_ref, dst_ref]
    ).fetchone()
    if exists is not None:
        return
    active = active_run()
    run_id = active.run_id if active else None
    con.execute(
        f"INSERT INTO {_t('lineage')} (src_ref, dst_ref, edge_type, run_id, discovered_at) "
        "VALUES (?, ?, ?, ?, current_timestamp)",
        [src_ref, dst_ref, edge_type, run_id],
    )


_ASSET_COLS = ("ref", "writer", "asset_type", "name", "first_seen", "last_run_id",
               "current_version")
_ASSET_SELECT = (
    "SELECT ref, writer, asset_type, name, first_seen::VARCHAR, last_run_id, current_version"
)


def list_assets(con: duckdb.DuckDBPyConnection, *, writer: str | None = None) -> list[dict]:
    """Registered assets, optionally filtered to one ``writer``."""
    where = ""
    params: list[Any] = []
    if writer is not None:
        where = "WHERE writer = ?"
        params.append(writer)
    rows = con.execute(
        f"{_ASSET_SELECT} FROM {_t('asset')} {where} ORDER BY ref", params
    ).fetchall()
    return [dict(zip(_ASSET_COLS, r, strict=True)) for r in rows]


_LINEAGE_COLS = ("src_ref", "dst_ref", "edge_type", "run_id", "discovered_at")
_LINEAGE_SELECT = "SELECT src_ref, dst_ref, edge_type, run_id, discovered_at::VARCHAR"


def lineage_for(
    con: duckdb.DuckDBPyConnection, ref: str, *, direction: str = "upstream"
) -> list[dict]:
    """Lineage edges touching ``ref``.

    ``direction='upstream'`` (default): edges where ``ref`` is ``dst_ref`` -- what it
    was built from. ``direction='downstream'``: edges where ``ref`` is ``src_ref`` --
    what it feeds.
    """
    if direction == "upstream":
        where = "dst_ref = ?"
    elif direction == "downstream":
        where = "src_ref = ?"
    else:
        raise ValueError(f"direction must be 'upstream' or 'downstream', got {direction!r}")
    rows = con.execute(f"{_LINEAGE_SELECT} FROM {_t('lineage')} WHERE {where}", [ref]).fetchall()
    return [dict(zip(_LINEAGE_COLS, r, strict=True)) for r in rows]


# --- SQLMesh snapshot attribution (ADR-0008 Amendment 2026-09-22; cdsci-lake#89) ---


def _changed_tables(con: duckdb.DuckDBPyConnection, changes: dict | None) -> set[str]:
    """The ``schema.table`` names touched by a snapshot's ``changes`` map.

    ``changes`` (from ``lake.snapshots()``) is a map of change-kind ->
    list-of-names/ids, e.g. ``{"tables_created": ["bugsigdb.signature"],
    "tables_dropped": ["2"], "inlined_insert": ["4"]}``. ``tables_created``/
    ``tables_dropped``/``tables_altered`` carry the dotted ``schema.table`` name
    directly; a data-only change -- ``inlined_insert``/``inlined_delete``/
    ``compacted``, the shape a SQLMesh INCREMENTAL/SCD2 apply produces since it
    never recreates the table -- carries only the internal table id. Those ids
    are resolved here against the catalog's own table/schema metadata (the
    DuckLake-internal ``__ducklake_metadata_<alias>`` attachment, which DuckLake
    itself keeps mounted alongside ``lake``) so a data-only change is matchable
    too, not just a full-refresh replace. That metadata table includes dropped
    tables (``end_snapshot`` set, row not removed), so a ``tables_dropped`` id
    still resolves.
    """
    names: set[str] = set()
    ids: set[int] = set()
    for values in (changes or {}).values():
        if not isinstance(values, list):
            continue
        for v in values:
            if not isinstance(v, str):
                continue
            if "." in v:
                names.add(v)
            elif v.isdigit():
                ids.add(int(v))
    if ids:
        meta = f"__ducklake_metadata_{LAKE}"
        rows = con.execute(
            f"SELECT s.schema_name, t.table_name FROM {meta}.ducklake_table t "
            f"JOIN {meta}.ducklake_schema s USING (schema_id) "
            f"WHERE t.table_id IN ({','.join('?' * len(ids))})",
            list(ids),
        ).fetchall()
        names.update(f"{schema}.{table}" for schema, table in rows)
    return names


def sync_sqlmesh_snapshot_attribution(
    con: duckdb.DuckDBPyConnection,
    *,
    project: str,
    model: str,
    target: str,
    version: str | None = None,
) -> str | None:
    """Attribute ``model``'s most recent SQLMesh apply (ADR-0008 Amendment; #89).

    SQLMesh writes DuckLake directly -- no :func:`run`/:meth:`Run.attribute`
    wrapper -- so its snapshots carry no ``commit_extra_info`` and ADR-0008's
    in-catalog guarantee never reaches them. This restores attribution from the
    other side: a per-model watermark (under ``source=f"sqlmesh:{project}"``)
    tracks the last DuckLake snapshot id this model was synced through; every
    call brackets ``(watermark, current_max]``, excludes any snapshot that
    already carries its own ``commit_extra_info`` (already attributed in-catalog
    -- ADR-0008 §1's guarantee takes priority over this side table), and keeps
    only the snapshots in that range whose ``changes`` actually touch
    ``target``'s ``schema.table`` -- a concurrent write to a *different* model in
    the same window is never attributed here. One ``lake_ops.run`` row (its
    ``started_at``/``finished_at`` the min/max ``snapshot_time`` of the matched
    snapshots, not the sync's own wall-clock time) and one
    ``lake_ops.snapshot_attribution`` row per matched snapshot are written
    (delete-then-insert per snapshot id, so a watermark reset re-syncing the same
    snapshot replaces rather than duplicates its row). The watermark is written
    **last**, after both inserts (also on the no-match path) -- so a crash
    between the run insert and the watermark write leaves the watermark exactly
    where it was and a retry re-scans and recovers the same range, instead of
    silently losing attribution on the next call.

    ``project`` must be ``"cdsci_lake"`` -- this is cdsci-lake's own sync path and
    must never attribute a model injected from another producer's SQLMesh
    project (e.g. omicidx).

    Returns the new run's ``run_id``, or ``None`` if there was nothing to sync.
    """
    if project != "cdsci_lake":
        raise ValueError(
            f"sync_sqlmesh_snapshot_attribution is cdsci_lake-only, got project={project!r} "
            "-- never sync a model belonging to another producer's SQLMesh project"
        )
    watermark_source = f"sqlmesh:{project}"
    since = get_watermark(con, watermark_source, model) or 0
    upto = _max_snapshot(con)
    if upto is None or upto <= since:
        return None

    _, schema, table = target.split(".", 2)
    qualified = f"{schema}.{table}"
    rows = con.execute(
        f"SELECT snapshot_id, snapshot_time, changes FROM {LAKE}.snapshots() "
        "WHERE snapshot_id > ? AND snapshot_id <= ? AND commit_extra_info IS NULL "
        "ORDER BY snapshot_id",
        [since, upto],
    ).fetchall()
    matched = [
        (sid, stime) for sid, stime, changes in rows
        if qualified in _changed_tables(con, changes)
    ]
    if not matched:
        set_watermark(con, watermark_source, model, upto)
        return None

    matched_ids = [sid for sid, _ in matched]
    started_at = min(stime for _, stime in matched)
    finished_at = max(stime for _, stime in matched)

    run_id = str(uuid.uuid4())
    con.execute(
        f"INSERT INTO {_t('run')} "
        "(run_id, source, target, version, status, snapshot_before, snapshot_after, "
        " started_at, finished_at, host) "
        "VALUES (?, ?, ?, ?, 'success', ?, ?, ?, ?, ?)",
        [run_id, model, target, version, since, matched_ids[-1], started_at, finished_at,
         "sqlmesh-sync"],
    )
    con.executemany(
        f"DELETE FROM {_t('snapshot_attribution')} WHERE snapshot_id = ?",
        [(sid,) for sid in matched_ids],
    )
    con.executemany(
        f"INSERT INTO {_t('snapshot_attribution')} (snapshot_id, run_id, source) "
        "VALUES (?, ?, 'sqlmesh_sync')",
        [(sid, run_id) for sid in matched_ids],
    )
    set_watermark(con, watermark_source, model, upto, run_id=run_id)
    return run_id


def snapshot_run_ids(con: duckdb.DuckDBPyConnection, snapshot_ids: list[int]) -> dict[int, str]:
    """``{snapshot_id: run_id}`` from the side table, for the ids given.

    The dashboard's fallback when a snapshot's own ``commit_extra_info`` carries
    no ``run_id`` (a SQLMesh-written snapshot, attributed by
    :func:`sync_sqlmesh_snapshot_attribution` rather than by the commit itself).
    """
    if not snapshot_ids:
        return {}
    rows = con.execute(
        f"SELECT snapshot_id, run_id FROM {_t('snapshot_attribution')} "
        f"WHERE snapshot_id IN ({','.join('?' * len(snapshot_ids))})",
        snapshot_ids,
    ).fetchall()
    return dict(rows)


# --- Publication receipts (ADR-0014 Amendment 2026-09-22; cdsci-lake#100) ---


def record_publication_receipt(con: duckdb.DuckDBPyConnection, receipt: PublicationReceipt) -> str:
    """Land ``receipt`` under ``(release_id, dataset_id, asset_ref)`` (delete-then-insert).

    ``asset_ref`` is derived, not a separate caller-supplied field -- the receipt's
    own ``dataset``/``release`` name the release-as-a-whole asset
    (``release.<dataset>.<release>``, the same ref
    :func:`cdsci.lake.publish.builder.record_release` registers via
    :func:`register_asset`), so re-running the same release's build replaces its one
    receipt rather than accumulating duplicates. Returns the generated ``receipt_id``.
    """
    release_id, dataset_id = receipt.release, receipt.dataset
    asset_ref = f"release.{dataset_id}.{release_id}"
    receipt_id = str(uuid.uuid4())
    # Fall back to the enclosing run() block's run_id when the receipt itself doesn't
    # carry one, so this row and register_asset()'s last_run_id (also active_run()-
    # derived) attribute to the same run instead of silently diverging.
    active = active_run()
    run_id = receipt.run_id or (active.run_id if active else None)
    con.execute(
        f"DELETE FROM {_t('publication_receipt')} "
        "WHERE release_id = ? AND dataset_id = ? AND asset_ref = ?",
        [release_id, dataset_id, asset_ref],
    )
    con.execute(
        f"INSERT INTO {_t('publication_receipt')} "
        "(receipt_id, release_id, dataset_id, asset_ref, spec_version, status, receipt, "
        " run_id, recorded_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, current_timestamp)",
        [receipt_id, release_id, dataset_id, asset_ref, receipt.spec_version,
         receipt.status.value, receipt.to_json(), run_id],
    )
    return receipt_id


def publication_receipts(
    con: duckdb.DuckDBPyConnection, release_id: str
) -> list[PublicationReceipt]:
    """Receipts for ``release_id``, oldest first, decoded back to ``PublicationReceipt``."""
    from .publish.release import PublicationReceipt  # lazy: publish is a downstream import

    rows = con.execute(
        f"SELECT receipt FROM {_t('publication_receipt')} "
        "WHERE release_id = ? ORDER BY recorded_at",
        [release_id],
    ).fetchall()
    return [PublicationReceipt.from_json(r[0]) for r in rows]
