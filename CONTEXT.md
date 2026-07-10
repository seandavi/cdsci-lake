# cdsci-lake

The shared research-data lake platform: the DuckLake **substrate** (one catalog +
write library + snapshot contract) that many independent producers write into and
many consumers read from. This glossary pins the terms the platform's contract
depends on; decisions live in `docs/adr/`.

## Roles

**Producer**:
A codebase that writes curated tables into the lake (this repo's ingestors,
omicidx, later scp/cmgd). Each owns its schema(s), extract, and transform, and
writes through the shared contract (ADR-0011).
_Also_: Publisher (ADR-0001's term for the same thing).

**Consumer**:
A codebase that only reads the lake (`read_only=True`) — a dashboard, API, or
project. Never mutates the substrate; assumes the data exists.

**Writer**:
The producer identity stamped on every write — the `writer` field of a registered
`Source` and the `author = "<writer>:<source>"` prefix on the snapshot it produces
(`cdsci`, `omicidx`). Distinguishes producers in the one shared ledger and catalog.

**Substrate**:
The shared, producer-agnostic core: `lake_connect`, the write verbs, and the `ops`
ledger. Has no dependency on any producer's ingestors; the dependency arrow points
*into* it.

## Write vocabulary

**Source**:
A registered upstream dataset (`icite`, `reporter`, `sra`, `geo`) that owns a lake
schema and refreshes on a cadence. Declared in producer code, materialised into
`lake_ops.source` via `register_sources`.

**upsert**:
The default write: a keyed MERGE that INSERTs new rows and UPDATEs only where a
non-key column actually differs (`IS DISTINCT FROM`), so an unchanged re-run writes
nothing and adds no snapshot (ADR-0003). No content-hash column — the compare set
is derived from the source projection.
_Avoid_: merge, load (when you mean this specific keyed MERGE).

**rebuild**:
A full-replace (CREATE OR REPLACE) write for recomputed derived tables. **Not part
of the EL write path** — derived tables are transform-layer artifacts and defer
with the transform layer (ADR-0013). Named here only so the term is fixed for when
that layer lands; today the EL contract has one verb, `upsert`.
_Avoid_: replace, truncate-and-load, refresh.

## Operational ledger (`lake_ops`)

**Ledger**:
Catalog-adjacent native state (the `ops` attachment: Postgres in production, a
sibling `ops.duckdb` locally) — **not** DuckLake data. Answers the questions
snapshots can't: when/whether/where a run happened (ADR-0006).
_Avoid_: metadata store (too broad), event log.

**Run**:
One recorded ingest invocation — a `lake_ops.run` row with status
(`running`/`success`/`idempotent`/`error`), the before/after snapshot ids, row
count, and version. Bracketed by the `ops.run(...)` context manager.

**Watermark**:
A mutable incremental **resume cursor**, keyed `(source, name)`, one value per
source (e.g. OpenAlex `updated_date`, SRA's high-water partition). In-place UPDATE
in the ledger.
_Not_: crawl state — a producer's per-partition "did I extract this key yet"
done-set (omicidx's raw-extract semaphore files) is thousands of keys, a different
lifecycle, and stays producer-local; the watermark is a single cursor (ADR-0011 §3).

**Snapshot attribution**:
The self-describing commit metadata a write stamps on the DuckLake snapshot it
produces — `author = "<writer>:<source>"` plus a canonical `commit_extra_info` JSON
(`writer, source, target, version, run_id, op`, + optional producer extras). Lets
one query attribute any snapshot without a ledger join (ADR-0008, ADR-0011 §5).

## Data layers

**Bronze**:
Raw upstream downloads (on R2/local), kept verbatim and unregistered, so a
re-curate needs no re-download (ADR-0012).

**Silver**:
The per-source curated, typed tables exposed via versioned views — the consumer
contract (ADR-0001 §3).

**Gold**:
Cohorts, entity resolution, cross-source analytics. Stays in **consumer** projects,
never in a source schema or the lake.
