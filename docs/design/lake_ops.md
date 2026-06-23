# `lake_ops` — the operational ledger

The operational metadata model behind ADR-0001 §6, decided in **ADR-0006**. This
doc is the concrete shape: where it attaches, the table DDL, and how an ingestor
wires into it.

`lake_ops` answers the questions DuckLake snapshots can't: *when did we last load
a source, did the run change/error, which snapshot did it produce, where do
incrementals resume, and what shape may consumers depend on.*

## Where it lives — a second attachment, native (not DuckLake) tables

`lake_ops` is **catalog-adjacent native state**, parallel to the catalog backend
(ADR-0006 §1). `lake_connect()` makes a second attachment `ops` on the **write
path only**:

| backend | catalog | `lake_ops` storage | attach |
|---------|---------|--------------------|--------|
| postgres (prod) | Postgres `lake` DB | `lake_ops` schema in the same DB | `ATTACH '…postgres…' AS ops` (postgres ext) |
| local (dev) | `<dir>/catalog.ducklake` | sibling `<dir>/ops.duckdb` | `ATTACH '<dir>/ops.duckdb' AS ops` |

Read-only consumers (`lake_connect(read_only=True)`) do **not** attach `ops` —
operational state is a writer concern. On first write-mode connect, `lake_connect`
runs the idempotent bootstrap (schema + tables `IF NOT EXISTS`, then upserts the
`source` registry from code).

Tables are addressed `ops.lake_ops.<table>` (local: `ops.main.<table>` or a
`lake_ops` schema in the sibling file — see `cdsci.lake.ops` for the resolved
prefix). **Implementation note (as built).** `ops` may be a real Postgres database reached
through DuckDB's `postgres` extension, whose DDL surface is narrow. So the tables
as implemented in `cdsci.lake.ops` carry **no** `SERIAL`/`DEFAULT`/`PRIMARY
KEY`/foreign-key constraints — one DDL works on both backends. Concretely:
`run_id` is a **client-generated UUID** (`VARCHAR`, `uuid.uuid4()`) — race-free
under the concurrent loads, no sequence; the watermark `value` is **JSON text**
(`VARCHAR`, `json.dumps`/`loads`); timestamps are written explicitly with
`current_timestamp` (no column `DEFAULT`); and uniqueness is enforced in code (the
registry refresh and `set_watermark` are delete-then-insert). The DDL below shows
the *intent* with Postgres types; read `BIGSERIAL`→client UUID, `jsonb`→JSON text,
and the keys/defaults as code-enforced.

## Tables

### `lake_ops.source` — the registry

One row per source. The authoritative registry is a code-level declaration
(a frozen dataclass list, like reporter's `Group`/scp's `Domain`); this table is
its materialization so the ledger and observability can join on it.

```sql
CREATE TABLE lake_ops.source (
    name             TEXT PRIMARY KEY,        -- 'reporter', 'icite', 'openalex', …
    lake_schema      TEXT NOT NULL,           -- 'reporter', 'icite', 'ref', …
    description      TEXT,
    cadence          TEXT,                    -- 'monthly', 'weekday-daily', 'static', …
    distribution     TEXT,                    -- 'figshare', 's3-snapshot', 'github-release', …
    license          TEXT,
    watermark_strategy TEXT,                  -- 'updated_date' | 'page_token' | 'full' | NULL
    registered_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

### `lake_ops.run` — the append-only ledger

One row per `ingest()` invocation, per target table (multi-table ingestors —
scp, ctgov, openalex — write one run row per table they touch, sharing nothing
but timing). Subsumes the `snap_before`/`snap_after` bracketing every ingestor
hand-rolls today. The per-source "versions" log is just
`SELECT DISTINCT version FROM run WHERE source = ?`.

```sql
CREATE TABLE lake_ops.run (
    run_id           BIGSERIAL PRIMARY KEY,
    source           TEXT NOT NULL REFERENCES lake_ops.source(name),
    target           TEXT NOT NULL,           -- fully-qualified 'lake.icite.metadata'
    version          TEXT,                    -- the load tag / snapshot_version label
    status           TEXT NOT NULL,           -- 'running'|'success'|'idempotent'|'error'
    snapshot_before  BIGINT,                  -- max(snapshot_id) on enter
    snapshot_after   BIGINT,                  -- max(snapshot_id) on exit
    rows_after       BIGINT,                  -- upsert() return (target rowcount)
    started_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at      TIMESTAMPTZ,
    error            TEXT,                    -- traceback summary when status='error'
    host             TEXT                     -- who ran it (operator/CI/cron)
);
CREATE INDEX run_source_started ON lake_ops.run (source, started_at DESC);
```

- `status='idempotent'` ⟺ `snapshot_after = snapshot_before` (the upsert was a
  no-op). Recording it is deliberate: "we checked at T, nothing new" is a useful
  fact for a daily-sync source.
- `status='error'` rows keep `snapshot_after`/`finished_at` and an `error` blurb;
  the next run reads the last **success** for its watermark, not the last attempt.

### `lake_ops.watermark` — incremental cursors

Mutable, in-place. The blocker this whole model removes.

```sql
CREATE TABLE lake_ops.watermark (
    source       TEXT NOT NULL REFERENCES lake_ops.source(name),
    name         TEXT NOT NULL,               -- 'updated_date', 'page_token', 'max_range'
    value        JSONB NOT NULL,              -- '"2026-05-01"', '"NF1..."', '{"range":42}'
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    set_by_run   BIGINT REFERENCES lake_ops.run(run_id),
    PRIMARY KEY (source, name)
);
```

Cursor semantics are the source's: OpenAlex `updated_date` (advance to the max
`updated_date` ingested), ctgov `page_token` (the API's `nextPageToken`), PMC
`max_range` (highest tarball range curated). `value` is `jsonb` so a cursor can be
a scalar or a small struct without a schema change.

### `lake_ops.dataset_contract` — the consumer contract (deferred)

Reserved here so the model is whole; **lands with the versioned-views
workstream**, not this ADR. The intent (ADR-0001 §3): per-source stable views
(`icite.v_rcr`, …) are what consumers bind to, and this table declares each view's
committed shape so a raw column rename can't silently break downstream.

```sql
CREATE TABLE lake_ops.dataset_contract (
    lake_schema      TEXT NOT NULL,
    view_name        TEXT NOT NULL,           -- 'v_rcr'
    contract_version INTEGER NOT NULL,        -- bump on a breaking change
    columns          JSONB NOT NULL,          -- [{name,type}, …] the committed shape
    backing_table    TEXT NOT NULL,           -- 'lake.icite.metadata'
    status           TEXT NOT NULL,           -- 'active'|'deprecated'
    published_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (lake_schema, view_name, contract_version)
);
```

Ideally **generated**, not hand-maintained — DuckLake stats give row counts and
schemas cheaply (cf. the `SCHEMA.md`/`CATALOG.md` idea in the roadmap).

## The ingestor API — `cdsci.lake.ops`

```python
from cdsci.lake import ops
```

### `ops.run(...)` — the run context manager

Replaces the manual snapshot bracketing. On enter: capture `snapshot_before`,
insert a `running` row, return a mutable handle. On exit: capture
`snapshot_after`, set `rows_after` from the handle, set status
(`error` if the block raised, else `idempotent`/`success` by snapshot delta),
finalize the row.

```python
def run(con, *, source: str, target: str, version: str | None = None):
    """Context manager. Yields a Run handle; records one lake_ops.run row."""
```

Before/after — iCite's `ingest()`:

```python
# before
snap_before = con.execute(f"SELECT max(snapshot_id) FROM {LAKE}.snapshots()").fetchone()[0]
rows = curate(con, paths, version, target=target, limit=limit)
snap_after  = con.execute(f"SELECT max(snapshot_id) FROM {LAKE}.snapshots()").fetchone()[0]
return {"table": target, "version": version, "rows": rows, "changed": snap_after != snap_before}

# after
with ops.run(con, source="icite", target=target, version=version) as r:
    r.rows = curate(con, paths, version, target=target, limit=limit)
return r.summary()   # {table, version, rows, changed, snapshot, run_id, status}
```

### Watermark accessors

```python
def get_watermark(con, source: str, name: str) -> Any | None: ...      # None on first run
def set_watermark(con, source: str, name: str, value: Any, *, run_id: int) -> None: ...
```

Incremental shape — OpenAlex:

```python
since = ops.get_watermark(con, "openalex", "updated_date")        # None → full pull
with ops.run(con, source="openalex", target=target) as r:
    high = curate_works(con, target, updated_since=since)          # returns max updated_date seen
    r.rows = ...
    if high:
        ops.set_watermark(con, "openalex", "updated_date", high, run_id=r.run_id)
```

## Rollout

1. `cdsci.lake.ops` + the bootstrap (attach `ops`, create schema/tables, seed
   `source`) wired into `lake_connect()` write mode. Tests against the local
   sibling-file backend (offline, like the existing suite).
2. Convert the seven ingestors to `ops.run(...)`; drop the hand-rolled
   before/after. Behaviour-preserving — `summary()` returns the same keys.
3. Add watermark use where it pays first: **OpenAlex** (`updated_date`) and
   whatever daily-sync source lands first (Retraction Watch — roadmap). ctgov/PMC
   can adopt cursors opportunistically; full re-read stays correct meanwhile.
4. `dataset_contract` + generation deferred to the versioned-views workstream.
