# 0009. Structured logging with loguru across the substrate and ingestors

- Status: accepted
- Date: 2026-06-24

## Context

Ingestors were observability-thin: a `print()` here, `typer.echo()` for the final
summary, nothing in between. A multi-hour bulk load (PMC) that failed left a bare
traceback with no trail of which range/step it reached, and there was no consistent,
leveled, timestamped output. We need one logging surface that is **on for the
ingest CLIs** but **silent for the read-client library** — a consumer who
`pip install cdsci-lake` and calls `lake_connect(read_only=True)` should get no log
noise unless they ask for it.

## Decision

**loguru, behind a shared `cdsci.lake.log` module.**

- `configure(level)` installs a single stderr sink (level from `$CDSCI_LOG_LEVEL`,
  default `INFO`) with a timestamp + level + `ctx` + message format. `logger` is the
  shared instance. `loguru` is a base dependency.
- **Library code never configures at import.** Only the CLIs call `configure()` —
  each source's `__main__.py` has a typer `@app.callback()` with a `--log-level`
  option that calls it. So importing the package as a read client stays quiet; an
  ingest invocation is fully logged.
- Modules bind a context: `_log = logger.bind(ctx="<source>")`, so every line is
  tagged with where it came from (`pmc`, `download`, `run:icite`, …).
- **Three layers of coverage, mostly shared so sources don't each reinvent it:**
  1. `ops.run` logs the run lifecycle — start / success / idempotent / error with
     rows and snapshot deltas — for **every** source automatically.
  2. The shared `download.py` logs fetch start/done (+ size) and zip extraction, so
     every source that downloads gets fetch logging for free.
  3. Per-source ingest adds milestone lines (per-table curate counts, per-batch /
     per-database / per-domain loop progress). PMC logs per-range stream/curate.
- Log calls use loguru brace formatting (`_log.info("x {} y {}", a, b)`), never
  f-strings, so message rendering stays lazy and structured. `print()` in the
  OpenAlex ingest was replaced with `_log.info`.

## Consequences

- Every `python -m cdsci.lake.sources.<source> …` emits a consistent, leveled,
  timestamped trail; a failure is preceded by the step it reached, and
  `--log-level DEBUG` turns up detail (e.g. download resume/range restarts).
- The read client stays silent by default — no sink is installed until a CLI (or a
  caller) invokes `configure()`.
- New sources inherit run + fetch logging by using `ops.run` and `download.py`; the
  only per-source work is binding `ctx` and a few milestone lines.
- Conventions to keep: `configure()` belongs in the CLI callback only; bind `ctx`
  per module; brace-format log args; reserve `INFO` for milestones and `DEBUG` for
  per-item/loop detail so a normal run stays readable.
