# 0005. OpenAlex ingest: prune by field, filter by domain, split references

- Status: accepted
- Date: 2026-06-23

## Context

OpenAlex (CC0) is the one source that gives us the **ROR-keyed institution graph**
plus topics, funders, and sources — the missing primitive for peer-center
benchmarking (institution ↔ grants ↔ pubs, ADR-0021 in the consumer repo). It is
also the largest source on the platform by far. The public snapshot, measured from
its manifest (2026-06):

- **works**: 639.2 GB gzipped, **492,361,307** records across **2,127** part files;
- partitioned by `updated_date`, updated monthly, readable **anonymously** over the
  https endpoint (`https://openalex.s3.amazonaws.com`) — no AWS account, no creds.

Loading works verbatim is neither necessary nor desirable. We measured per-field
byte share across a biomedical sample; the cost is concentrated and much of it is
redundant with what we already hold or is OpenAlex-deprecated:

| field | ~% of a work | disposition |
|-------|-------------:|-------------|
| `authorships` | 27% | keep (institution ROR is the benchmarking key) |
| `mesh` | 20% | **drop** — redundant with omicidx PubMed (MeSH only exists for PubMed-indexed works), recover via PMID |
| `locations` (array) | 8% | **drop** — keep `primary_location` + `best_oa_location` subsets |
| `abstract_inverted_index` | 6.5% | **transform** → plaintext (smaller, FTS-ready) |
| `concepts` | 6% | **drop** — OpenAlex-deprecated, superseded by `topics` |
| `referenced_works` | 5.5% | **split** to a separate edge table |
| `related_works` | 1% | **drop** — algorithmically derived |

Two filtering grains were considered: by **source** (e.g. exclude physics journals)
vs by **topic domain**. Source is the wrong grain — multidisciplinary megajournals
(Nature, PLOS ONE, Scientific Reports) carry both biomedical and physical-science
work, so a source filter both over- and under-cuts. The topic hierarchy is clean,
and the domain sizes make the case: Physical 162M + Social 117M vs **Health 64M +
Life 52M ≈ 116M of 492M (~24%)**.

## Decision

Ingest OpenAlex **directly from the public snapshot with DuckDB** — read, project,
filter, and reconstruct in one pass; never re-stage the 639 GB locally. The snapshot
*is* the durable raw layer (immutable monthly release, addressable by `updated_date`),
which is our deliberate deviation from the local-bronze medallion contract (ADR-0012):
re-curating means re-reading the snapshot, not re-downloading to disk.

Three prunings, all recorded here so they are auditable and revisitable:

1. **Row filter** — keep only works whose `primary_topic.domain` is in
   `openalex_domains` (default `["1","4"]` = Life + Health Sciences). Works with no
   `primary_topic` are dropped.
2. **Field prune** — `read_json` reads only the keys we keep, so `mesh`, `concepts`,
   `locations`, and `related_works` are never even decoded.
3. **Transform** — reconstruct the `abstract_inverted_index` to plaintext in SQL
   (`json_each` + positional `string_agg`). Plaintext is smaller than the position
   lists and directly usable for FTS (the same need as the PMC mining layer).

`referenced_works` becomes a separate **`openalex.work_references`** edge table
(`work_id` ↔ `referenced_work_id`, both short ids). This preserves the full citation
graph — including the non-PubMed works iCite cannot see — without bloating the works
row, and lets the edge table compress as two narrow columns.

The small **reference entities** (`institutions`, `sources`, `funders`, `topics`)
load as their own tables and are loaded **first** — they are cheap and are the actual
benchmarking unlock, useful even before the works decision is final.

Loads go through the standard `upsert` (ADR-0003); `snapshot_version` is the pull
label (`YYYY-MM`), excluded from change-detection. The initial bulk uses
`mode="append"` (write-only INSERT) because part files are disjoint by id within a
snapshot — re-reading the growing target on every batch would be O(n²) (the lesson
from the PMC load, ADR-0002). Monthly incrementals read only partitions newer than
the watermark and use `mode="merge"`. Parts process in batches of
`openalex_batch_files` so a temp table never holds the whole corpus; `max_files`
caps the part count, making a laptop subset and the full server run the same code.

## Consequences

- The curated `openalex.works` lands at roughly 24% of records, pruned and
  abstract-reconstructed — expected to be low tens of GB as columnar zstd Parquet,
  not 639 GB.
- **`mesh` is recoverable** via `works.pmid` ↔ omicidx PubMed, but only for
  PubMed-indexed works; non-PubMed works lose MeSH. Accepted (revisit if a non-PubMed
  MeSH use case appears — flip a field back on and re-read the snapshot).
- **`referenced_works` is the citation graph**; dropping the nested array in favor of
  the edge table means citation *metrics* still come from iCite/`cited_by_count`,
  while graph traversal uses `work_references`.
- `authorships`/`topics`/`keywords`/`grants` are kept as JSON for v1 (institution
  ROR lives in `authorships`). A future refinement may trim `authorships`
  (drop `raw_affiliation_strings`/`lineage`) or split a typed author edge table — both
  are re-derivable from the snapshot, so deferring is cheap.

See `docs/design/openalex.md` for the table schemas and the join paths into the rest
of the lake.
