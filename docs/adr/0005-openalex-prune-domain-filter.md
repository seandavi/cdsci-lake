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
| `authorships` | 27% | **split** to `works_authorships` (institution ROR is the benchmarking key — promote it out of JSON) |
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

Two structures are promoted out of the works row into **edge tables**, both derived
from the same single read of each part:

- **`work_references`** (`work_id` ↔ `referenced_work_id`, short ids) — the citation
  graph, including non-PubMed works iCite cannot see.
- **`works_authorships`** (`work_id`, `author_id`, `institution_id` + author name /
  position / `is_corresponding` / institution ROR / country) — the **ROR-keyed
  affiliation edge**. This is the peer-center benchmarking join, so it must be a
  first-class table, not a nested extraction from an `authorships` JSON blob. The
  grain is one row per (work, author, institution); authorships with no institution
  are dropped (the ROR is the point), and the full struct is unnested with the
  *lenient* `from_json` so OpenAlex adding fields — or schema drift across the
  historical partitions — does not break the load.

DOIs are **normalized on read** — `lower(replace(doi,'https://doi.org/',''))` — so
`works.doi` is the bare lowercase form. OpenAlex stores the prefixed URL, and the
unprefixed lowercase form is the cross-source join key (a normalization other lakes
learned the hard way; see Prior art).

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
- `topics`/`keywords`/`grants` are kept as JSON for v1 (small, and not yet a join
  surface). `authorships` is **not** kept as JSON — it is fully represented by
  `works_authorships`, which loses authors with no institution; the full author list
  (incl. unaffiliated) is re-derivable from the snapshot if a use case needs it.
- `works_authorships` is large (the upstream `authorships` is 1.3B rows corpus-wide;
  our domain-filtered slice is a fraction of that). It is two-plus narrow columns, so
  it compresses well, but it is a "filter aggressively" table for consumers.

## Prior art

The DOI normalization and the edge-table shape are adopted from
[J0nasW/science-datalake](https://github.com/J0nasW/science-datalake), a portable
DuckDB-over-Parquet lake of the same scholarly sources. We took its lessons on
content/schema while keeping our own storage substrate (shared DuckLake on Postgres +
R2, with change-detecting upserts and time-travel — vs. its view-only local Parquet):

- **DOI normalization** — it documents that OpenAlex/SciSciNet ship `https://doi.org/`
  -prefixed DOIs while S2AG ships bare lowercase, and centralizes
  `lower(replace(doi,'https://doi.org/',''))`. We normalize at ingest so `works.doi`
  is join-ready.
- **Edge tables over nested JSON** — it decomposes works into `works_authorships`
  (1.32B) and `works_referenced_works` (3.01B) rather than nesting; we do the same for
  exactly the benchmarking-join reason.
- Its `xref.doi_map` / `xref.unified_papers` are the template for our planned
  `ref.id_crosswalk`, and its LLM-oriented `SCHEMA.md`/`CATALOG.md` inform the planned
  consumer contract docs — both tracked in `docs/ROADMAP.md`.

See `docs/design/openalex.md` for the table schemas and the join paths into the rest
of the lake.
