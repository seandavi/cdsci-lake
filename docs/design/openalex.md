# OpenAlex — design

Ingest design + table schemas + join paths. The prune/filter/split decision (and
why) is ADR-0005; write semantics are ADR-0003.

## Source

- **Snapshot:** the public S3 bucket `s3://openalex`, read **anonymously** over its
  https endpoint `https://openalex.s3.amazonaws.com` (no AWS account, no creds).
  Each entity has `data/<entity>/manifest` (JSON: `entries[].url` + `meta`) and gz
  NDJSON part files under `data/<entity>/updated_date=YYYY-MM-DD/part_*.gz`.
- **Cadence:** monthly release; partitioned by `updated_date`. Incrementals read
  only partitions newer than the last pull (watermark) and MERGE on `id`.
- **Scale (works, 2026-06):** 639 GB gz, 492,361,307 records, 2,127 parts.
- **License:** CC0 — clean to republish into the shared lake.

## Pipeline

DuckDB reads each part directly from https, applies the projection + domain filter +
abstract reconstruction in one pass, and loads the lake. The snapshot is the durable
raw layer, so there is no local bronze (ADR-0005). Parts run in batches of
`openalex_batch_files` (default 50, ~300 MB / ~230k works each) so a temp table never
holds the whole corpus; `openalex_max_files` caps the part count — a laptop subset and
the full server run are the same code path.

Read once per batch into `_oa_raw`, then derive **both** the works row (everything
except the `referenced_works` list) and the edge rows (`referenced_works` unnested)
from it, so the S3 bytes are read once.

## Abstract reconstruction

OpenAlex ships no plaintext abstract (legal); it ships an inverted index
`{word: [positions]}`. Reconstruction is sanctioned and done in SQL:

```sql
SELECT string_agg(w.key, ' ' ORDER BY p.pos)
FROM json_each(abstract_inverted_index) AS w,
     unnest(CAST(w.value AS BIGINT[])) AS p(pos)
```

Plaintext is smaller than the position-list JSON and directly FTS-able.

## Tables (schema `openalex`)

### `works` — key `id` (short form, e.g. `W2741809807`)

Typed core + crosswalk ids + reconstructed abstract; heavy structures kept as JSON.

```
id, doi, pmid (BIGINT), pmcid (PMCxxxx), mag_id,
title, display_name, publication_year, publication_date (DATE), language, type,
abstract,                                            -- reconstructed plaintext
topic_id, topic_name, subfield_name, field_name, domain_id, domain_name,
oa_status, is_oa, source_id, source_name, source_issn_l, source_type, oa_pdf_url,
cited_by_count, fwci, is_retracted, referenced_works_count,
authorships (JSON), topics (JSON), keywords (JSON), grants (JSON),
updated_date, snapshot_version
```

`pmid` is `BIGINT` (matches `reporter.publink.pmid`, `icite.metadata.pmid`).
Institution ROR lives inside `authorships` (kept as JSON for v1).

### `work_references` — key (`work_id`, `referenced_work_id`)

The citation graph as id→id edges (both short form), incl. non-PubMed works iCite
can't see. `snapshot_version` excluded from change-detection.

### Reference entities (loaded first — the benchmarking unlock)

- `institutions` — key `id`; `ror`, `display_name`, `country_code`, `type`,
  `works_count`, `cited_by_count`, `grid_id`, geo (`city`/`region`/`country`/
  `latitude`/`longitude`), `lineage` (parent institution ids).
- `sources` — key `id`; `issn_l`, `display_name`, `type`, `host_org_id`,
  `host_organization_name`, `country_code`, `is_oa`, `is_in_doaj`, counts, `issn`.
- `funders` — key `id`; `display_name`, `country_code`, `ror`, `crossref_id`,
  `works_count`, `grants_count`, `cited_by_count`.
- `topics` — key `id`; `display_name`, `description`, `subfield_*`, `field_*`,
  `domain_*`, `keywords`, `works_count`.

## Join paths into the lake

- `works.pmid` ↔ `reporter.publink.pmid` ↔ `reporter.projects` — grant → OpenAlex.
- `works.pmid` ↔ `icite.metadata.pmid` — RCR/citation metrics (kept there, not here).
- `works.pmid` ↔ omicidx PubMed — recover `mesh` (dropped here) + titles.
- `works.pmcid` ↔ `pmc.fulltext.pmcid` — link to full text for mining.
- `authorships[].institutions[].ror` ↔ `institutions.ror` — the ROR institution
  graph for peer-center benchmarking.
- `work_references` (work_id → referenced_work_id) — citation graph traversal.

## CLI

```
python -m cdsci.lake.sources.openalex parts --entity works        # part count
python -m cdsci.lake.sources.openalex entities --max-files 2       # ref entities (subset)
python -m cdsci.lake.sources.openalex works --max-files 4 --mode merge
# full server run: entities, then `works --mode append` for the bulk,
# then monthly `works --mode merge` on new updated_date partitions
```

## Deferred

- Trim `authorships` (drop `raw_affiliation_strings`/`lineage`) and/or a typed author
  edge table — re-derivable from the snapshot, so cheap to defer.
- Watermark-driven incrementals (read only `updated_date > last_pull`) — wire into the
  planned `lake_ops` metadata model (ADR-0001 §6).
- Type-normalize `pmid` against omicidx PubMed (`VARCHAR`) for `ref.id_crosswalk`.
