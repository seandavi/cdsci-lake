# cdsci-lake — roadmap

Living list of planned work. This is the **forward-looking** companion to
`docs/STATUS.md` (which is a point-in-time snapshot of what's loaded *now*).
Decisions are captured as ADRs (`docs/adr/`); this file is the backlog and the
rationale for *what's next*, not how it was built.

## Platform (near-term)

- **`lake_ops` metadata model** (ADR-0001 §6) — source / version / run / watermark /
  contract tables in the Postgres catalog; wire every ingestor to record runs +
  snapshot ids. Unblocks watermark-driven incrementals (e.g. OpenAlex
  `updated_date > last_pull`).
- **Scoped roles** — replace the admin bootstrap credential with `lake_writer`
  (ingest) and `lake_reader` (consumers) Postgres roles + scoped R2 tokens.
- **Versioned consumer views + `dataset_contract` registry** — per-source stable
  views (`icite.v_rcr`, …) as the consumer contract, so column renames don't break
  downstream. Add before multiple consumers depend on raw table column names.
- **`ref.id_crosswalk`** — the unified PMID ↔ DOI ↔ PMCID ↔ core_project_num ↔ NCT
  table. Adopt the science-datalake `xref` shape (see below). Normalize `pmid` types
  (`publink.pmid` BIGINT vs omicidx `VARCHAR`) and DOI (lowercase, unprefixed —
  already done for `openalex.works`).
- **CRISP historical RePORTER (1970–2009, XML)** — the two pre-CSV ExPORTER groups
  need an XML stream-parse path. Not implemented.
- **Consumer migration** — point `cancer_center` enrichment at `lake.icite.metadata`
  (RCR) instead of per-project caches.
- **Repo remote** — push this repo to GitHub (local history only today).

## OpenAlex follow-ups (ADR-0005)

- Watermark incrementals once `lake_ops` lands.
- Optional full author table (incl. unaffiliated authorships) — re-derivable from the
  snapshot if a use case appears.
- Optional `valid_title_abstract`-style quality flag on `works` (English, title/abstract
  length, ASCII ratio) — cheap at curate; useful for FTS. (Pattern from science-datalake.)

## Candidate sources (researched 2026-06-23)

Admission order is **license-to-republish → non-redundancy → use-case pull → bulk
availability** (a proposed ADR-0004 "source admission criteria" should formalize this).
All four below join cleanly off the OpenAlex hub we now have.

| source | cadence | distribution | join key | license | size | ingest plan |
|--------|---------|--------------|----------|---------|------|-------------|
| **Reliance on Science** (patents↔papers) | versioned, ~annual (Jun–Jul); latest 2024 ed. (Zenodo rec 11461587) | Zenodo concept DOI `10.5281/zenodo.3236339`, `_pcs_oa.csv` (CSV) | **OpenAlex Work ID** (+DOI/PMID crosswalks) | **CC BY-NC 4.0** — non-commercial; flag for review | ~2.5 GB core / ~25 GB full; ~16M cites | poll concept DOI ~quarterly; infrequent recurring loader |
| **Retraction Watch** (integrity) | **weekday-daily** | Crossref GitLab `crossref/retraction-watch-data`, single `retraction_watch.csv` (semicolon multi-value) | DOI (`OriginalPaperDOI`); PMID secondary — **needs DOI→OA crosswalk** | effectively CC0 | ~65 MB, ~70.6k rows | **recurring daily sync** (weekday); full-file diff on `Record ID`; split multi-value fields |
| **SciSciNet v2** (sci-of-science: disruption, etc.) | **static per version**; v2 ~spring 2025, no v3 | GCS `gs://sciscinet-neo/v2/` (Parquet); HF small tables; BigQuery (form-gated) | **OpenAlex Work ID** (v2 PaperID) | BSD-3-Clause-Clear (verify) | ~210 GB core (+1.7 TB embeddings, optional) | one-time pinned-snapshot loader; use OA directly for freshness, SciSciNet only for derived measures |
| **PreprintToPaper** (bioRxiv/medRxiv→published) | **static**, occasional versions; v2.0.0 (2025-12-19) | Zenodo `10.5281/zenodo.17992421`, `PreprintToPaper.csv` (CSV) | DOI pairs — **no OA ids/PMIDs**, needs DOI→OA crosswalk | CC-BY-4.0 | ~617 MB, ~145.5k rows | one-shot static reference table |

Notes:
- **Only Reliance on Science is non-open** (CC BY-NC) — review before republishing in
  the shared lake. The other three are CC0 / CC-BY / BSD.
- **Retraction Watch is the one true recurring sync** here — small and daily; a good
  first customer for whatever scheduler we adopt (cron / pg_cron + LISTEN/NOTIFY).
- Reliance on Science + SciSciNet v2 join on the OpenAlex Work ID **for free**;
  Retraction Watch + PreprintToPaper are DOI-only and motivate `ref.id_crosswalk`.
- Use-case fit: **Reliance on Science** (papers→patents = translational benchmarking)
  and **Retraction Watch** (integrity flags) are the highest-value adds; **SciSciNet**
  (disruption as a complement to RCR) is a maybe given its 210 GB; **PreprintToPaper**
  is biomedical-relevant but narrow.

## Adopted from science-datalake (prior art)

[J0nasW/science-datalake](https://github.com/J0nasW/science-datalake) is a portable
DuckDB-over-Parquet lake of the same sources; we keep our DuckLake substrate but adopt
its content/schema lessons (see ADR-0005 "Prior art"):

- **`ref.id_crosswalk` ← its `xref` layer** — `doi_map` (normalized DOI →
  `(source, source_id)`, UNION-ALL, "always filter on `doi=`") + `unified_papers`
  (denormalized pre-join with per-source coverage flags). This is the template.
- **DOI normalization** — done for `openalex.works`; apply uniformly as sources land.
- **Edge tables over nested JSON** — done for `works_authorships` / `work_references`.

## Consumer-facing docs (the CATALOG.md idea)

science-datalake's `CATALOG.md` (query examples + dataset quirks) and `SCHEMA.md`
(every table with row counts, a scan-cost tier S/M/L/VL, and join recipes, written
*for an LLM*) are simple but very usable. We should generate equivalents for our lake:

- **`CATALOG.md`** — per-source quirks + copy-paste cross-source join examples.
- **`SCHEMA.md`** — table/column/row-count reference with performance tiers and join
  strategies, designed to ground the `cancer_center` chat/ask consumer.

These dovetail with the `dataset_contract` registry — ideally **generated** from the
catalog (row counts via DuckLake stats are cheap) rather than hand-maintained.

## Notes on the raw stage

For bulk sources whose upstream is an immutable, addressable, durable release, the
**upstream IS our raw/bronze layer** — we project/curate straight into the lake with no
local re-stage. OpenAlex is the first to use this (ADR-0005): the S3 snapshot is
re-readable by `updated_date`, so re-curating means re-reading, not re-downloading 639
GB. SciSciNet v2 (pinned GCS snapshot) and the Zenodo-hosted sources fit the same
pattern; Retraction Watch (a single rolling CSV) is the exception that wants a kept
bronze copy per pull for time-travel.
