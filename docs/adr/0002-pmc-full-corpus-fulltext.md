# 0002. PMC full text: load the full corpus, full BioC record

- Status: accepted
- Date: 2026-06-23

## Context

PubMed Central full text is available via **BioC-PMC**: a bulk FTP of per-PMCID-range
tarballs (`PMC{range}XXXXX_json_unicode.tar.gz`, one BioC *collection* JSON per
article — inner files named `.xml` but JSON content) and a per-article REST API
for incrementals. The json-unicode set is **22 tarballs ≈ 130 GB compressed, ~6–12M
articles**.

This is by far the heaviest source, so scope was a real decision. A 1-range pilot
(`PMC000XXXXX`) measured: **390,347 articles → 7.3 GB on R2** (~19.6 KB/article,
full record kept), **93% have a PMID** / 74% a DOI, **~12 min/range**. Extrapolated:
**~210 GB on R2, ~5–7 h** one-time load (~$3–4/mo R2 storage + read egress).

The choice: load the **full corpus** vs a cohort/cancer-topic **subset** (filter to
PMIDs already in the lake via `reporter.publink` / `ctgov.references` / cohort).

## Decision

**Load the full PMC corpus**, storing the **complete BioC record** per article.

The use cases are **corpus-wide text mining, not cohort lookup**, so a PMID subset
would discard most of the signal:

1. **Accession FTS** — find GEO / SRA / BioSample / BioProject accessions mentioned
   in the literature (omicidx-adjacent: which papers reference which deposits).
2. **Software mentions** — GitHub / Bioconductor / CRAN / PyPI URLs in text.
3. **CFDE** — project names and URLs, for CFDE evaluation.

These span all of biomedical literature; restricting to cancer/grant/trial-linked
PMIDs would miss the bulk of accessions, software, and CFDE references.

- **Two normalized tables, document → passage one-to-many** (not a flat table
  carrying the BioC `record` as a nested blob):
  - **`pmc.documents`** — key `pmcid`; `pmid`/`doi` (extracted from
    `passages[0].infons.article-id_*` — the crosswalk into iCite/grants/trials/omicidx),
    `license`, `title`, `n_passages`.
  - **`pmc.passages`** — key `(pmcid, passage_index)`; one row per BioC passage with
    `passage_offset`, `section_type`/`passage_type`, `text`, and the full passage
    `infons` JSON. This is the full-text surface (FTS / mining read it directly).
  Nothing passage-level is lost, and any future extraction is re-derivable from the
  bronze `(pmcid, record)` Parquet without re-fetch.
- **Per-range streaming** bounds local disk: download tarball → gzipped NDJSON →
  bronze Parquet → curate both silver tables (MERGE on the keys above) → **delete all
  local transients** → next range. The bronze `(pmcid, record)` Parquet is the
  faithful capture; the silver tables on R2 are derived from it.
- **Incrementals** via the per-article REST API for PMCIDs newer than the bulk;
  same MERGE-on-`pmcid` makes bulk + top-ups idempotent (one-time bulk, then API).

## Consequences

- ~210 GB on R2 — the largest tables in the lake, an accepted cost given the
  corpus-wide use cases. (Maintenance/expiry reclaims superseded versions.)
- Exploding passages into their own rows makes the mining layer (accession /
  software / CFDE extraction, FTS) read `pmc.passages.text` directly — section-aware
  search with no per-query JSON parse — and it stays **derived** from the bronze
  capture, recomputable as patterns improve. See `docs/design/pmc.md`.
- `pmcid`↔`pmid`/`doi` (93%/74%) completes the literature layer and gives the
  planned `ref.id_crosswalk` its PMCID anchor.

## Alternatives considered

- **Cohort/topic subset.** Cheaper (~20–60 GB) but defeats the use cases —
  accessions/software/CFDE mentions live across the whole corpus, not our PMIDs.
- **Flat text / dropping the bronze capture.** Lossy; couldn't re-mine for new
  patterns (new accession formats, software hosts) without re-fetching 130 GB. The
  bronze `(pmcid, record)` Parquet is kept precisely so silver can be re-derived.
- **Nested passages in one flat table.** The earlier `pmc.fulltext` design carried
  the whole BioC `record` per row; every text query re-parsed the JSON and passage
  metadata (`section_type`/offset) wasn't queryable. Normalizing to
  `documents` → `passages` makes passages first-class rows for FTS/mining.
- **Live-scrape / API for the bulk.** The bulk FTP exists and is far cheaper than
  ~6–12M API calls; API is for incrementals only.
