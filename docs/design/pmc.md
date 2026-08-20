# BioC-PMC full text — design

Ingest design + the planned mining layer. Decision to load the full corpus (and
why) is ADR-0002; write semantics are ADR-0003.

## Source

- **Bulk:** `https://ftp.ncbi.nlm.nih.gov/pub/wilbur/BioC-PMC/` — 22 tarballs for
  the `json_unicode` variant (`PMC{range}XXXXX_json_unicode.tar.gz`, ~4.4–7.6 GB
  each). Each tarball = one BioC **collection** JSON per article (inner files named
  `.xml` but the content is JSON). ~130 GB compressed, ~6–12M articles.
- **Incrementals:** `…/RESTful/pmcoa.cgi/BioC_json/{PMCID|PMID}/unicode` — one
  article per call, for PMCIDs newer than the last bulk; MERGE-on-`pmcid` makes
  bulk + API top-ups idempotent.

## BioC record shape (extraction map)

```
collection.documents[0]
  .id                                  -> pmcid          ("PMC1790863")
  .infons.license                      -> license        ("CC BY")
  .passages[0].text                    -> title
  .passages[0].infons."article-id_pmid"-> pmid           (93% present)
  .passages[0].infons."article-id_doi" -> doi            (74% present)
  .passages[*]                         -> full text (offset-tagged sections)
```

## Ingest flow (medallion, per range)

`download tarball → stream to gzipped NDJSON → bronze Parquet (pmcid, record) →
load both silver tables → delete local transients → next range`. The bronze
Parquet `(pmcid, record)` is the faithful capture; the two silver tables on R2
(`pmc.documents`, `pmc.passages`) are derived from it in one curate pass — one row
per article and one row per passage — so nothing local is retained (bounds disk to
~one range). Peak ~15 GB/range; ~12 min/range.

**Bulk uses `append`, not `merge`.** The PMCID ranges are disjoint, so a MERGE's
whole-table target read (which DuckLake can't prune — `pmcid` is a string and
sorts lexically, not numerically) finds zero matches yet grows to ~200 GB of R2
reads by the last range (O(n²), and it timed out the first attempt). `mode=append`
(INSERT, no target read) makes the initial bulk write-only. **Incrementals use
`mode=merge`** (the per-article API fetches scattered PMCIDs that *do* exist) —
idempotent on `pmcid`. Loads also set a generous HTTP timeout + retries
(`lake.connect`) so large R2 parquet GETs survive the link.

## Tables

Normalized into a **document → passage one-to-many** rather than a flat table that
carried the whole BioC `record` as a nested blob. The bronze Parquet `(pmcid,
record)` remains the faithful raw capture; both silver tables are derived from it
and recomputable as extraction patterns improve (no re-download).

- **`pmc.documents`** (key `pmcid`) — one row per article: `pmid`, `doi`,
  `license`, `title`, `n_passages`, `snapshot_version`. The crosswalk + article
  metadata, with no nested text.
- **`pmc.passages`** (key `(pmcid, passage_index)`) — one row per BioC passage,
  exploded out of `record.documents[0].passages[*]`: `passage_offset` (BioC char
  offset), `section_type` / `passage_type` (from passage `infons`), `text`, and the
  full passage `infons` JSON (nothing passage-level lost). `pmcid` →
  `documents.pmcid`. This is the full-text surface the mining layer reads.

## Planned mining layer (the use cases)

Derived tables/views over `pmc.passages.text`. These are the reason the full
corpus is loaded:

1. **`pmc.accession_mentions`** `(pmcid, accession, accession_type, passage_offset)`
   — regex for GEO (`GSE/GSM/GPL`), SRA (`SRR/SRX/SRP/PRJNA`), BioSample (`SAMN`),
   BioProject (`PRJNA/PRJEB/PRJDB`) ids. Joins to `omicidx` (same accession space)
   to link papers ⇄ data deposits — the omicidx-adjacent FTS use case.
2. **`pmc.software_mentions`** `(pmcid, url, host)` — extract URLs and bucket by host:
   `github.com`, `bioconductor.org`, `cran.r-project.org`, `pypi.org`. Software in
   the literature.
3. **`pmc.cfde_mentions`** `(pmcid, project, url)` — CFDE project names / URLs, for
   CFDE evaluation.

Plus **full-text search**: DuckDB's `fts` extension over `pmc.passages.text` gives
granular, section-aware search out of the box now that passages are first-class
rows (`section_type` / `passage_type` are filterable columns).

## Crosswalk

`pmcid` ↔ `pmid`/`doi` ties full text to `icite.metadata` (RCR), `reporter.publink`
(grants), `ctgov.references` (trials), and `omicidx.pubmed_article` — each a
direct join on the shared id, no crosswalk table in between (`ref.id_crosswalk`
was retired unused; see `docs/ROADMAP.md`). Note the recurring type reconciliation:
`pmc.documents.pmid` is BIGINT vs `omicidx.pubmed_article.pmid` VARCHAR.
