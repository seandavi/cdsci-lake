# SciSciNet: a large-scale open data lake for the science of science

> Lin, Z., Yin, Y., Liu, L., & Wang, D. (2023). **SciSciNet: A large-scale open
> data lake for the science of science research.** *Scientific Data* 10, 315.
> DOI [10.1038/s41597-023-02198-9](https://doi.org/10.1038/s41597-023-02198-9) ·
> OA: [PMC10235093](https://pmc.ncbi.nlm.nih.gov/articles/PMC10235093/) ·
> code: [kellogg-cssi/SciSciNet](https://github.com/kellogg-cssi/SciSciNet)

The reference example of the thing we are building: an open, pre-linked,
pre-computed data lake for quantitative studies of science. Worth studying both as
a **candidate source** (already on `docs/ROADMAP.md`) and as a **methods + schema
template**.

## What it is

A curated data lake that takes a bibliographic backbone, links it to external
"uses" of science (funding, patents, trials, media), and **pre-computes the
common science-of-science measures** so analysts don't re-derive them. 22 tables,
~1.5B+ records, distributed as TSV on Figshare. Note the lineage: v1 is built on
**Microsoft Academic Graph (MAG, Dec 2021)** — now discontinued; the **v2**
successor (on our roadmap) rebuilds on OpenAlex, which is why the OpenAlex **Work
ID** is the free join key for us.

## Backbone + external linkages

- **Backbone (from MAG):** 134.1M primary papers, 134.2M authors (disambiguated by
  MAG), 26,998 institutions, 49k journals; a 1.59B-edge citation network and a
  413.8M-row paper–author–affiliation network; 19 top-level + 292 subfield
  classifications.
- **External linkages (the valuable part):** NIH RePORTER (6M grant→paper),
  NSF (1.3M), USPTO/EPO patents (38.7M patent→paper citations),
  ClinicalTrials.gov (438K), Twitter + news via Crossref Event Data
  (55.8M tweets / 595K news), Nobel-laureate sets.

## Methods catalogued (the reason this note exists)

**Paper-level impact / dynamics:**
- Citation counts `c5`/`c10`; **field-normalized citation** `cf` (divide by field-year
  mean); hit-paper flags (top 1/5/10%).
- **Disruption index (D / CD index)** — does a paper *eclipse* its predecessors
  (disruptive) or *build on* them (developing)? Computed from the
  citing-papers-also-cite-the-references pattern.
- **Novelty / conventionality** — z-scores over journal-reference *pairings* vs. a
  reshuffled null (Uzzi et al.); atypical combinations = novelty.
- **Sleeping beauty** coefficient (delayed recognition) and the **WSB** citation
  model (immediacy μ, longevity σ, fitness λ).
- Team size, institution count, #external references.

**Author / institution:** h-index, productivity, mean `c10` / log `c10`;
name-based **gender inference** P(female) (flagged as imperfect).

**External impact:** counts of patent, clinical-trial, news, and social citations;
funding-support counts.

## Linkage recipes (directly reusable)

- **PMID as the intermediate key** for biomedical linkage (NIH, trials): 98.9%
  retention; MAG↔PMID kept 31.2M pairs at 95.6%. Matches our publink/iCite/ctgov
  reliance on PMID.
- **DOI normalization** before matching news/Twitter → 94%+ match rates.
- **Funder grant→paper matching ladder (NSF):** DOI exact → title
  standardization → Elasticsearch candidate search with a z-score threshold
  (p=0.05) → fuzzy match → Crossref backfill; 1.3M links incl. 178K fuzzy.
- **Validation by rank correlation** (Spearman 0.99 citations/refs, 0.95
  cross-database) as an acceptance check.

## Methodological choices & limitations

- Curation: drop records without DOI/doc-type, aggregate paper "families" to a
  primary record, **exclude retracted papers from the main table** (kept in a
  details table), and **recompute citation counts within the 134M subset** for
  internal consistency.
- Limits: funding = NSF/NIH only; patents = USPTO/EPO only; trials/media skew
  US/Crossref; **gender inference is imperfect**; it's a **static MAG snapshot**;
  author disambiguation inherits MAG's errors; **family aggregation can blur the
  preprint↔published distinction** (relevant to our PreprintToPaper candidate).

## Relevance to cdsci-lake

- **Source decision (ROADMAP).** Reinforces the roadmap stance: ingest **SciSciNet
  v2** (OpenAlex-based) only for its *derived* measures (disruption, novelty,
  sleeping-beauty), and get freshness/coverage from OpenAlex directly. The 210 GB
  size keeps it a "maybe."
- **Methods we could compute ourselves** on `openalex.works` + `work_references`
  (we already have the citation edge table): **field-normalized citations**,
  **disruption index**, **novelty z-scores**. These are candidates for the planned
  versioned consumer views / a `metrics`-style derived layer — but note our charter
  keeps *derived/gold* in consumers, so weigh whether these are source-faithful
  enough to live in the lake.
- **Linkage validation.** Their PMID-intermediate + DOI-normalization + rank-
  correlation acceptance pattern is a concrete template for **`ref.id_crosswalk`**
  and for QA on our reporter/iCite/ctgov joins.
- **Retraction handling.** "Exclude retracted from the main table, keep a details
  table" pairs naturally with the **Retraction Watch** candidate source.
