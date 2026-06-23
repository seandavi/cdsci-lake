# Research notes — bibliometrics & science-of-science methods

A small, growing collection of notes on **methods** relevant to the lake:
bibliometric metrics, entity linkage/disambiguation, field normalization,
impact/disruption measures, and the design of comparable open data lakes. The
emphasis is *methods and datasets we could adopt or join against*, not a
literature survey.

These notes are background that feeds the backlog (`docs/ROADMAP.md`) and the
ADRs; when a method here turns into a decision, it graduates to an ADR or a source
design doc (`docs/design/`).

## How to add a note

One file per paper/method, named `YYYY-short-slug.md`. Lead with the citation
(+ DOI and an open-access link), then a structured summary, then a short
**"Relevance to cdsci-lake"** section connecting it to our sources/roadmap. Add a
row to the index below.

## Index

| note | topic | relevance |
|------|-------|-----------|
| [2023-sciscinet-data-lake.md](2023-sciscinet-data-lake.md) | SciSciNet — open science-of-science data lake (MAG-based); disruption, novelty, sleeping-beauty, external linkages | Candidate source (ROADMAP); methods + linkage recipes to borrow |
| [bibliometric-network-methods.md](bibliometric-network-methods.md) | Co-citation, bibliographic coupling, co-authorship/co-affiliation networks + centrality, Rao–Stirling interdisciplinarity, FWCI/RCR | What/why for network + diversity metrics |
| [metrics-sql-sketch.md](metrics-sql-sketch.md) | The metrics as candidate DuckDB SQL over our OpenAlex tables (cf, hit-papers, disruption, novelty, sleeping-beauty, team size, h-index, co-citation, bibliographic coupling, co-authorship, diversity) | Working sketches toward derived metric views |
