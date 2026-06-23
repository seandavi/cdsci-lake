# Bibliometric network & diversity methods

Survey notes on methods beyond the [SciSciNet](2023-sciscinet-data-lake.md)
per-paper metric set — specifically **citation-based networks** (co-citation,
bibliographic coupling), **collaboration networks** (co-authorship, institutional
co-affiliation), and **diversity/interdisciplinarity** measures. SQL sketches for
the SQL-expressible ones are in [metrics-sql-sketch.md](metrics-sql-sketch.md)
(§"Network metrics"); this note is the *what and why*.

We are unusually well-placed for these: we already hold the two edge tables they
need — `lake.openalex.work_references` (citation graph) and
`lake.openalex.works_authorships` (authorship graph, incl. ROR institution ids).

## Three ways to relate papers by citation

All three turn the citation graph into a paper–paper similarity network; they
differ in *which* shared structure defines a link.

| method | link rule | neighbor order | temporal behavior | best for |
|--------|-----------|----------------|-------------------|----------|
| **Direct citation** | A cites B | first | accrues over time | citation flow; we have it raw |
| **Co-citation** | A & B both cited *by* a later paper C | second | **dynamic** — strength grows as new citers appear; favors **older**, established papers | mapping *foundational* / intellectual base |
| **Bibliographic coupling (BC)** | A & B both *cite* a common reference | second | **time-invariant** — fixed once A,B are published | mapping *research fronts*; **best for biomedical fronts** |

- **Co-citation strength** of (A,B) = number of papers citing both. It excludes
  not-yet-cited recent papers, so it lags the frontier.
- **Bibliographic coupling strength** of (A,B) = number of references they share.
  It's available immediately at publication and doesn't drift, which is why Boyack
  & Klavans found BC represents the *research front* most accurately, and it
  "captures more unique information" than co-citation or direct citation.
- Both are usually **normalized** (cosine/Salton: `cocite(A,B) / sqrt(deg(A)·deg(B))`)
  before clustering, so high-degree papers don't dominate.
- Downstream use: **science mapping** (cluster the network → topic/field maps),
  related-paper recommendation, research-front detection. Citation-based
  clustering is a recognized alternative to topic-model text clustering.

> Sources: Kleminski et al. 2022 (*JIS*) on direct vs co-citation vs BC for topic
> identification; Boyack & Klavans on which best represents the research front;
> Habib & Afzal on BC accuracy for biomedical fronts. See links at the bottom.

## Collaboration networks

### Co-authorship (author–author)
Nodes = authors, an edge = co-authored ≥1 paper (weight = #papers together), built
from `works_authorships`. The standard SNA toolkit applies:

- **Degree centrality** — # distinct collaborators (SQL-native).
- **Betweenness centrality** — brokerage across communities; "reveals how
  influential an author is in a research community." (needs shortest paths → graph lib)
- **Closeness, eigenvector centrality** — reach / influence-weighted prominence.
- **Clustering coefficient, components (LCC), modularity, assortativity** —
  community structure and network resilience.
- Empirical hook worth testing on our data: an article's **citation count
  correlates positively with its co-authors' degree and betweenness centrality**,
  and centrality correlates with collaboration strength.

### Institutional co-affiliation (ROR–ROR)
The same construction on `institution_id`/`institution_ror` gives an
**institution collaboration network** — directly the **peer-center benchmarking**
unlock (ADR-0005 calls the ROR affiliation edge the benchmarking join). Center A's
collaborators, brokerage position, and shared-output volume with peer centers all
fall out of this graph.

> Sources: Newman, "Coauthorship networks and patterns of scientific
> collaboration"; centrality-vs-impact case studies; collaboration-network
> resilience (clustering/modularity/LCC) reviews.

## Diversity / interdisciplinarity

Interdisciplinarity is operationalized as **diversity of the disciplines a paper
draws on**, measured over the fields of its *referenced works*. Three components
(Stirling): **variety** (# distinct fields), **balance** (evenness of the
distribution), **disparity** (how *different* the fields are from each other).

- **Rao–Stirling diversity:** `D = Σ_{i≠j} p_i · p_j · d_ij`, where `p_i` is the
  share of references in field *i* and `d_ij` is a disparity (distance) between
  fields *i,j*. Combines all three components.
- **Simplification we can do today:** with `d_ij = 1[i≠j]` it reduces to the
  **Gini–Simpson index** `1 − Σ p_i²` (variety+balance, no disparity) — pure SQL
  over reference-field shares. Full Rao–Stirling needs a **field–field disparity
  matrix** (e.g. `1 − cosine` similarity from field co-citation), a precompute step.
- Caveat from the literature: the "dual-concept" Rao–Stirling can give anomalous
  results; Leydesdorff et al. recommend measuring variety, balance, disparity
  **independently** then combining — so compute the components separately too
  (distinct-field count; Shannon evenness; mean pairwise disparity).

> Sources: Leydesdorff, Wagner & Bornmann 2019 (*JIS*) on Rao–Stirling vs variety
> + Gini; Stirling's variety/balance/disparity framework; PLOS ONE institutional
> Rao–Stirling application.

## Field-normalized impact (relation to what we already have)

- **FWCI** (Elsevier) and **RCR** (NIH/iCite) are article-level, field-normalized
  citation metrics; studies find they're largely **interchangeable** and correlate
  with established field-normalized indicators. **We already hold RCR** in
  `lake.icite.metadata` — so rather than reinvent FWCI, we can (a) use RCR directly
  and (b) **validate our home-grown `cf`** (field-normalized citations,
  metrics-sql §2) against iCite RCR on shared PMIDs as a sanity check.
- Caveat: RCR/mean-based normalization is criticized for skewed, long-tailed
  citation distributions (outlier-sensitive); **percentile/hit-paper** approaches
  (metrics-sql §3) are the robust complement.

## What's SQL-native vs needs a graph library

- **SQL-native (DuckDB):** all the edge constructions (co-citation, BC,
  co-authorship, co-affiliation edge lists + weights), **degree** centrality,
  cosine/Salton normalization, Gini–Simpson / Shannon / variety diversity.
- **Needs export to a graph lib** (networkx / igraph / graph-tool) or an iterative
  engine: **betweenness / closeness / eigenvector** centrality, **community
  detection / modularity**, connected components at scale. Plan: materialize the
  edge list with DuckDB (cheap, set-based), then run path/community algorithms in
  Python on the slice of interest. Several of these also want the **re-expanded
  citation corpus** (the domain prune is a reversible cost dial, ADR-0005) when run
  beyond a Life+Health cohort.

## Sources

- [Kleminski et al. 2022 — direct citation vs co-citation vs BC for topic ID](https://journals.sagepub.com/doi/10.1177/0165551520962775)
- [Boyack & Klavans — which citation approach represents the research front most accurately](https://www.researchgate.net/publication/220433193_Co-Citation_Analysis_Bibliographic_Coupling_and_Direct_Citation_Which_Citation_Approach_Represents_the_Research_Front_Most_Accurately)
- [Bibliographic coupling overview (ScienceDirect Topics)](https://www.sciencedirect.com/topics/economics-econometrics-and-finance/bibliographic-coupling)
- [Newman — coauthorship networks and patterns of scientific collaboration](https://www.researchgate.net/publication/8901684_Coauthorship_Networks_and_Patterns_of_Scientific_Collaboration)
- [Network effects on scientific collaborations (PMC)](https://www.ncbi.nlm.nih.gov/pmc/articles/PMC3585377/)
- [Collaboration-network resilience via bibliometric + network analysis (MDPI Data 2025)](https://www.mdpi.com/2306-5729/10/11/184)
- [Leydesdorff, Wagner & Bornmann 2019 — Rao–Stirling diversity, relative variety, Gini (arXiv)](https://arxiv.org/pdf/1807.04115)
- [Analysing institutions' interdisciplinarity via Rao–Stirling (PLOS ONE)](https://journals.plos.org/plosone/article?id=10.1371/journal.pone.0170296)
- [Comparison of FWCI and RCR (field-independent article metrics)](https://www.researchgate.net/publication/332344383_Comparison_of_two_article-level_field-independent_citation_metrics_Field-Weighted_Citation_Impact_FWCI_and_Relative_Citation_Ratio_RCR)
- [Bornmann & Haunschild — RCR empirical study (arXiv)](https://arxiv.org/pdf/1511.08088)
