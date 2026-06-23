# Science-of-science metrics → SQL sketches (against our OpenAlex tables)

Working notes turning the measures catalogued in
[SciSciNet](2023-sciscinet-data-lake.md) into **candidate DuckDB SQL** over the
lake we actually have. These are *sketches to react to*, not finished views —
correctness and (especially) cost still need validation on real data.

## What we have to compute from

- **`lake.openalex.works`** — `id` (e.g. `W123`), `doi`, `pmid`, `publication_year`,
  `publication_date`, `cited_by_count` (OpenAlex **global** count),
  `referenced_works_count`, `topic_name`/`subfield_name`/`field_name`/`domain_id`,
  `source_id`/`source_name` (the journal), …
- **`lake.openalex.work_references`** — citation edges `(work_id → referenced_work_id)`
  = *work_id cites referenced_work_id*.
- **`lake.openalex.works_authorships`** — affiliation edges `(work_id, author_id,
  author_name, author_position, is_corresponding, institution_id, institution_ror,
  institution_country)`.

### Two caveats that color everything below

1. **The edge graph is pruned to our corpus — but the prune is reversible.**
   `work_references` only contains edges whose **citing** work is in our
   Life+Health subset (~116M of 492M), so edge-based metrics (c5/c10, disruption,
   sleeping-beauty series) currently **undercount** citations from outside the
   subset. This is a *current choice, not a ceiling*: the OpenAlex S3 snapshot is
   re-readable by `updated_date` (ADR-0005), so we can **re-expand the corpus**
   (more domains, or all works) when a metric needs the full citation graph — the
   prune is a cost/scope dial, not a permanent boundary. Two practical options when
   a structure metric matters: (a) re-curate the full citation edge table while
   keeping the works projection domain-pruned (edges are small relative to works),
   or (b) re-expand fully. Until then: for *raw impact* prefer `works.cited_by_count`
   (OpenAlex global, unaffected), and read edge-based structure metrics as
   "within the in-corpus graph" — or re-expand first. (`cited_by_count` itself is
   global regardless, so cf/hit-papers/h-index below are already corpus-independent.)
2. **`works_authorships` drops unaffiliated authors** (ADR-0005 — authors with no
   institution aren't in the edge table). So `count(author_id)` is a *floor* on
   team size. True team size needs the optional full-authorship table on the
   roadmap. Flagged per metric.

> **Governance.** These are *derived/gold* measures. ADR-0001 keeps derived
> products in consumers, not source schemas. So this likely lands as either
> consumer-side views or a clearly-marked derived layer (e.g. `metricsx.*`), not
> in `openalex.*`. Decision deferred; noted so we don't quietly violate the charter.

---

## 1. Citation windows c5 / c10 (edge-based)

Citations received within 5/10 years of publication. `cited` = focal paper,
`citing` = paper that cites it.

```sql
SELECT
    r.referenced_work_id AS work_id,
    count(*) FILTER (WHERE citing.publication_year <= cited.publication_year + 5)  AS c5,
    count(*) FILTER (WHERE citing.publication_year <= cited.publication_year + 10) AS c10
FROM lake.openalex.work_references r
JOIN lake.openalex.works citing ON citing.id = r.work_id
JOIN lake.openalex.works cited  ON cited.id  = r.referenced_work_id
WHERE cited.publication_year IS NOT NULL
GROUP BY 1;
```

Caveat 1 applies (subset undercount). c5/c10 are only meaningful for papers old
enough to have a full window — filter `cited.publication_year <= <snapshot_year> - 10`.

## 2. Field-normalized citations (cf)

Raw count ÷ mean count of same field-year cohort. Uses the **global**
`cited_by_count`, so caveat 1 doesn't bite.

```sql
WITH base AS (
    SELECT id, field_name, publication_year, cited_by_count
    FROM lake.openalex.works
    WHERE field_name IS NOT NULL AND publication_year IS NOT NULL
)
SELECT
    b.id AS work_id,
    b.cited_by_count,
    b.cited_by_count
      / nullif(avg(b.cited_by_count) OVER (PARTITION BY b.field_name, b.publication_year), 0) AS cf
FROM base b;
```

Swap `field_name` → `subfield_name` (292 cells, SciSciNet-like) for finer
normalization. Consider a `count >= N` floor per cell to avoid noisy small cohorts.

## 3. Hit papers (top 1% / 5% / 10%)

Percentile within field-year by citations.

```sql
WITH ranked AS (
    SELECT id AS work_id, field_name, publication_year, cited_by_count,
           percent_rank() OVER (PARTITION BY field_name, publication_year
                                ORDER BY cited_by_count) AS pr
    FROM lake.openalex.works
    WHERE field_name IS NOT NULL AND publication_year IS NOT NULL
)
SELECT work_id, cited_by_count,
       pr >= 0.99 AS is_top1, pr >= 0.95 AS is_top5, pr >= 0.90 AS is_top10
FROM ranked;
```

## 4. Disruption index (CD / D index)

For focal *F*: among papers citing *F*, `ni` cite **only** *F* (not its refs),
`nj` cite *F* **and** ≥1 ref of *F*; `nk` cite ≥1 ref of *F* but **not** *F*.
`D = (ni − nj) / (ni + nj + nk)`, in [−1, 1] (disruptive → developing).

```sql
WITH refs AS (   -- references of the focal
    SELECT work_id AS focal, referenced_work_id AS ref FROM lake.openalex.work_references
),
citers AS (      -- papers citing the focal
    SELECT referenced_work_id AS focal, work_id AS citer FROM lake.openalex.work_references
),
citer_hits_ref AS (   -- does each citer also cite any ref of the focal?
    SELECT c.focal, c.citer,
           count(wr.referenced_work_id) > 0 AS cites_ref
    FROM citers c
    LEFT JOIN refs r  ON r.focal = c.focal
    LEFT JOIN lake.openalex.work_references wr
           ON wr.work_id = c.citer AND wr.referenced_work_id = r.ref
    GROUP BY c.focal, c.citer
),
nij AS (
    SELECT focal,
           count(*) FILTER (WHERE NOT cites_ref) AS ni,
           count(*) FILTER (WHERE cites_ref)     AS nj
    FROM citer_hits_ref GROUP BY focal
),
nk AS (          -- cite a ref of focal but not focal
    SELECT r.focal, count(DISTINCT wr.work_id) AS nk
    FROM refs r
    JOIN lake.openalex.work_references wr ON wr.referenced_work_id = r.ref
    WHERE NOT EXISTS (
        SELECT 1 FROM lake.openalex.work_references x
        WHERE x.work_id = wr.work_id AND x.referenced_work_id = r.focal)
    GROUP BY r.focal
)
SELECT n.focal AS work_id,
       (n.ni - n.nj)::DOUBLE / nullif(n.ni + n.nj + coalesce(k.nk, 0), 0) AS disruption
FROM nij n LEFT JOIN nk k USING (focal);
```

⚠️ **Cost.** This is the expensive one — `citer_hits_ref` is roughly *citations ×
references* per focal, and `nk` scans the citers of every reference. **Do not run
corpus-wide as-is.** Run it for a **focal subset** (a cohort, a center's papers, a
field-year slice) by adding `WHERE focal IN (…)` early in `refs`/`citers`. A
scalable corpus-wide build likely wants a precomputed citing-set bitmap/array per
paper rather than this triple-join. Treat the SQL as the *definition*, not the
production plan.

## 5. Novelty / conventionality (Uzzi journal-pair z-scores)

Observed part is SQL; the null is not. For each focal, take the **journals** of
its referenced works, form all journal pairs, and count co-occurrence across the
corpus:

```sql
WITH ref_journal AS (   -- each focal's references mapped to their journal (source)
    SELECT r.work_id AS focal, w.source_id AS journal
    FROM lake.openalex.work_references r
    JOIN lake.openalex.works w ON w.id = r.referenced_work_id
    WHERE w.source_id IS NOT NULL
),
pairs AS (              -- unordered journal pairs co-cited within a focal
    SELECT a.focal, least(a.journal, b.journal) AS j1, greatest(a.journal, b.journal) AS j2
    FROM ref_journal a JOIN ref_journal b
      ON a.focal = b.focal AND a.journal < b.journal
)
SELECT j1, j2, count(*) AS observed_cooccurrence
FROM pairs GROUP BY 1, 2;
```

The z-score needs a **null distribution** from degree-preserving reshuffles of the
reference network (Monte Carlo) — not expressible in plain SQL. Plan: compute
observed pair counts here, generate the null in Python (e.g. `networkx`/`numpy`
shuffles), then a paper's novelty = the 10th-percentile pair z-score,
conventionality = the median. Logged as a Python follow-up, not a view.

## 6. Sleeping-beauty (annual citation series → B coefficient)

The series is SQL; the coefficient is a per-series reduction. Build the histogram:

```sql
SELECT cited.id AS work_id,
       citing.publication_year - cited.publication_year AS years_after,
       count(*) AS cites
FROM lake.openalex.work_references r
JOIN lake.openalex.works citing ON citing.id = r.work_id
JOIN lake.openalex.works cited  ON cited.id  = r.referenced_work_id
WHERE citing.publication_year >= cited.publication_year
GROUP BY 1, 2;
```

Then `B = Σ_{t=0..tm} [ ((c_tm − c_0)/tm)·t + c_0 − c_t ] / max(1, c_t)`, where
`tm` is the year of peak citations `c_tm`. Computable per series with array
aggregation/window functions, but fiddly and edge-undercounted (caveat 1) —
sketch only; revisit if delayed-recognition is a real use case.

## 7. Team size & institution count (with the floor caveat)

```sql
SELECT work_id,
       count(DISTINCT author_id)      AS team_size_floor,   -- excludes unaffiliated authors
       count(DISTINCT institution_id) AS institution_count,
       count(DISTINCT institution_country) AS country_count
FROM lake.openalex.works_authorships
GROUP BY work_id;
```

`team_size_floor` undercounts where authors lack an affiliation (caveat 2). When
the full-authorship table lands, recompute from it; meanwhile label the column
honestly.

## 8. Author h-index (global counts)

```sql
WITH author_paper AS (
    SELECT a.author_id, w.cited_by_count
    FROM lake.openalex.works_authorships a
    JOIN lake.openalex.works w ON w.id = a.work_id
),
ranked AS (
    SELECT author_id, cited_by_count,
           row_number() OVER (PARTITION BY author_id ORDER BY cited_by_count DESC) AS rnk
    FROM author_paper
)
SELECT author_id, max(rnk) FILTER (WHERE cited_by_count >= rnk) AS h_index
FROM ranked GROUP BY author_id;
```

Inherits OpenAlex author disambiguation (its quality is the ceiling). The same
shape over `institution_id` gives an institutional h-index — the peer-center
benchmarking primitive.

---

## Suggested next steps

- **Tier by cost/value.** cf (#2), hit-papers (#3), team/institution (#7), h-index
  (#8) are cheap, global-count-based, and high-value — natural **first derived
  views**. Disruption (#4) is high-value but needs a scalable plan + a focal
  subset. Novelty (#5) and sleeping-beauty (#6) need Python; defer.
- **Validate on a known slice.** Reproduce a handful of SciSciNet values for
  papers we both cover (join on OpenAlex Work ID / DOI) as an acceptance check
  before trusting our numbers — mirrors their rank-correlation validation.
- **Decide the home** (consumer views vs. a derived `*x` schema) per the
  governance note before promoting any of these.
