# Reliance on Science (`reliance`) source

**Reliance on Science** (Matt Marx, Cornell) links **patents to the scientific
papers they cite** (and same-team paper/patent pairs), resolved to **OpenAlex Work
IDs**. It is the translational/economic-impact axis: *which research is relied upon
by patented invention*, complementing our citation-impact metrics (RCR, disruption).

## ⚠️ License — CC BY-NC 4.0 (read first)

This is the **only non-open source in the lake**. It is **CC BY-NC 4.0**
(Attribution–**NonCommercial**).

- **Permitted:** internal, **non-commercial** research use. As a state university,
  our analytical use qualifies.
- **Required:** **attribution** — Marx, M., *Reliance on Science*, Zenodo
  (pinned record below).
- **Prohibited:** **redistribution.** Do **not** publish this data or any extract,
  view, or derived table that contains it outside the institution, and do not use
  it for commercial purposes. It lives only in the private lake (Postgres catalog +
  private R2); it must never appear in a public/redistributable surface.

**Carry-forward mechanism:** the license is recorded machine-readably in the source
registry — `SELECT license FROM ops.lake_ops.source WHERE name='reliance'` →
`cc-by-nc-4.0`. Consumers should check `license` before exporting anything, and any
export tooling should refuse to include `reliance.*` (and any join output derived
from it) in shared artifacts. Also summarized in `docs/data-licenses.md`.

## Source = a pinned Zenodo record

Versioned annually on Zenodo (concept DOI `10.5281/zenodo.3236339`); we pin the
**2024 edition, record `11461587`** (`Settings.reliance_zenodo_record`) for
reproducibility. `snapshot_version` defaults to `zenodo-<record>`. Two CSV files:

| file | → table | what |
|------|---------|------|
| `_pcs_oa.csv` (~2.46 GB) | `reliance.patent_citations` | patent→paper **citations** |
| `_patent_paper_pairs.csv` (~22 MB) | `reliance.patent_paper_pairs` | same-team paper/patent **matches** |

## Tables (`reliance` schema)

### `reliance.patent_citations` — key `(patent, work_id, reftype, wherefound)`

| column | type | note |
|--------|------|------|
| `patent` | VARCHAR | USPTO patent id (lowercased, e.g. `us-11426570-b2`) |
| `work_id` | VARCHAR | **`W`-form OpenAlex Work ID** (from `oaid`) → joins `openalex.works.id` |
| `oaid` | BIGINT | raw OpenAlex numeric id |
| `reftype` | VARCHAR | `app` (applicant) / `exam` (examiner-added) |
| `wherefound` | VARCHAR | `frontonly` / `bodyonly` / `both` |
| `confscore` | INTEGER | match confidence 1–10 |
| `self_cite` | BOOLEAN | patent and paper share an author |
| `uspto` | BOOLEAN | USPTO patent |
| `snapshot_version` | VARCHAR | excluded from change-detection |

### `reliance.patent_paper_pairs` — key `(work_id, patent)`

| column | type | note |
|--------|------|------|
| `work_id` | VARCHAR | OpenAlex Work ID (already `W`-form) |
| `patent` | VARCHAR | USPTO patent id (uppercased) |
| `ppp_score` | INTEGER | pairing confidence |
| `days_paper_to_patent` | INTEGER | days between paper and patent filing (can be negative) |
| `all_patents_for_paper` | VARCHAR | other patents matched to the same paper |
| `snapshot_version` | VARCHAR | |

Both curate read all-varchar, normalize the OpenAlex id to `W`-form, and **GROUP BY
the natural key** so the staged rows are unique (the citations file repeats a paper
for front/body/applicant/examiner variants); MERGE-upsert keeps reloads to deltas.

## Loading

`python -m cdsci.lake.sources.reliance run` loads both files; `--dataset
citations|pairs` loads one, `--file` a local CSV, `--limit` a smoke cap. Run via
`ops.run`. The big `_pcs_oa.csv` (~2.46 GB, tens of millions of rows) is a long
single MERGE — run it in the background.

## Why it's useful

`work_id` joins straight to `openalex.works` (once loaded), so a center's papers →
`reliance.patent_citations` answers **"which of our research is cited by patents,
and how much"** — a translational benchmarking signal distinct from academic
citations. The pairs table adds **"our researchers who also patent."** DOI/PMID
join paths to `icite`/`publink` are direct on the shared id — normalize DOI
(lowercase, unprefixed) and `pmid` type at the join site.

## Open items

- **Depends on OpenAlex** for the richest joins (`work_id` → `openalex.works`);
  usable standalone meanwhile (patent counts per Work ID).
- **License enforcement** is documentary + the `ops` registry flag today; an
  export guard that refuses `reliance.*` belongs with the consumer tooling.
