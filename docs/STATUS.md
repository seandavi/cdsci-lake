# cdsci-lake — status / handoff

_Snapshot of where the platform stands after the extraction + importer build._

## What this repo is

The shared research-data lake platform (ADR-0001): a **read client** + **bulk
source ingestors** publishing into the existing DuckLake (Postgres `lake` catalog
+ R2 `cdsci-lake`), a sibling publisher to omicidx. Consumers `pip install
cdsci-lake` and `lake_connect(read_only=True)`.

## Done

- **Extracted** from `cu-research-intelligence` (the `cri` package) into this repo,
  history preserved (git-filter-repo). Renamed to the PEP 420 namespace package
  **`cdsci.lake`** (`lake.py`→`connect.py`; read client exported at top level).
  Base deps = read client (duckdb + pydantic-settings); `[ingest]` extra = the
  importers. `cu-research-intelligence` had `cri` removed and is now a pure consumer.
- **RePORTER importer — all 4 modern CSV groups** (`projects`, `abstracts`,
  `publications`, `publink`) via a group registry, each MERGE-upserting into
  `reporter.<table>` on its natural key. `years=None` loads all years (union_by_name
  tolerates decades of column drift). CLI: `groups` / `years` / `run -g <group>
  --schema`.
- **iCite importer** — MERGE-upsert on `pmid` into `icite.metadata` from the monthly
  figshare snapshot (drops the huge `cited_by`/`references` list columns).
- **ClinicalTrials.gov importer** — full v2-API JSON (the flat CSV is lossy). Stream
  paginated API → NDJSON → bronze Parquet (`nct_id, record`) → curate `ctgov.studies`
  (typed + full record JSON, key `nct_id`) and `ctgov.references` (`nct_id`↔`pmid`
  crosswalk). MERGE-upsert both.
- **MERGE-upsert** (`cdsci.lake.upsert`) — keyed, change-detecting (updates only on
  real diffs), idempotent (no-op re-run adds no snapshot) → meaningful time-travel.
- **DuckLake maintenance** (`cdsci.lake.maintenance`) — expire snapshots → cleanup
  unused files (+ compact / vacuum), `dry_run` default; loud warning that expiry is
  catalog-global.
- **Docs**: ADR-0001 (platform charter), `docs/design/reporter-icite-mapping.md`
  (empirically-verified source mapping). **Tests**: 8 offline pass; ruff clean.

## Live lake state — PROMOTED to production source schemas

`_dev` is **developer scratch only** (never a staging pipeline); production loads
go straight to the source schemas. Promoted (full history):

| table | rows | note |
|-------|------|------|
| `lake.reporter.projects` | 2,951,294 | all project-years 1985–2025 |
| `lake.reporter.publink`  | 7,571,393 | grants↔PMID crosswalk 1980–2025 |
| `lake.reporter.publications` | 3,050,141 | all years |
| `lake.reporter.abstracts`    | 2,558,580 | all years |
| `lake.icite.metadata`    | 40,588,073 | iCite full snapshot 2026-05 |
| `lake.ctgov.studies`     | 590,635 | full JSON record kept (~16.8 KB/study) |
| `lake.ctgov.references`  | 1,057,838 | nct↔pmid crosswalk |

**Trial↔grant↔literature triangle verified:** 70,376 trials link to 92,470 NIH
grants via 145,811 shared publications (`ctgov.references` ⋈ `reporter.publink`);
trial RESULT pubs average RCR 18.9 (`ctgov.references` ⋈ `icite.metadata`).

Cross-source chain verified against the **production** schemas — all-time NCI P30
grant → `reporter.publink` → `icite.metadata` RCR:

```
P30CA008748 (MSKCC)       23,804 pubs   avg RCR 3.40
P30CA016672 (MD Anderson) 20,369 pubs   avg RCR 2.73
…
```

(and grant → publink → `omicidx.pubmed_article` for titles). This is the peer-
benchmarking primitive (ADR-0021) working across three sources + omicidx.

## Next steps (not yet done)

1. **CRISP (1970–2009, XML)** — the 2 historical RePORTER groups need an XML
   stream-parse path (design doc §1.7–1.8). Not implemented.
3. **`ref.id_crosswalk`** — the cross-source ID table (PMID↔DOI↔PMCID↔core_project_num↔
   NCT). Note: `publink.pmid` is BIGINT but `omicidx.pubmed_article.pmid` is VARCHAR —
   normalize types here.
4. **`lake_ops` metadata model** (ADR-0001 §6) — source/version/run/watermark/contract
   tables in Postgres; wire ingestors to record runs + snapshot ids.
5. **Scoped roles** — replace the admin bootstrap credential with `lake_writer`
   (ingest) and `lake_reader` (consumers) Postgres roles + scoped R2 tokens.
6. **Consumer migration** — point `cancer_center` enrichment at `lake.icite.metadata`
   (RCR) instead of the per-project caches.
7. **Repo remote** — this repo has local history only; create the GitHub repo and push
   (`git remote add origin … && git push -u origin <branch>`).

## Open versioned views / contract

Per-source **views** (`icite.v_rcr`, etc.) as the stable consumer contract are not
yet created — consumers currently read tables directly. Add views + a
`dataset_contract` registry before multiple consumers depend on column names.
