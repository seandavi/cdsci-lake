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
- **MERGE-upsert** (`cdsci.lake.upsert`) — keyed, change-detecting (updates only on
  real diffs), idempotent (no-op re-run adds no snapshot) → meaningful time-travel.
- **DuckLake maintenance** (`cdsci.lake.maintenance`) — expire snapshots → cleanup
  unused files (+ compact / vacuum), `dry_run` default; loud warning that expiry is
  catalog-global.
- **Docs**: ADR-0001 (platform charter), `docs/design/reporter-icite-mapping.md`
  (empirically-verified source mapping). **Tests**: 8 offline pass; ruff clean.

## Live lake state (verified end-to-end)

Loaded into the **`_dev`** schema on the shared lake (staging before promotion):

| table | rows | note |
|-------|------|------|
| `lake._dev.projects` | 159,309 | RePORTER projects FY2024–2025 |
| `lake._dev.publink`  | 597,691 | grants↔PMID crosswalk 2024–2025 |
| `lake._dev.metadata` | _(loading)_ | iCite — full ~40M upsert running in background |

Cross-source crosswalk proven live: NCI grants → `publink` → `omicidx.pubmed_article`
returns the expected top cancer-center grants (P30CA008748 MSKCC, P30CA016672 MD
Anderson, …) and resolves grant→PMID→pubmed titles in one query.

## In progress / on return

- **iCite full load** is running in the background into `lake._dev.metadata`
  (download → unzip → 40M-row upsert). Check `logs/icite_pipeline.log` in the
  cu-research-intelligence repo. If it didn't finish, re-run:
  `CU_OPENALEX_LAKE_BACKEND=postgres python -m cdsci.lake.sources.icite run --schema _dev`.

## Next steps (not yet done)

1. **Promote `_dev` → source schemas** once reviewed: load all years and write to
   `reporter` / `icite` (drop the `--schema _dev`). E.g. full history:
   `... reporter run -g publink --schema reporter` (all years), same for projects/
   abstracts/publications; `... icite run --schema icite`.
2. **CRISP (1970–2009, XML)** — the 2 historical RePORTER groups need an XML
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
