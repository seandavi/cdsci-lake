# NIH RePORTER + iCite import mapping

Data-source mapping that drives the `reporter` and `icite` importers. All schemas
verified empirically against live endpoints (RePORTER `allFilesInfo` /
`DownloadFromDocService`, iCite figshare snapshot `2026-05`, iCite API), 2026-06-22.

## 1. NIH RePORTER ExPORTER

### API mechanics (no auth)

- **List a group's files:** `POST https://reporter.nih.gov/services/exporter/allFilesInfo`
  body `{"file_group":"<GROUP>"}` → array of
  `{file_id, display_name, fy, to_fy, file_name, created_date, file_size, file_type, file_group, doc_type_code, doc_key_id}`.
  Invalid group → HTTP 400 empty body; valid-but-empty → `[]`. `file_size` is a
  human string (`"~60MB"`), not bytes.
- **Download a file:** `GET .../services/exporter/DownloadFromDocService?DocType=<doc_type_code>&KeyId=<doc_key_id>`
  (follow redirects) → `.zip` with one data file. `doc_key_id` = FY for single-year files.

### Valid file groups (6; PATENT→400, CLINICAL_STUDY/CRISP-bare→empty)

| `file_group`     | `doc_type_code`        | files | FY range  | format | lake table | key | grain |
|------------------|------------------------|-------|-----------|--------|------------|-----|-------|
| `PROJECT`        | `EXPPRJ` (+1 `EXPFND`) | 42    | 1985–2025 | CSV    | `reporter.projects`       | `APPLICATION_ID` | project-year |
| `ABSTRACT`       | `EXPABS`               | 41    | 1985–2025 | CSV    | `reporter.abstracts`      | `APPLICATION_ID` | project |
| `PUBLICATION`    | `EXPPUB`               | 46    | 1980–2025 | CSV    | `reporter.publications`   | `PMID` | publication |
| `LINK` ⭐        | `EXPPL`                | 46    | 1980–2025 | CSV    | `reporter.publink`        | `(PMID, PROJECT_NUMBER)` | grant↔pub edge |
| `CRISP_PROJECT`  | `EXPCPX` full / `EXPCPC` delta | 80 | 1970–2009 | **XML** | `reporter.crisp_projects` | `APPLICATION_ID` | project-year |
| `CRISP_ABSTRACT` | `EXPCAX` full / `EXPCAC` delta | 76 | 1972–2009 | **XML** | `reporter.crisp_abstracts`| `APPLICATION_ID` | project |

Modern groups are CSV; **CRISP (1970–2009) is XML** (`<PROJECTS><row>…`), use the
`_X_` (full) series not `_C_` deltas. The single `EXPFND` record under PROJECT is
a multi-year funding aggregate (`RePORTER_PRJFUNDING_C_FY1985_FY1999`) — optional,
not part of the per-year loop.

### Column lists

**`reporter.projects`** (PROJECT/EXPPRJ; key `APPLICATION_ID`; latest KeyId 2025):
```
APPLICATION_ID, ACTIVITY, ADMINISTERING_IC, APPLICATION_TYPE, ARRA_FUNDED,
AWARD_NOTICE_DATE, BUDGET_START, BUDGET_END, ASSISTANCE_LISTING_NUMBER,
CORE_PROJECT_NUM, ED_INST_TYPE, "OPPORTUNITY NUMBER", FULL_PROJECT_NUM,
FUNDING_ICs, FUNDING_MECHANISM, FY, IC_NAME, NIH_SPENDING_CATS, ORG_CITY,
ORG_COUNTRY, ORG_DEPT, ORG_DISTRICT, ORG_DUNS, ORG_FIPS, ORG_IPF_CODE, ORG_NAME,
ORG_STATE, ORG_ZIPCODE, PHR, PI_IDS, PI_NAMEs, PROGRAM_OFFICER_NAME,
PROJECT_START, PROJECT_END, PROJECT_TERMS, PROJECT_TITLE, SERIAL_NUMBER,
STUDY_SECTION, STUDY_SECTION_NAME, SUBPROJECT_ID, SUFFIX, SUPPORT_YEAR,
DIRECT_COST_AMT, INDIRECT_COST_AMT, TOTAL_COST, TOTAL_COST_SUB_PROJECT
```
Gotchas: `"OPPORTUNITY NUMBER"` header has an embedded space (was `FOA_NUMBER` in
older years — load with `union_by_name`). `PI_IDS`/`PI_NAMEs`/`FUNDING_ICs`/
`NIH_SPENDING_CATS`/`PROJECT_TERMS` are semicolon-delimited multi-value strings
(contact PI marked `(contact)`). Money fields are strings → cast. Derive "active"
from `PROJECT_END`, not any RePORTER flag.

**`reporter.abstracts`** (ABSTRACT/EXPABS; key `APPLICATION_ID`): `APPLICATION_ID, ABSTRACT_TEXT`.

**`reporter.publications`** (PUBLICATION/EXPPUB; key `PMID`; calendar-year files):
```
AFFILIATION, AUTHOR_LIST, COUNTRY, ISSN, JOURNAL_ISSUE, JOURNAL_TITLE,
JOURNAL_TITLE_ABBR, JOURNAL_VOLUME, LANG, PAGE_NUMBER, PMC_ID, PMID, PUB_DATE,
PUB_TITLE, PUB_YEAR
```

**`reporter.publink`** ⭐ (LINK/EXPPL; key `(PMID, PROJECT_NUMBER)`; ~3 MB/yr): `PMID, PROJECT_NUMBER`.
The grants↔PMID crosswalk. `PROJECT_NUMBER` = **`CORE_PROJECT_NUM`** form (join to
`reporter.projects.CORE_PROJECT_NUM`, not FULL/APPLICATION). `PMID` bridges to
`reporter.publications`, PubMed, and `icite.metadata`. Cheapest, highest-value table.

**CRISP (XML, historical)** — `crisp_projects` 22 elements (APPLICATION_ID, GRANT_NUM,
SUBPROJECT_ID, PROJECT_TITLE, FY, ACTIVITY_CODE, ORG_NAME, PI_{FIRST,MIDDLE,LAST}_NAME,
PROJ_PERIOD_{START,END}_DATE, …); `crisp_abstracts` = APPLICATION_ID, ABSTRACT_TEXT.
Needs an XML stream-parse path (`<PROJECTS><row>`, nulls as `xsi:nil`). Deferred —
see implementation status.

### Implementation notes
Loop per group: `allFilesInfo` → filter to the chosen `doc_type_code` series →
`DownloadFromDocService` per record. Modern CSVs are full-year snapshots: re-download
latest FY periodically and MERGE-upsert; past years are stable (load once). Read with
`read_csv(all_varchar=true, union_by_name=true, ignore_errors=true)`, cast in a typed
projection. Total modern history ≈ a few GB of zips.

## 2. iCite (monthly figshare snapshot, collection 4586573)

Latest verified `2026-05` (article 32676936). Files: `icite_metadata.csv.zip` (13.7 GB),
`icite_metadata.tar.gz` (15.6 GB, JSON), `open_citation_collection.csv.zip` (4.9 GB).
Resolve via `GET api.figshare.com/v2/collections/4586573/articles?page_size=1` →
article → `files[].download_url`.

**`icite.metadata`** (key `pmid`; one row per PMID; 25 columns, exact order):
```
pmid, doi, title, authors, year, journal, is_research_article, citation_count,
field_citation_rate, expected_citations_per_year, citations_per_year,
relative_citation_ratio, nih_percentile, human, animal, molecular_cellular,
x_coord, y_coord, apt, is_clinical, cited_by_clin, cited_by, references,
provisional, last_modified
```
`relative_citation_ratio` = RCR (NIH median 1.0); `nih_percentile`; `apt` =
approximate potential to translate. `cited_by`, `cited_by_clin`, `references` are
**space-delimited PMID lists** (can be huge) — keep as text or explode; for the full
graph prefer the OCC table. Booleans are `True`/`False` text. The bulk CSV (not the
API) is the authoritative snake_case schema; it has `last_modified`/`provisional` the
API lacks.

**`icite.open_citations`** (optional; key `(citing, referenced)`; 4.9 GB; ~2B rows):
`citing, referenced` — both PMIDs, public-domain NIH Open Citation Collection.

## 3. Cross-source crosswalk

```
reporter.projects.CORE_PROJECT_NUM
   └─(1:N)─ reporter.publink (PMID, PROJECT_NUMBER)      PROJECT_NUMBER = CORE_PROJECT_NUM
              └─ PMID ─┬─ reporter.publications.PMID
                       ├─ icite.metadata.pmid            (RCR, citations, apt)
                       └─ icite.open_citations.citing/referenced
```
Attribute iCite bibliometrics to a grant portfolio via `projects → publink → icite`.
Load `reporter.publink` first for crosswalk work.
