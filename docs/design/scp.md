# State Cancer Profiles (`scp`) source

State Cancer Profiles (statecancerprofiles.cancer.gov) is the NCI/CDC joint
portal of county- and state-level cancer **burden** (incidence, mortality),
**risk factors / screening**, and **socio-economic demographics**. The `scp`
source publishes four lake tables from it.

## Source = monthly GitHub releases, not a live scrape

We do **not** scrape the portal. We consume the maintainer's monthly GitHub
*releases* of [`seandavi/state-cancer-profile-scraper`][repo]. Each release tag
(e.g. `2026-06-01`) is the `snapshot_version`, and the release carries one
`.csv.gz` asset per domain.

Why releases, not a scrape:

- **Versioned & reproducible.** A release tag pins an exact snapshot; the same
  ingest re-runs deterministically and time-travel is meaningful month over month.
- **Re-curatable bronze.** The downloaded `.csv.gz` is kept verbatim in the raw
  layer — every column exactly as scraped. The lake's silver tables are typed
  projections of it (ADR-0012 medallion); a re-curate needs no re-fetch.
- **Decoupled from portal fragility.** The scraper (its rate-limiting, retries,
  page-shape handling) is someone else's concern, run once a month, not on our
  ingest path.

The latest release is resolved via the GitHub API
(`/repos/{repo}/releases/latest`); `Settings.scp_repo` (default
`seandavi/state-cancer-profile-scraper`) is the only knob.

[repo]: https://github.com/seandavi/state-cancer-profile-scraper

## Four domains → four tables (`scp` schema)

Registry-driven (a frozen `Domain` dataclass, like reporter's `Group`): asset →
table → MERGE key.

| asset (`.csv.gz`)                          | table             | MERGE key                                   | shape |
|--------------------------------------------|-------------------|---------------------------------------------|-------|
| `state_cancer_profiles_incidence`          | `scp.incidence`   | `(fips, cancer, sex, race, stage, age)`     | tidy  |
| `state_cancer_profiles_mortality`          | `scp.mortality`   | `(fips, cancer, sex, race, stage, age)`     | tidy  |
| `state_cancer_profiles_risk`               | `scp.risk`        | `(fips, risk, race, sex, datatype)`         | tidy  |
| `state_cancer_profiles_demographics`       | `scp.demographics`| `(area_code, topic, demo, race, sex, age)`  | WIDE  |

Each table carries `snapshot_version` (the release tag) on every row. MERGE-upsert
on the key makes a monthly refresh record only real deltas.

## Projection rules

Read every column as text
(`read_csv(..., header=true, all_varchar=true, sample_size=-1, union_by_name=true, ignore_errors=true)`;
DuckDB auto-decompresses by the `.gz` extension), then cast in SQL with explicit,
**stable projections** (never `SELECT *`) so the silver schema is fixed and the
monthly MERGE stays clean.

Two columns are deliberately **dropped from silver** (kept in the bronze
`.csv.gz`):

- **`_extracted_at`** — a per-scrape timestamp. If kept, every row would "change"
  each month and break MERGE idempotency (a full rewrite each snapshot).
- **`2023_rural_urban_continuum_codesrural_urban_note`** — the year prefix drifts
  annually, so the *column name* is unstable. Dropping it keeps the silver schema
  fixed across snapshots. (It needs quoting if ever referenced.)

`incidence` / `mortality` / `risk` are **tidy** (one value per row): the dimension
columns are kept as text and the rate/percent + CIs + trend columns are
`TRY_CAST` to `DOUBLE`.

### Why `demographics` stays WIDE

`demographics` is kept **wide, per-column typed** rather than tidied to a single
value column. Tidying would force the value column to `VARCHAR`, because the
measures are heterogeneous:

- percents, dollar values (`value_dollars`), people-counts (`people_*`), an index
  (`value_index`) — different units;
- `persistent_poverty` is **categorical** `yes`/`no` (not numeric);
- sentinels like `"data not available"` appear in measure cells.

So the projection keeps the dimension columns + **every** measure column,
`TRY_CAST`s each numeric measure to `DOUBLE` (the sentinels and blanks become
`NULL` cleanly), and keeps `persistent_poverty` as `VARCHAR`. The result is a
sparse wide table (~44 columns, most `NULL` for any given row, because each
`topic`/`demo` populates only its own measure) — that sparsity is expected and
correct. Odd identifiers are quoted in the projection, e.g.
`"people_education:_less_than_9th_grade"`, `"households_with_>1_person_per_room"`,
`"people_<150pct_of_poverty"`, `"people_ai/an"`.

## Geo-key reconciliation (a future `ref` concern)

`scp` keys geography by **FIPS** (`fips` / `area_code` / `state_fips`) and carries
state *names* (in `reported_locale` / `state`). RePORTER keys organizations by the
**2-letter `org_state`**. A burden-vs-funding join across the two therefore needs
a crosswalk (state-name ↔ FIPS ↔ 2-letter abbrev). A join on state name/abbrev is
fine for a demonstration; a durable `ref` crosswalk (FIPS ↔ state abbrev, and
county FIPS for sub-state work) belongs in the planned `ref` schema, not in this
source.

## CLI

```
python -m cdsci.lake.sources.scp latest          # latest release tag + domain assets
python -m cdsci.lake.sources.scp run --schema scp # download latest → MERGE all 4 domains
python -m cdsci.lake.sources.scp run --file risk=/tmp/risk.csv.gz   # offline, one domain
```
