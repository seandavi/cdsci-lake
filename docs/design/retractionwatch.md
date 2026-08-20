# Retraction Watch (`retractionwatch`) source

[Retraction Watch](https://retractionwatch.com/) is the reference catalogue of
retractions, corrections, and expressions of concern. Crossref publishes it as a
**single rolling CSV under CC0** at
[`gitlab.com/crossref/retraction-watch-data`](https://gitlab.com/crossref/retraction-watch-data),
refreshed most weekdays. The `retractionwatch` source loads it into one tidy lake
table, `lake.retractionwatch.retractions`.

## Source = one rolling CC0 CSV (the recurring-sync exemplar)

~70k rows, ~65 MB, 20 columns, keyed by `Record ID`. Several fields are
**semicolon-separated** multi-value lists (with a trailing `;`): `Subject`,
`Institution`, `Country`, `Author`, `URLS`, `Reason`. Dates are `M/D/YYYY H:MM`;
`RetractionNature` is a small enum (`Retraction` / `Correction` / `Expression of
concern` / `Reinstatement` / `None`); `Paywalled` is `Yes`/`No`.

Unlike our versioned bulk sources, this is a **single rolling file with no
upstream version**, so (per ADR-0005's "notes on the raw stage") we **keep each
pull's CSV as the bronze copy** and tag the snapshot by **pull date**. It is the
natural first customer for a scheduler (roadmap): small, CC0, weekday-daily.

## One tidy table → `retractionwatch.retractions`

Key: `record_id` (BIGINT). Multi-value fields become `VARCHAR[]` arrays (split on
`;`, trimmed, empties dropped); dates parse to `DATE`; DOIs are normalized
(trimmed, resolver-prefix stripped, lowercased, sentinels → NULL) exactly like
`openalex.works`; PMIDs cast to BIGINT with `0` → NULL.

| column | type | note |
|--------|------|------|
| `record_id` | BIGINT | **key** (Retraction Watch Record ID) |
| `title` | VARCHAR | |
| `subjects` | VARCHAR[] | subject/area tags |
| `institutions` | VARCHAR[] | author affiliations |
| `journal`, `publisher` | VARCHAR | |
| `countries` | VARCHAR[] | |
| `authors` | VARCHAR[] | `Surname, F` forms |
| `urls` | VARCHAR[] | notice URLs |
| `article_type` | VARCHAR | |
| `retraction_date` | DATE | |
| `retraction_doi` | VARCHAR | DOI of the *notice* (normalized) |
| `retraction_pmid` | BIGINT | |
| `original_paper_date` | DATE | |
| `original_paper_doi` | VARCHAR | **join key** to the retracted paper (normalized) |
| `original_paper_pmid` | BIGINT | **join key** (PMID) |
| `retraction_nature` | VARCHAR | Retraction / Correction / Expression of concern / … |
| `reasons` | VARCHAR[] | controlled reason phrases |
| `paywalled` | BOOLEAN | |
| `notes` | VARCHAR | |
| `snapshot_version` | VARCHAR | pull date; excluded from change-detection |

## Loading

`python -m cdsci.lake.sources.retractionwatch run` downloads the CSV (kept in the
raw layer) and MERGE-upserts on `record_id` — the roadmap's **"full-file diff on
Record ID"**: a daily pull rewrites only changed notices and inserts new ones, so
each DuckLake snapshot is the day's delta (ADR-0003). `--file` loads a local CSV;
`--version` overrides the pull-date tag; `--limit` is a smoke cap. The run is
recorded via `ops.run` (ADR-0006).

## Why it's useful

An **integrity flag** for every other source: join `original_paper_doi` /
`original_paper_pmid` to `icite` / `reporter.publink` / `openalex` / omicidx to
mark retracted work in any cohort or benchmarking query (e.g. exclude or surface
retractions in a center's publication set, or in trial-linked literature). It is
DOI/PMID-keyed, so each of those joins is direct on the id both sides already
carry (`ref.id_crosswalk` was retired unused — see `docs/ROADMAP.md`).

## Open items

- **DOI→OpenAlex joins** — by normalized DOI directly against `openalex.works`
  (already lowercased/unprefixed there). Both sides must normalize DOI at the
  join site; there is no central crosswalk to lean on.
- **Scheduler** — this is the intended first recurring sync (weekday-daily) once
  a scheduler lands; `snapshot_version` = pull date already supports daily diffs.
- **`Reason` vocabulary** — kept as free-ish phrases in an array; a normalized
  reason dimension could come later if a use case needs it.
