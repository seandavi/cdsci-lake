"""Ingest State Cancer Profiles into the ``scp`` schema.

Source = the maintainer's **monthly GitHub releases** of
``seandavi/state-cancer-profile-scraper`` (NOT a live scrape). Each release tag
(e.g. ``2026-06-01``) is the ``snapshot_version``; the release carries one
``.csv.gz`` asset per domain. Releasing-not-scraping gives a stable, versioned,
re-curatable source: the kept ``.csv.gz`` *is* the faithful bronze layer (every
column verbatim), and the silver tables are typed projections of it (ADR-0012).

Four domains -> four lake tables (registry-driven, like reporter's ``Group``):

| asset                                       | table             | key                                  |
|---------------------------------------------|-------------------|--------------------------------------|
| state_cancer_profiles_incidence.csv.gz      | scp.incidence     | (fips, cancer, sex, race, stage, age)|
| state_cancer_profiles_mortality.csv.gz      | scp.mortality     | (fips, cancer, sex, race, stage, age)|
| state_cancer_profiles_risk.csv.gz           | scp.risk          | (fips, risk, race, sex, datatype)    |
| state_cancer_profiles_demographics.csv.gz   | scp.demographics  | (area_code, topic, demo, race, sex, age)|

Projection rules (see docs/design/scp.md):

* Read all-varchar (``sample_size=-1``, ``union_by_name``, ``ignore_errors``).
* DROP ``_extracted_at`` from silver — a per-scrape timestamp would make every
  row "change" each month and break MERGE idempotency.
* DROP the year-prefixed ``2023_rural_urban_continuum_codesrural_urban_note``
  column — its name drifts annually -> schema instability (bronze keeps it).
* Explicit, stable projections (never ``SELECT *``) so the silver schema is fixed
  and monthly MERGEs stay clean.
* incidence/mortality/risk are tidy (one value per row): ``TRY_CAST`` numerics to
  DOUBLE, key/dimension cols kept as text.
* demographics stays WIDE, per-column typed: the measure columns are
  heterogeneous (percent, dollars, counts, index) and ``persistent_poverty`` is
  categorical yes/no, so tidying would force VARCHAR. ``TRY_CAST`` the numeric
  measures to DOUBLE (sentinels like "data not available" -> NULL) and keep
  ``persistent_poverty`` as VARCHAR. A sparse wide table (~44 cols, mostly NULL
  per row) is expected and correct.
* Every table carries ``'<release_tag>' AS snapshot_version``.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import duckdb

from ... import ops
from ...config import Settings, get_settings
from ...connect import LAKE, csv_source, lake_connect, raw_dir, upsert
from ...download import download, get_json

_RAW = "scp"  # raw-download subdir name


def _sql_str(value: str) -> str:
    """Quote a value as a SQL string literal (tags/paths are trusted, but safe)."""
    return "'" + value.replace("'", "''") + "'"


@dataclass(frozen=True)
class Domain:
    """One release asset -> one lake table (a typed silver projection of it)."""

    table: str  # lake table name (schema-qualified at write time)
    asset: str  # release asset file name (the .csv.gz)
    key: tuple[str, ...]  # MERGE key (output column names)
    projection: str  # typed SELECT body over the raw all-varchar CSV


# incidence / mortality share the cancer-rate shape (tidy: one rate per row).
# 2023_rural_urban_continuum_codesrural_urban_note and _extracted_at are dropped.
_INCIDENCE = Domain(
    table="incidence",
    asset="state_cancer_profiles_incidence.csv.gz",
    key=("fips", "cancer", "sex", "race", "stage", "age"),
    projection="""
        reported_locale                                       AS reported_locale,
        fips                                                  AS fips,
        state_fips                                            AS state_fips,
        state                                                 AS state,
        areatype                                              AS areatype,
        locale                                                AS locale,
        locale_type                                           AS locale_type,
        cancer                                                AS cancer,
        sex                                                   AS sex,
        race                                                  AS race,
        stage                                                 AS stage,
        age                                                   AS age,
        year                                                  AS year,
        measurement                                           AS measurement,
        TRY_CAST(age_adjusted_rate_per_100_000 AS DOUBLE)     AS age_adjusted_rate,
        TRY_CAST(lower_ci_rate AS DOUBLE)                     AS lower_ci_rate,
        TRY_CAST(upper_ci_rate AS DOUBLE)                     AS upper_ci_rate,
        TRY_CAST(ci_rank AS DOUBLE)                           AS ci_rank,
        TRY_CAST(lower_ci_rank AS DOUBLE)                     AS lower_ci_rank,
        TRY_CAST(upper_ci_rank AS DOUBLE)                     AS upper_ci_rank,
        TRY_CAST(average_annual_count AS DOUBLE)              AS average_annual_count,
        recent_trend                                          AS recent_trend,
        TRY_CAST(recent_5_year_trend_in_rate AS DOUBLE)       AS recent_5yr_trend,
        TRY_CAST(lower_ci_trend_in_rate AS DOUBLE)            AS lower_ci_trend,
        TRY_CAST(upper_ci_trend_in_rate AS DOUBLE)            AS upper_ci_trend,
        TRY_CAST(percent_of_cases_with_late_stage AS DOUBLE)  AS percent_late_stage,
        url                                                   AS url
    """,
)

_MORTALITY = Domain(
    table="mortality",
    asset="state_cancer_profiles_mortality.csv.gz",
    key=("fips", "cancer", "sex", "race", "stage", "age"),
    projection="""
        reported_locale                                   AS reported_locale,
        fips                                              AS fips,
        state_fips                                        AS state_fips,
        state                                             AS state,
        areatype                                          AS areatype,
        locale                                            AS locale,
        locale_type                                       AS locale_type,
        cancer                                            AS cancer,
        sex                                               AS sex,
        race                                              AS race,
        stage                                             AS stage,
        age                                               AS age,
        year                                              AS year,
        measurement                                       AS measurement,
        TRY_CAST(age_adjusted_rate_per_100_000 AS DOUBLE) AS age_adjusted_rate,
        TRY_CAST(lower_ci_rate AS DOUBLE)                 AS lower_ci_rate,
        TRY_CAST(upper_ci_rate AS DOUBLE)                 AS upper_ci_rate,
        TRY_CAST(ci_rank AS DOUBLE)                       AS ci_rank,
        TRY_CAST(lower_ci_rank AS DOUBLE)                 AS lower_ci_rank,
        TRY_CAST(upper_ci_rank AS DOUBLE)                 AS upper_ci_rank,
        TRY_CAST(average_annual_count AS DOUBLE)          AS average_annual_count,
        recent_trend                                      AS recent_trend,
        TRY_CAST(recent_5_year_trend_in_rate AS DOUBLE)   AS recent_5yr_trend,
        TRY_CAST(lower_ci_trend_in_rate AS DOUBLE)        AS lower_ci_trend,
        TRY_CAST(upper_ci_trend_in_rate AS DOUBLE)        AS upper_ci_trend,
        url                                               AS url
    """,
)

_RISK = Domain(
    table="risk",
    asset="state_cancer_profiles_risk.csv.gz",
    key=("fips", "risk", "race", "sex", "datatype"),
    projection="""
        reported_locale                          AS reported_locale,
        fips                                     AS fips,
        state_fips                               AS state_fips,
        statefips_query                          AS statefips_query,
        locale_type                              AS locale_type,
        topic                                    AS topic,
        topic_label                              AS topic_label,
        risk                                     AS risk,
        risk_label                               AS risk_label,
        race                                     AS race,
        sex                                      AS sex,
        datatype                                 AS datatype,
        TRY_CAST(percent AS DOUBLE)              AS percent,
        TRY_CAST(lower_ci_percent AS DOUBLE)     AS lower_ci_percent,
        TRY_CAST(upper_ci_percent AS DOUBLE)     AS upper_ci_percent,
        TRY_CAST(respondents AS DOUBLE)          AS respondents,
        TRY_CAST(model_based_percent3 AS DOUBLE) AS model_based_percent,
        url                                      AS url
    """,
)

# demographics stays WIDE: heterogeneous measures + a categorical column.
# Odd identifiers are quoted; numeric measures TRY_CAST to DOUBLE; the
# categorical persistent_poverty stays VARCHAR.
_DEMOGRAPHICS = Domain(
    table="demographics",
    asset="state_cancer_profiles_demographics.csv.gz",
    key=("area_code", "topic", "demo", "race", "sex", "age"),
    projection="""
        reported_locale                                                AS reported_locale,
        area_code                                                      AS area_code,
        state_fips                                                     AS state_fips,
        areatype                                                       AS areatype,
        locale_type                                                    AS locale_type,
        topic                                                          AS topic,
        topic_label                                                    AS topic_label,
        demo                                                           AS demo,
        demo_label                                                     AS demo_label,
        race                                                           AS race,
        sex                                                            AS sex,
        age                                                            AS age,
        TRY_CAST(rank AS DOUBLE)                                       AS rank,
        TRY_CAST(percent AS DOUBLE)                                    AS percent,
        TRY_CAST("households_with_>1_person_per_room" AS DOUBLE)       AS households_gt1_person_per_room,
        TRY_CAST("people_education:_less_than_9th_grade" AS DOUBLE)    AS people_education_lt_9th_grade,
        TRY_CAST("people_education:_at_least_bachelor's_degree" AS DOUBLE) AS people_education_bachelors_plus,
        TRY_CAST(people_with_limited_access AS DOUBLE)                 AS people_with_limited_access,
        TRY_CAST(value_dollars AS DOUBLE)                             AS value_dollars,
        TRY_CAST(people_insured AS DOUBLE)                            AS people_insured,
        TRY_CAST(people_uninsured AS DOUBLE)                          AS people_uninsured,
        TRY_CAST(people_age_under_18 AS DOUBLE)                       AS people_age_under_18,
        TRY_CAST(people_age_40_and_over AS DOUBLE)                    AS people_age_40_and_over,
        TRY_CAST(people_age_50_and_over AS DOUBLE)                    AS people_age_50_and_over,
        TRY_CAST(people_age_65_and_over AS DOUBLE)                    AS people_age_65_and_over,
        TRY_CAST(people_age_18_39 AS DOUBLE)                          AS people_age_18_39,
        TRY_CAST(people_age_40_64 AS DOUBLE)                          AS people_age_40_64,
        TRY_CAST(people_foreign_born AS DOUBLE)                       AS people_foreign_born,
        TRY_CAST(people_black AS DOUBLE)                              AS people_black,
        TRY_CAST("people_ai/an" AS DOUBLE)                           AS people_ai_an,
        TRY_CAST(people_api AS DOUBLE)                                AS people_api,
        TRY_CAST(people_hispanic AS DOUBLE)                          AS people_hispanic,
        TRY_CAST(people_white AS DOUBLE)                             AS people_white,
        TRY_CAST(people_non_hispanic_origin_recode AS DOUBLE)        AS people_non_hispanic_origin_recode,
        TRY_CAST(people_male AS DOUBLE)                              AS people_male,
        TRY_CAST(people_female AS DOUBLE)                            AS people_female,
        TRY_CAST(families_below_poverty AS DOUBLE)                   AS families_below_poverty,
        persistent_poverty                                           AS persistent_poverty,
        TRY_CAST(people_below_poverty AS DOUBLE)                     AS people_below_poverty,
        TRY_CAST("people_<150pct_of_poverty" AS DOUBLE)             AS people_lt_150pct_poverty,
        TRY_CAST(value_index AS DOUBLE)                             AS value_index,
        url                                                          AS url
    """,
)

DOMAINS: dict[str, Domain] = {
    d.table: d for d in (_INCIDENCE, _MORTALITY, _RISK, _DEMOGRAPHICS)
}


def resolve_latest(settings: Settings | None = None) -> dict:
    """Return ``{tag, assets}`` for the latest State Cancer Profiles release.

    ``tag`` is the release tag (the ``snapshot_version``); ``assets`` is the
    GitHub asset list (each has ``name``/``browser_download_url``/``size``).
    """
    s = settings or get_settings()
    rel = get_json(s.scp_releases_api)
    if not isinstance(rel, dict) or "tag_name" not in rel:
        raise RuntimeError(f"No release found at {s.scp_releases_api}")
    return {"tag": rel["tag_name"], "assets": rel.get("assets", [])}


def download_assets(
    tag: str | None = None, settings: Settings | None = None
) -> tuple[str, dict[str, Path]]:
    """Download the latest (or named) release's domain ``.csv.gz`` assets.

    Returns ``(tag, {table: csv_gz_path})``. The kept ``.csv.gz`` is the bronze
    layer; re-running does not re-download (see :func:`cdsci.lake.download`).
    """
    s = settings or get_settings()
    latest = resolve_latest(s)
    tag = tag or latest["tag"]
    by_name = {a.get("name"): a for a in latest["assets"]}
    root = raw_dir(_RAW, s)
    paths: dict[str, Path] = {}
    for table, dom in DOMAINS.items():
        asset = by_name.get(dom.asset)
        if not asset:
            raise RuntimeError(f"Release {tag} has no asset {dom.asset!r}")
        dest = root / f"{tag}-{dom.asset}"
        paths[table] = download(asset["browser_download_url"], dest)
    return tag, paths


def _select_sql(domain: str, csv_path: Path | str, tag: str, limit: int | None) -> str:
    """Typed silver projection over a domain's all-varchar ``.csv.gz`` (DuckDB
    auto-decompresses by extension). Sentinels like "data not available" become
    NULL via ``TRY_CAST``; the release ``tag`` is stamped on every row.
    """
    dom = DOMAINS[domain]
    limit_sql = f" LIMIT {int(limit)}" if limit else ""
    return f"""
        SELECT
            {dom.projection},
            CAST({_sql_str(tag)} AS VARCHAR) AS snapshot_version
        FROM read_csv(
            {csv_source(str(csv_path) if isinstance(csv_path, Path) else csv_path)},
            header = true, all_varchar = true, sample_size = -1,
            union_by_name = true, ignore_errors = true
        ){limit_sql}
    """


def curate(
    con: duckdb.DuckDBPyConnection,
    domain: str,
    csv_path: Path | str,
    tag: str,
    *,
    target: str | None = None,
    limit: int | None = None,
) -> int:
    """Upsert a domain's ``.csv.gz`` into its lake table on the domain's key."""
    dom = DOMAINS[domain]
    target = target or f"{LAKE}.scp.{dom.table}"
    return upsert(
        con, target, _select_sql(domain, csv_path, tag, limit),
        key=list(dom.key), exclude_change_cols=["snapshot_version"],
    )


def ingest(
    *,
    files: dict[str, str] | None = None,
    tag: str | None = None,
    schema: str = "scp",
    limit: int | None = None,
    settings: Settings | None = None,
) -> dict:
    """End-to-end: (download release assets unless ``files`` given) -> upsert all
    four domains -> return a summary.

    ``files`` maps ``{domain: local_csv_gz}`` to load offline (a domain absent
    from the map is skipped); ``tag`` overrides the snapshot label.
    """
    s = settings or get_settings()
    if files:
        tag = tag or "local"
        paths: dict[str, Path] = {d: Path(p) for d, p in files.items()}
    else:
        tag, paths = download_assets(tag, s)

    con = lake_connect(s)
    try:
        counts: dict[str, int] = {}
        with ops.run(con, source="scp", target=f"{LAKE}.{schema}", version=tag) as r:
            for domain, path in paths.items():
                if domain not in DOMAINS:
                    raise ValueError(f"Unknown domain {domain!r}; choose from {sorted(DOMAINS)}")
                target = f"{LAKE}.{schema}.{DOMAINS[domain].table}"
                counts[domain] = curate(con, domain, path, tag, target=target, limit=limit)
            r.rows = sum(counts.values())
    finally:
        con.close()
    return {**r.summary(), "schema": schema, "tag": tag, "counts": counts}
