"""NIH RePORTER ExPORTER importer — all modern (CSV) file groups, all years.

Verified groups (see docs/design/reporter-icite-mapping.md):

| group        | file_group  | doc_type | lake table             | key                       |
|--------------|-------------|----------|------------------------|---------------------------|
| projects     | PROJECT     | EXPPRJ   | reporter.projects      | appl_id                   |
| abstracts    | ABSTRACT    | EXPABS   | reporter.abstracts     | appl_id                   |
| publications | PUBLICATION | EXPPUB   | reporter.publications  | pmid                      |
| publink      | LINK        | EXPPL    | reporter.publink       | (pmid, project_number)    |

Each group is a per-year bulk CSV. The importer lists a group's files
(`allFilesInfo`), downloads the chosen years (`DownloadFromDocService`), and
MERGE-upserts into ``reporter.<table>`` keyed on the group's natural key. Loading
all of a group's years together with ``union_by_name`` tolerates the column drift
across decades (a column absent from one year's file reads as NULL).

`publink` is the grants<->PMID crosswalk: ``project_number`` is the
``core_project_num`` form; ``pmid`` bridges to PubMed and iCite.

CRISP (1970-2009, XML) is a separate path - not yet implemented (see docs).
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import duckdb

from ... import ops
from ...config import Settings, get_settings
from ...connect import LAKE, csv_source, lake_connect, raw_dir, upsert
from ...download import download, post_json, unzip


@dataclass(frozen=True)
class Group:
    """One ExPORTER file group -> one lake table."""

    table: str  # lake table name (schema-qualified at write time)
    file_group: str  # allFilesInfo file_group
    doc_type: str  # doc_type_code series to keep
    key: tuple[str, ...]  # MERGE key (output column names)
    key_filter: str  # WHERE over source columns (drop rows with no key)
    projection: str  # typed SELECT body over the raw all-varchar CSV


_PROJECTS = Group(
    table="projects",
    file_group="PROJECT",
    doc_type="EXPPRJ",
    key=("appl_id",),
    key_filter="TRY_CAST(application_id AS BIGINT) IS NOT NULL",
    projection="""
        TRY_CAST(application_id AS BIGINT)    AS appl_id,
        core_project_num                      AS core_project_num,
        full_project_num                      AS project_num,
        TRY_CAST(fy AS INTEGER)               AS fiscal_year,
        activity                              AS activity_code,
        application_type                      AS application_type,
        administering_ic                      AS admin_ic,
        ic_name                               AS ic_name,
        funding_mechanism                     AS funding_mechanism,
        funding_ics                           AS funding_ics,
        TRY_CAST(total_cost AS DOUBLE)        AS total_cost,
        TRY_CAST(direct_cost_amt AS DOUBLE)   AS direct_cost,
        TRY_CAST(indirect_cost_amt AS DOUBLE) AS indirect_cost,
        project_title                         AS project_title,
        project_start                         AS project_start,
        project_end                           AS project_end,
        budget_start                          AS budget_start,
        budget_end                            AS budget_end,
        org_name                              AS org_name,
        org_city                              AS org_city,
        org_state                             AS org_state,
        org_country                           AS org_country,
        org_duns                              AS org_duns,
        "OPPORTUNITY NUMBER"                  AS foa_number,
        pi_ids                                AS pi_ids,
        pi_names                              AS pi_names,
        program_officer_name                  AS program_officer_name,
        study_section_name                    AS study_section_name,
        nih_spending_cats                     AS nih_spending_cats,
        project_terms                         AS project_terms,
        TRY_CAST(support_year AS INTEGER)     AS support_year,
        subproject_id                         AS subproject_id
    """,
)

_ABSTRACTS = Group(
    table="abstracts",
    file_group="ABSTRACT",
    doc_type="EXPABS",
    key=("appl_id",),
    key_filter="TRY_CAST(application_id AS BIGINT) IS NOT NULL",
    projection="""
        TRY_CAST(application_id AS BIGINT) AS appl_id,
        abstract_text                      AS abstract_text
    """,
)

_PUBLICATIONS = Group(
    table="publications",
    file_group="PUBLICATION",
    doc_type="EXPPUB",
    key=("pmid",),
    key_filter="TRY_CAST(pmid AS BIGINT) IS NOT NULL",
    projection="""
        TRY_CAST(pmid AS BIGINT)        AS pmid,
        pmc_id                          AS pmc_id,
        pub_title                       AS pub_title,
        journal_title                   AS journal_title,
        journal_title_abbr              AS journal_title_abbr,
        issn                            AS issn,
        TRY_CAST(pub_year AS INTEGER)   AS pub_year,
        pub_date                        AS pub_date,
        author_list                     AS author_list,
        affiliation                     AS affiliation,
        country                         AS country,
        lang                            AS lang,
        journal_volume                  AS journal_volume,
        journal_issue                   AS journal_issue,
        page_number                     AS page_number
    """,
)

_PUBLINK = Group(
    table="publink",
    file_group="LINK",
    doc_type="EXPPL",
    key=("pmid", "project_number"),
    key_filter="TRY_CAST(pmid AS BIGINT) IS NOT NULL AND project_number IS NOT NULL",
    projection="""
        TRY_CAST(pmid AS BIGINT) AS pmid,
        project_number           AS project_number
    """,
)

GROUPS: dict[str, Group] = {
    g.table: g for g in (_PROJECTS, _ABSTRACTS, _PUBLICATIONS, _PUBLINK)
}


def list_files(file_group: str, settings: Settings | None = None) -> list[dict]:
    """ExPORTER file catalog for a group (one record per year)."""
    s = settings or get_settings()
    files = post_json(s.exporter_files_api, {"file_group": file_group})
    return files if isinstance(files, list) else []


def available_years(group: str, settings: Settings | None = None) -> list[int]:
    """Fiscal/calendar years available for ``group`` (matching its doc_type)."""
    g = GROUPS[group]
    files = list_files(g.file_group, settings)
    return sorted(
        int(f["fy"]) for f in files if f.get("fy") and f.get("doc_type_code") == g.doc_type
    )


def download_group(
    group: str, years: list[int] | None = None, settings: Settings | None = None
) -> list[Path]:
    """Download + unzip a group's files for ``years`` (all available if None)."""
    s = settings or get_settings()
    g = GROUPS[group]
    by_year = {
        int(f["fy"]): f
        for f in list_files(g.file_group, s)
        if f.get("fy") and f.get("doc_type_code") == g.doc_type
    }
    chosen = sorted(by_year) if years is None else years
    missing = [y for y in chosen if y not in by_year]
    if missing:
        raise ValueError(f"No {group} ExPORTER file for year(s): {missing}")

    root = raw_dir(f"reporter_{g.table}", s)
    csvs: list[Path] = []
    for year in chosen:
        rec = by_year[year]
        archive = download(
            s.exporter_download_url,
            root / rec.get("file_name", f"{g.table}_{year}.zip"),
            params={"DocType": rec["doc_type_code"], "KeyId": rec["doc_key_id"]},
        )
        csvs.extend(p for p in unzip(archive, root / str(year)) if p.suffix.lower() == ".csv")
    return csvs


def _source_sql(group: str, csv_paths: list[Path] | str, limit: int | None) -> str:
    g = GROUPS[group]
    limit_sql = f" LIMIT {int(limit)}" if limit else ""
    return f"""
        SELECT {g.projection}
        FROM read_csv(
            {csv_source(csv_paths)},
            header = true, all_varchar = true, sample_size = -1,
            quote = '"', escape = '"', union_by_name = true,
            ignore_errors = true, null_padding = true
        )
        WHERE {g.key_filter}{limit_sql}
    """


def curate(
    con: duckdb.DuckDBPyConnection,
    group: str,
    csv_paths: list[Path] | str,
    *,
    target: str | None = None,
    limit: int | None = None,
) -> int:
    """Upsert a group's CSV(s) into its lake table on the group's key."""
    g = GROUPS[group]
    target = target or f"{LAKE}.reporter.{g.table}"
    return upsert(con, target, _source_sql(group, csv_paths, limit), key=list(g.key))


def ingest(
    *,
    group: str = "projects",
    years: list[int] | None = None,
    files: list[str] | str | None = None,
    schema: str = "reporter",
    limit: int | None = None,
    settings: Settings | None = None,
) -> dict:
    """End-to-end: download a group's years (or local files) -> upsert -> summary.

    ``years=None`` loads **all** available years for the group together (the
    union tolerates schema drift). ``schema`` is the target lake schema (e.g.
    ``_dev`` to stage before promoting to ``reporter``).
    """
    if group not in GROUPS:
        raise ValueError(f"Unknown group {group!r}; choose from {sorted(GROUPS)}")
    s = settings or get_settings()
    g = GROUPS[group]
    if files:
        paths: list[Path] | str = files if isinstance(files, str) else [Path(f) for f in files]
    else:
        paths = download_group(group, years, s)

    target = f"{LAKE}.{schema}.{g.table}"
    con = lake_connect(s)
    try:
        with ops.run(con, source="reporter", target=target) as r:
            r.rows = curate(con, group, paths, target=target, limit=limit)
    finally:
        con.close()
    return {**r.summary(), "group": group}
