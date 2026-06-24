"""Ingest **Reliance on Science** (Marx) — patent↔paper links — into ``lake.reliance``.

> **License: CC BY-NC 4.0 (Attribution-NonCommercial).** Internal, non-commercial
> research use only (we are a state university). **Do not redistribute** this data
> or any extract that includes it outside the institution. Attribution: Marx, M.,
> *Reliance on Science* (Zenodo). The constraint is carried forward in the
> ``lake_ops.source`` registry (``license='cc-by-nc-4.0'``) — consumers can and
> should read it. See ``docs/design/reliance.md`` and ``docs/data-licenses.md``.

Two files from a pinned Zenodo record (v63, 2024 ed.), both keyed by the
**OpenAlex Work ID** so they join straight onto ``openalex.works``:

* ``_pcs_oa.csv`` → ``reliance.patent_citations`` — patents citing papers
  (the "reliance" signal: research relied upon by patented invention).
* ``_patent_paper_pairs.csv`` → ``reliance.patent_paper_pairs`` — same-team
  paper/patent matches.

MERGE-upsert on the natural key (ADR-0003); run recorded via ``ops.run`` (ADR-0006).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import duckdb

from ... import ops
from ...config import Settings, get_settings
from ...connect import LAKE, csv_source, lake_connect, raw_dir, upsert
from ...download import download

_RAW = "reliance"  # raw-download subdir name


@dataclass(frozen=True)
class Dataset:
    """One Reliance file → one lake table + its natural key."""

    filename: str
    table: str
    key: list[str] = field(default_factory=list)


DATASETS: dict[str, Dataset] = {
    "citations": Dataset("_pcs_oa.csv", "patent_citations",
                         ["patent", "work_id", "reftype", "wherefound"]),
    "pairs": Dataset("_patent_paper_pairs.csv", "patent_paper_pairs",
                     ["work_id", "patent"]),
}


def _sql_str(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def download_dataset(dataset: str, version: str, settings: Settings | None = None) -> Path:
    """Download one Reliance file into the raw layer (resumably). Returns the path."""
    s = settings or get_settings()
    spec = DATASETS[dataset]
    url = f"{s.reliance_base_url}/{spec.filename}"
    dest = raw_dir(_RAW, s) / f"{version}-{spec.filename}"
    return download(url, dest)


def _citations_sql(path: Path, version: str, limit: int | None) -> str:
    """patent→paper citations: normalize oaid→W-form, dedup on the natural key."""
    limit_sql = f" LIMIT {int(limit)}" if limit else ""
    return f"""
        SELECT
            lower(trim(patent))                          AS patent,
            'W' || CAST(TRY_CAST(oaid AS BIGINT) AS VARCHAR) AS work_id,
            TRY_CAST(oaid AS BIGINT)                     AS oaid,
            reftype,
            wherefound,
            max(TRY_CAST(confscore AS INTEGER))          AS confscore,
            bool_or(lower("self") = 'self')              AS self_cite,
            bool_or(TRY_CAST(uspto AS INTEGER) = 1)      AS uspto,
            CAST({_sql_str(version)} AS VARCHAR)         AS snapshot_version
        FROM read_csv(
            {csv_source([path])},
            header = true, all_varchar = true, sample_size = -1, ignore_errors = true
        )
        WHERE TRY_CAST(oaid AS BIGINT) IS NOT NULL
        GROUP BY patent, work_id, oaid, reftype, wherefound{limit_sql}
    """


def _pairs_sql(path: Path, version: str, limit: int | None) -> str:
    """same-team paper/patent pairs: paperid is already W-form; dedup on (work_id, patent)."""
    limit_sql = f" LIMIT {int(limit)}" if limit else ""
    return f"""
        SELECT
            paperid                                      AS work_id,
            upper(trim(patent))                          AS patent,
            max(TRY_CAST(ppp_score AS INTEGER))          AS ppp_score,
            max(TRY_CAST(daysdiffcont AS INTEGER))       AS days_paper_to_patent,
            any_value(nullif(trim(all_patents_for_the_same_paper), '')) AS all_patents_for_paper,
            CAST({_sql_str(version)} AS VARCHAR)         AS snapshot_version
        FROM read_csv(
            {csv_source([path])},
            header = true, all_varchar = true, sample_size = -1, ignore_errors = true
        )
        WHERE nullif(trim(paperid), '') IS NOT NULL
        GROUP BY work_id, patent{limit_sql}
    """


def curate(
    con: duckdb.DuckDBPyConnection,
    dataset: str,
    path: Path,
    version: str,
    *,
    schema: str = "reliance",
    limit: int | None = None,
) -> int:
    """MERGE-upsert one Reliance dataset into its ``reliance`` table; return its row count."""
    spec = DATASETS[dataset]
    target = f"{LAKE}.{schema}.{spec.table}"
    sql = _citations_sql(path, version, limit) if dataset == "citations" \
        else _pairs_sql(path, version, limit)
    upsert(con, target, sql, key=spec.key, exclude_change_cols=["snapshot_version"])
    return con.execute(f"SELECT count(*) FROM {target}").fetchone()[0]


def ingest(
    *,
    dataset: str | None = None,
    file: str | None = None,
    version: str | None = None,
    schema: str = "reliance",
    limit: int | None = None,
    settings: Settings | None = None,
) -> dict:
    """End-to-end: download + MERGE the Reliance dataset(s). ``dataset`` loads one
    (``citations`` | ``pairs``); ``file`` loads a local CSV for that ``dataset``;
    omit both to load both files."""
    s = settings or get_settings()
    version = version or f"zenodo-{s.reliance_zenodo_record}"
    names = [dataset] if dataset else list(DATASETS)

    con = lake_connect(s)
    try:
        counts: dict[str, int] = {}
        with ops.run(con, source="reliance", target=f"{LAKE}.{schema}", version=version) as r:
            for name in names:
                path = Path(file) if file else download_dataset(name, version, s)
                counts[name] = curate(con, name, path, version, schema=schema, limit=limit)
            r.rows = sum(counts.values())
    finally:
        con.close()
    return {**r.summary(), "schema": schema, "counts": counts}
