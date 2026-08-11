"""BugSigDB curated microbial signatures -> ``lake.bugsigdb.signatures``.

BugSigDB is manually curated microbial signatures: per publication, a contrast
between two groups of subjects, and the taxa differentially abundant in one of
them. ``full_dump.csv`` is the canonical export, flattened across study,
experiment and signature — ~7,400 rows, one file, none of the streaming
machinery the larger sources need.

**Versioned by release tag, not retrieval date.** The exports repo re-renders
from bugsigdb.org every hour, so ``devel`` is a moving target; the tagged
releases are the manually-reviewed ones, each archived under a Zenodo DOI.
Landing from a tag is what keeps this idempotent and citable per version.
Ported from bioc-on-ice's ``src/bioconice/bugsigdb.py``, which is retiring in
favor of this (bioc-on-ice#67 / cdsci-lake#31) — bioc-on-ice becomes a
publication layer, this is where the source lives now.

The column contract is explicit on purpose: the CSV header is trusted and the
SELECT names each column, so an upstream rename/removal fails loudly in
DuckDB's binder rather than silently shifting every value one column over —
same reasoning as the original.

``metaphlan_taxon_names`` / ``ncbi_taxonomy_ids`` land as their raw
comma/semicolon-delimited strings (one element per signature member,
positionally matched between the two columns). Exploding them into a
signature<->taxon bridge table is the transform layer's job
(``models/bugsigdb/signature_taxon.sql``), not this EL step's.
"""

from __future__ import annotations

import re
from pathlib import Path

import duckdb

from ... import ops
from ...config import Settings, get_settings
from ...connect import LAKE, csv_source, lake_connect, raw_dir, upsert
from ...download import download

_RAW = "bugsigdb"

# Upstream header -> our column name. Snake-cased throughout; `Source` becomes
# `source_in_paper` because `source` means "the asserting authority" everywhere
# else in this lake, and here it means "Table 2".
COLUMNS = {
    "BSDB ID": "bsdb_id",
    "Study": "study",
    "Study design": "study_design",
    "PMID": "pmid",
    "DOI": "doi",
    "URL": "url",
    "Authors list": "authors_list",
    "Title": "title",
    "Journal": "journal",
    "Year": "year",
    "Keywords": "keywords",
    "Experiment": "experiment",
    "Location of subjects": "location_of_subjects",
    "Host species": "host_species",
    "Body site": "body_site",
    "UBERON ID": "uberon_id",
    "Condition": "condition",
    "EFO ID": "efo_id",
    "Group 0 name": "group_0_name",
    "Group 1 name": "group_1_name",
    "Group 1 definition": "group_1_definition",
    "Group 0 sample size": "group_0_sample_size",
    "Group 1 sample size": "group_1_sample_size",
    "Antibiotics exclusion": "antibiotics_exclusion",
    "Sequencing type": "sequencing_type",
    "16S variable region": "variable_region_16s",
    "Sequencing platform": "sequencing_platform",
    "Data transformation": "data_transformation",
    "Statistical test": "statistical_test",
    "Significance threshold": "significance_threshold",
    "MHT correction": "mht_correction",
    "LDA Score above": "lda_score_above",
    "Matched on": "matched_on",
    "Confounders controlled for": "confounders_controlled_for",
    "Pielou": "pielou",
    "Shannon": "shannon",
    "Chao1": "chao1",
    "Simpson": "simpson",
    "Inverse Simpson": "inverse_simpson",
    "Richness": "richness",
    "Signature page name": "signature_page_name",
    "Source": "source_in_paper",
    "Curated date": "curated_date",
    "Curator": "curator",
    "Revision editor": "revision_editor",
    "Description": "description",
    "Abundance in Group 1": "abundance_in_group_1",
    "MetaPhlAn taxon names": "metaphlan_taxon_names",
    "NCBI Taxonomy IDs": "ncbi_taxonomy_ids",
    "State": "state",
    "Reviewer": "reviewer",
}

# A handful of columns are typed rather than left VARCHAR — the ones with join
# or filter value (like retractionwatch's dates/PMIDs/arrays). Everything else
# stays plain text; this is a curated *export*, not a normalized schema, and
# most columns are free text or fixed-vocabulary strings not worth guessing at.
_TYPED = {
    "PMID": 'TRY_CAST("PMID" AS BIGINT)',
    "Year": 'TRY_CAST("Year" AS INTEGER)',
    "Curated date": "TRY_STRPTIME(\"Curated date\", '%d %B %Y')::DATE",
    "Group 0 sample size": 'TRY_CAST("Group 0 sample size" AS INTEGER)',
    "Group 1 sample size": 'TRY_CAST("Group 1 sample size" AS INTEGER)',
}


def dump_url(version: str, repo: str) -> str:
    return f"{repo}/{version}/full_dump.csv"


def download_csv(version: str, settings: Settings | None = None) -> Path:
    """Download one release tag's ``full_dump.csv`` into the raw layer.

    Dest is keyed by tag, not date — re-landing the same tag is a no-op
    (``download`` skips an existing file) and landing a new tag accumulates
    alongside the old one, same as bioc-on-ice's original.
    """
    s = settings or get_settings()
    dest = raw_dir(_RAW, s) / f"{version}-full_dump.csv"
    return download(dump_url(version, s.bugsigdb_repo), dest)


def _exported_at(path: Path) -> str | None:
    """The export's self-declared banner timestamp (first line), not file mtime.

    The first line is ``# BugSigDB 2026-04-24_00:41_UTC, License: ..., URL:
    ...`` — only that line is read, not the whole file.
    """
    with open(path, encoding="utf-8") as f:
        first = f.readline()
    m = re.match(r"#\s*BugSigDB\s+([^,\s]+)", first)
    return m.group(1) if m else None


def _column_sql(src: str, dst: str) -> str:
    expr = _TYPED.get(src, f'"{src}"')
    return f"{expr} AS {dst}"


def _select_sql(path: Path, version: str, exported_at: str | None, limit: int | None) -> str:
    limit_sql = f" LIMIT {int(limit)}" if limit else ""
    select = ",\n            ".join(_column_sql(src, dst) for src, dst in COLUMNS.items())
    exported_sql = f"'{exported_at}'" if exported_at else "NULL::VARCHAR"
    # Dialect stated, not sniffed: free-text columns carry commas/quotes/newlines,
    # a sniffer guessing differently between releases would shift values silently.
    # nullstr='NA' is BugSigDB's missing marker. skip=1 drops the banner line so
    # the real header is read as the header.
    return f"""
        SELECT
            {select},
            {exported_sql} AS export_timestamp,
            '{version}' AS bugsigdb_version
        FROM read_csv({csv_source([path])}, skip=1, header=true, all_varchar=true,
                      nullstr='NA', delim=',', quote='"', escape='"', ignore_errors=true)
        {limit_sql}
    """


def curate(
    con: duckdb.DuckDBPyConnection,
    path: Path,
    version: str,
    exported_at: str | None,
    *,
    target: str | None = None,
    limit: int | None = None,
) -> int:
    """MERGE-upsert one release's ``full_dump.csv`` into ``signatures`` on ``bsdb_id``."""
    target = target or f"{LAKE}.bugsigdb.signatures"
    return upsert(
        con, target, _select_sql(path, version, exported_at, limit),
        key="bsdb_id", exclude_change_cols=["export_timestamp"],
    )


def ingest(
    *,
    file: str | None = None,
    version: str | None = None,
    schema: str = "bugsigdb",
    limit: int | None = None,
    settings: Settings | None = None,
) -> dict:
    """End-to-end: download (unless ``file``) a release tag -> MERGE-upsert -> summary."""
    s = settings or get_settings()
    version = version or s.bugsigdb_version
    path = Path(file) if file else download_csv(version, s)
    exported_at = _exported_at(path)
    target = f"{LAKE}.{schema}.signatures"
    con = lake_connect(s)
    try:
        with ops.run(con, source="bugsigdb", target=target, version=version) as r:
            r.rows = curate(con, path, version, exported_at, target=target, limit=limit)
    finally:
        con.close()
    return r.summary()
