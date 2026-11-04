"""Offline tests for iCite's added columns and citation graph (bioc-on-ice#183).

The fixture (``fixtures/icite_sample.csv``) has the 2026-08 snapshot's 25 columns.
"""

from __future__ import annotations

from pathlib import Path

from cdsci.lake import Settings, lake_connect, upsert
from cdsci.lake.sources import icite

CSV = Path(__file__).parent / "fixtures" / "icite_sample.csv"

# The 17-column projection as it was before the verbatim columns, frozen here: what
# other readers of lake.icite.metadata depend on.
TYPED = f"""
    SELECT
        TRY_CAST(pmid AS BIGINT) AS pmid, NULLIF(lower(doi), '') AS doi,
        TRY_CAST(year AS INTEGER) AS year, title, journal,
        TRY_CAST(relative_citation_ratio AS DOUBLE) AS rcr,
        TRY_CAST(nih_percentile AS DOUBLE) AS nih_percentile,
        TRY_CAST(citation_count AS BIGINT) AS citation_count,
        TRY_CAST(citations_per_year AS DOUBLE) AS citations_per_year,
        TRY_CAST(expected_citations_per_year AS DOUBLE) AS expected_citations_per_year,
        TRY_CAST(field_citation_rate AS DOUBLE) AS field_citation_rate,
        lower(CAST(is_research_article AS VARCHAR)) IN ('yes','true','1') AS is_research_article,
        lower(CAST(is_clinical AS VARCHAR)) IN ('yes','true','1') AS is_clinical,
        TRY_CAST(apt AS DOUBLE) AS apt, TRY_CAST(human AS DOUBLE) AS human,
        TRY_CAST(animal AS DOUBLE) AS animal, CAST('2026-08' AS VARCHAR) AS snapshot_version
    FROM read_csv('{CSV}', header = true, all_varchar = true, sample_size = -1,
                  quote = '"', escape = '"', ignore_errors = true)
    WHERE TRY_CAST(pmid AS BIGINT) IS NOT NULL
"""
TYPED_NAMES = ("pmid", "doi", "year", "title", "journal", "rcr", "nih_percentile",
               "citation_count", "citations_per_year", "expected_citations_per_year",
               "field_citation_rate", "is_research_article", "is_clinical", "apt", "human",
               "animal", "snapshot_version")


def _settings(tmp_path: Path) -> Settings:
    return Settings(storage_base_uri=f"file://{tmp_path}", lake_backend="local")


def test_typed_columns_are_unchanged(tmp_path: Path):
    """The 17 typed columns keep their names, order, types and values; ADDED follow."""
    con = lake_connect(_settings(tmp_path))
    try:
        icite.curate(con, [CSV], "2026-08")
        con.execute(f"CREATE TABLE old AS {TYPED}")
        old = con.execute("DESCRIBE old").fetchall()
        new = con.execute("DESCRIBE lake.icite.metadata").fetchall()
        assert [(c[0], c[1]) for c in new[:17]] == [(c[0], c[1]) for c in old]
        assert [c[0] for c in old] == list(TYPED_NAMES)
        cols = ", ".join(TYPED_NAMES)
        for a, b in (("old", "lake.icite.metadata"), ("lake.icite.metadata", "old")):
            assert con.execute(f"SELECT count(*) FROM (SELECT {cols} FROM {a} EXCEPT ALL "
                               f"SELECT {cols} FROM {b})").fetchone()[0] == 0
        assert [(c[0], c[1]) for c in new[17:]] == [
            ("authors", "VARCHAR"), ("molecular_cellular", "DOUBLE"),
            ("provisional", "BOOLEAN"), ("x_coord", "DOUBLE"), ("y_coord", "DOUBLE"),
            ("cited_by_clin", "BIGINT[]"), ("cited_by", "BIGINT[]"), ("references", "BIGINT[]"),
            ("last_modified", "TIMESTAMP WITH TIME ZONE")]
    finally:
        con.close()


def test_added_columns_are_typed_from_the_csv(tmp_path: Path):
    con = lake_connect(_settings(tmp_path))
    try:
        icite.curate(con, [CSV], "2026-08")
        row = con.execute(
            "SELECT authors, molecular_cellular, provisional, x_coord, y_coord, cited_by_clin, "
            "cited_by, \"references\", last_modified = TIMESTAMPTZ '2026-08-30 03:38:37.156+00' "
            "FROM lake.icite.metadata WHERE pmid = 23456789").fetchone()
        assert row == ("Ann Smith, Bo Li", 0.0, False, 0.0, 1.0, None, [30000001, 30000002],
                       [30000001, 30000001, 99999999], True)
        assert con.execute("SELECT authors, provisional, cited_by FROM lake.icite.metadata "
                           "WHERE pmid = 30000002").fetchone() == (None, True, None)
    finally:
        con.close()


def test_a_pre_existing_table_gains_the_columns_and_mirrors_the_snapshot(tmp_path: Path):
    """The production table has only the 17 typed columns: a full load adds ADDED in
    place, leaves the typed values alone, and prunes a PMID the snapshot no longer has."""
    con = lake_connect(_settings(tmp_path))
    try:
        upsert(con, "lake.icite.metadata",
               f"SELECT * FROM ({TYPED}) UNION ALL SELECT 1, NULL, 1990, 'withdrawn', NULL, "
               "NULL, NULL, 0, NULL, NULL, NULL, false, false, NULL, NULL, NULL, '2026-05'",
               key="pmid", exclude_change_cols=["snapshot_version"])
        assert icite.curate(con, [CSV], "2026-09") == 3
        # The added columns were NULL, so every row changed: the first such load rewrites all.
        assert con.execute("SELECT pmid, snapshot_version FROM lake.icite.metadata "
                           "ORDER BY pmid").fetchall() == [
            (23456789, "2026-09"), (30000001, "2026-09"), (30000002, "2026-09")]
        assert con.execute("SELECT count(*) FROM lake.icite.metadata "
                           "WHERE cited_by IS NOT NULL").fetchone()[0] == 2
    finally:
        con.close()


def test_citations_are_the_distinct_references(tmp_path: Path):
    """Edges come from `references` (duplicates collapse), sharded on cited_pmid % 16; a
    re-run writes nothing and a later load prunes an edge it no longer carries."""
    con = lake_connect(_settings(tmp_path))
    try:
        icite.curate(con, [CSV], "2026-08")
        assert icite.curate_citations(con) == 4
        edges = con.execute("SELECT citing_pmid, cited_pmid, shard FROM lake.icite.citation "
                            "ORDER BY ALL").fetchall()
        assert edges == [(23456789, 30000001, 30000001 % 16),
                         (23456789, 99999999, 99999999 % 16),
                         (30000001, 23456789, 23456789 % 16),
                         (30000002, 23456789, 23456789 % 16)]
        snap = "SELECT max(snapshot_id) FROM lake.snapshots()"
        before = con.execute(snap).fetchone()[0]
        icite.curate_citations(con)
        assert con.execute(snap).fetchone()[0] == before
        con.execute("UPDATE lake.icite.metadata SET \"references\" = NULL WHERE pmid = 30000002")
        assert icite.curate_citations(con) == 3
    finally:
        con.close()
