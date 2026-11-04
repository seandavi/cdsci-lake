"""Offline tests for iCite's verbatim columns and citation graph (bioc-on-ice#183).

The fixture (``fixtures/icite_sample.csv``) has the 2026-08 snapshot's 25 columns.
"""

from __future__ import annotations

from pathlib import Path

from cdsci.lake import Settings, lake_connect, upsert
from cdsci.lake.sources import icite
from cdsci.lake.sources.icite.ingest import RAW_TEXT

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
    """The 17 typed columns keep their names, order, types and values; verbatim ones follow."""
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
        assert [c[0] for c in new[17:]] == [*RAW_TEXT.values(), "snapshot"]
        assert {c[1] for c in new[17:]} == {"VARCHAR"}
    finally:
        con.close()


def test_verbatim_columns_reproduce_the_csv(tmp_path: Path):
    """Every upstream column round-trips as printed, under its upstream or ``_text`` name."""
    con = lake_connect(_settings(tmp_path))
    try:
        icite.curate(con, [CSV], "2026-08")
        raw = ", ".join(f'"{c}"' for c in RAW_TEXT)
        got = ", ".join(f'"{n}"' for n in RAW_TEXT.values())
        csv = (f"SELECT {raw}, title, journal FROM read_csv('{CSV}', header = true, "
               "all_varchar = true) WHERE pmid <> 'notapmid'")
        lake = f"SELECT {got}, title, journal FROM lake.icite.metadata"
        for a, b in ((csv, lake), (lake, csv)):
            assert con.execute(f"SELECT count(*) FROM ({a} EXCEPT ALL {b})").fetchone()[0] == 0
        assert con.execute("SELECT doi_text, relative_citation_ratio FROM lake.icite.metadata "
                           "WHERE pmid = 23456789").fetchone() == ("10.1002/CNCR.27976", "2.51")
    finally:
        con.close()


def test_a_pre_existing_table_gains_the_columns(tmp_path: Path):
    """The production table has only the 17 typed columns: a load adds the rest in place,
    leaves the typed values alone, and a row the new load does not carry stays, its new
    columns NULL, so the current snapshot is WHERE snapshot = <label>."""
    con = lake_connect(_settings(tmp_path))
    try:
        upsert(con, "lake.icite.metadata",
               f"SELECT * FROM ({TYPED}) UNION ALL SELECT 1, NULL, 1990, 'withdrawn', NULL, "
               "NULL, NULL, 0, NULL, NULL, NULL, false, false, NULL, NULL, NULL, '2026-05'",
               key="pmid", exclude_change_cols=["snapshot_version"])
        icite.curate(con, [CSV], "2026-09")
        assert con.execute("SELECT count(*) FROM lake.icite.metadata").fetchone()[0] == 4
        assert con.execute("SELECT pmid FROM lake.icite.metadata WHERE snapshot = '2026-09' "
                           "ORDER BY pmid").fetchall() == [(23456789,), (30000001,), (30000002,)]
        assert con.execute("SELECT title, snapshot, cited_by FROM lake.icite.metadata "
                           "WHERE pmid = 1").fetchone() == ("withdrawn", None, None)
    finally:
        con.close()


def test_citations_are_the_distinct_references(tmp_path: Path):
    """Edges come from `references` (duplicates collapse), sharded on cited_pmid % 16; a
    re-run writes nothing and a later load prunes an edge it no longer carries."""
    con = lake_connect(_settings(tmp_path))
    try:
        icite.curate(con, [CSV], "2026-08")
        assert icite.curate_citations(con, "2026-08") == 4
        edges = con.execute("SELECT citing_pmid, cited_pmid, shard FROM lake.icite.citation "
                            "ORDER BY ALL").fetchall()
        assert edges == [("23456789", "30000001", 30000001 % 16),
                         ("23456789", "99999999", 99999999 % 16),
                         ("30000001", "23456789", 23456789 % 16),
                         ("30000002", "23456789", 23456789 % 16)]
        snap = "SELECT max(snapshot_id) FROM lake.snapshots()"
        before = con.execute(snap).fetchone()[0]
        icite.curate_citations(con, "2026-08")
        assert con.execute(snap).fetchone()[0] == before
        con.execute("UPDATE lake.icite.metadata SET \"references\" = '' WHERE pmid = 30000002")
        assert icite.curate_citations(con, "2026-08") == 3
    finally:
        con.close()
