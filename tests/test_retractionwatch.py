"""Offline tests for the Retraction Watch source.

Curate the CSV into the tidy ``retractionwatch.retractions`` table: multi-value
fields → arrays, M/D/YYYY date parsing, DOI normalization, PMID sentinels → NULL,
Paywalled → bool, non-numeric key dropped, and MERGE idempotency. No network.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from cdsci.lake import Settings, lake_connect, table_exists
from cdsci.lake.sources import retractionwatch

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def lake_settings(tmp_path: Path) -> Settings:
    return Settings(storage_base_uri=f"file://{tmp_path}")


def test_retractionwatch_curate(lake_settings: Settings):
    con = lake_connect(lake_settings)
    try:
        # 3 fixture rows; the non-numeric "Record ID" row drops.
        n = retractionwatch.curate(
            con, FIXTURES / "retractionwatch_sample.csv", "test-2026-06-23"
        )
        assert n == 2
        assert table_exists(con, "retractions")

        a = con.execute("""
            SELECT subjects, institutions, countries, authors, reasons,
                   retraction_date::VARCHAR, original_paper_date::VARCHAR,
                   original_paper_doi, original_paper_pmid, retraction_pmid,
                   retraction_nature, paywalled
            FROM lake.retractionwatch.retractions WHERE record_id = 100
        """).fetchone()
        (subjects, insts, countries, authors, reasons, rdate, odate,
         odoi, opmid, rpmid, nature, paywalled) = a
        assert subjects == ["Biochemistry", "Oncology"]      # split, trailing ';' dropped
        assert insts == ["Inst A", "Inst B"]
        assert countries == ["USA", "China"]
        assert authors == ["Doe, J", "Roe, R"]               # commas inside quoted field
        assert reasons == ["Plagiarism", "Error in Data"]
        assert rdate == "2026-03-20" and odate == "2020-01-02"   # M/D/YYYY parsed
        assert odoi == "10.1/abc"                            # lowercased
        assert opmid == 222 and rpmid == 111
        assert nature == "Retraction" and paywalled is False

        b = con.execute("""
            SELECT original_paper_doi, original_paper_pmid, paywalled, subjects,
                   retraction_date::VARCHAR
            FROM lake.retractionwatch.retractions WHERE record_id = 101
        """).fetchone()
        odoi_b, opmid_b, paywalled_b, subjects_b, rdate_b = b
        assert odoi_b == "10.2/xyz"        # resolver prefix stripped + lowercased
        assert opmid_b is None             # PMID '0' → NULL
        assert paywalled_b is True
        assert subjects_b == []            # empty multi-value → empty array
        assert rdate_b == "2026-06-15"

        # Idempotent re-run adds no snapshot.
        before = con.execute("SELECT max(snapshot_id) FROM lake.snapshots()").fetchone()[0]
        retractionwatch.curate(con, FIXTURES / "retractionwatch_sample.csv", "test-2026-06-23")
        after = con.execute("SELECT max(snapshot_id) FROM lake.snapshots()").fetchone()[0]
        assert before == after
    finally:
        con.close()
