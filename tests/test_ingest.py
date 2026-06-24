"""Offline tests for the ``cri`` lake substrate and the iCite / RePORTER curates.

These exercise the full **curate-into-DuckLake** mechanics against small CSV
fixtures (no network): attach a temp lake, build the table, assert typing,
casting, row-dropping, and that a snapshot was recorded. Live bulk fetches are
covered by an opt-in integration test (``RUN_INTEGRATION=1``).
"""

from __future__ import annotations

import gzip
import json
import os
from pathlib import Path

import pytest

from cdsci.lake import Settings, csv_source, lake_connect, snapshots, table_exists, upsert
from cdsci.lake.sources import icite, reporter, scp
from cdsci.lake.sources.icite.ingest import _pick_metadata_file
from cdsci.lake.sources.openalex.ingest import curate_works_batch

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def lake_settings(tmp_path: Path) -> Settings:
    """A Settings pointing the whole lake (catalog + data) into a temp dir."""
    return Settings(storage_base_uri=f"file://{tmp_path}")


def test_csv_source_rendering():
    assert csv_source("a/*.csv") == "'a/*.csv'"
    assert csv_source([Path("/x/a.csv"), Path("/x/b.csv")]) == "['/x/a.csv', '/x/b.csv']"


def test_icite_curate(lake_settings: Settings):
    con = lake_connect(lake_settings)
    try:
        rows = icite.curate(con, [FIXTURES / "icite_sample.csv"], "test-2026-06")
        # 4 rows in the fixture; the non-numeric pmid row is dropped.
        assert rows == 3
        assert table_exists(con, "metadata")

        r = con.execute(
            "SELECT pmid, doi, rcr, citation_count, is_research_article, snapshot_version "
            "FROM lake.icite.metadata WHERE pmid = 23456789"
        ).fetchone()
        pmid, doi, rcr, citations, is_article, version = r
        assert pmid == 23456789
        assert doi == "10.1002/cncr.27976"  # lowercased
        assert rcr == pytest.approx(2.51)
        assert citations == 142
        assert is_article is True
        assert version == "test-2026-06"

        # Empty DOI becomes NULL, not "".
        null_doi = con.execute(
            "SELECT doi FROM lake.icite.metadata WHERE pmid = 30000002"
        ).fetchone()[0]
        assert null_doi is None

        # A snapshot was committed to the catalog (time-travel / versioning).
        assert len(snapshots(con)) >= 1
    finally:
        con.close()


def test_icite_curate_limit(lake_settings: Settings):
    con = lake_connect(lake_settings)
    try:
        rows = icite.curate(con, [FIXTURES / "icite_sample.csv"], "test", limit=1)
        assert rows == 1
    finally:
        con.close()


def test_icite_ingest_records_ops_run(lake_settings: Settings):
    """End-to-end ingest(--file) goes through ops.run and records a success row."""
    from cdsci.lake import ops

    summary = icite.ingest(file=str(FIXTURES / "icite_sample.csv"), settings=lake_settings)
    assert summary["status"] == "success" and summary["run_id"]
    assert summary["changed"] is True
    assert summary["rows"] == 3

    con = lake_connect(lake_settings)
    try:
        last = ops.last_run(con, "icite")
        assert last["status"] == "success"
        assert last["target"] == summary["table"]
        assert last["snapshot_after"] == summary["snapshot"]
    finally:
        con.close()


def test_upsert_time_travel_semantics(lake_settings: Settings):
    """MERGE upsert: new rows insert, changed rows update, a no-op adds no snapshot."""
    con = lake_connect(lake_settings)
    try:
        src1 = "SELECT * FROM (VALUES (1, 'a'), (2, 'b')) v(id, val)"
        assert upsert(con, "lake.main.t", src1, key="id") == 2

        # Identical re-run must be idempotent: no new snapshot.
        before = con.execute("SELECT max(snapshot_id) FROM lake.snapshots()").fetchone()[0]
        upsert(con, "lake.main.t", src1, key="id")
        after = con.execute("SELECT max(snapshot_id) FROM lake.snapshots()").fetchone()[0]
        assert before == after

        # A real change updates the matched row and inserts the new one.
        src2 = "SELECT * FROM (VALUES (2, 'B2'), (3, 'c')) v(id, val)"
        assert upsert(con, "lake.main.t", src2, key="id") == 3
        assert con.execute("SELECT val FROM lake.main.t WHERE id = 2").fetchone()[0] == "B2"
        assert con.execute("SELECT max(snapshot_id) FROM lake.snapshots()").fetchone()[0] > after
    finally:
        con.close()


def test_upsert_excludes_metadata_from_change(lake_settings: Settings):
    """A per-load stamp (snapshot_version) must not force a rewrite by itself."""
    con = lake_connect(lake_settings)
    try:
        excl = ["snapshot_version"]
        s1 = "SELECT * FROM (VALUES (1,'a','2026-05'),(2,'b','2026-05')) v(id,val,snapshot_version)"
        upsert(con, "lake.main.t", s1, key="id", exclude_change_cols=excl)
        before = con.execute("SELECT max(snapshot_id) FROM lake.snapshots()").fetchone()[0]

        # Same data, NEW tag only → no-op (no snapshot), old tag retained.
        s2 = "SELECT * FROM (VALUES (1,'a','2026-06'),(2,'b','2026-06')) v(id,val,snapshot_version)"
        upsert(con, "lake.main.t", s2, key="id", exclude_change_cols=excl)
        tag1 = "SELECT snapshot_version FROM lake.main.t WHERE id=1"
        assert con.execute("SELECT max(snapshot_id) FROM lake.snapshots()").fetchone()[0] == before
        assert con.execute(tag1).fetchone()[0] == "2026-05"

        # Real change to id=1 → updates AND re-stamps; id=2 untouched keeps old tag.
        s3 = "SELECT * FROM (VALUES (1,'A','2026-07'),(2,'b','2026-07')) v(id,val,snapshot_version)"
        upsert(con, "lake.main.t", s3, key="id", exclude_change_cols=excl)
        row1 = con.execute("SELECT val, snapshot_version FROM lake.main.t WHERE id=1").fetchone()
        assert row1 == ("A", "2026-07")
        tag2 = con.execute("SELECT snapshot_version FROM lake.main.t WHERE id=2").fetchone()[0]
        assert tag2 == "2026-05"  # unchanged row keeps its original stamp
    finally:
        con.close()


def test_reporter_projects_curate(lake_settings: Settings):
    con = lake_connect(lake_settings)
    try:
        rows = reporter.curate(con, "projects", [FIXTURES / "reporter_sample.csv"])
        assert rows == 2  # the 'badid' row drops on non-numeric appl_id

        r = con.execute(
            "SELECT core_project_num, fiscal_year, total_cost, org_name, pi_names "
            "FROM lake.reporter.projects WHERE appl_id = 10617263"
        ).fetchone()
        core, fy, total, org, pis = r
        assert core == "5R01CA123456"
        assert fy == 2023
        assert total == pytest.approx(750000.0)
        assert "COLORADO" in org.upper()
        assert "DAVIS, SEAN" in pis  # PI_NAMEs header read case-insensitively
    finally:
        con.close()


def test_reporter_publink_composite_key(lake_settings: Settings, tmp_path):
    """The LINK group keys on (pmid, project_number) — the grants<->PMID crosswalk."""
    csv = tmp_path / "publink.csv"
    csv.write_text("PMID,PROJECT_NUMBER\n111,R01CA1\n111,R01CA2\n222,R01CA1\nx,R01BAD\n")
    con = lake_connect(lake_settings)
    try:
        rows = reporter.curate(con, "publink", [csv])
        assert rows == 3  # 3 valid edges; the non-numeric pmid row drops

        before = con.execute("SELECT max(snapshot_id) FROM lake.snapshots()").fetchone()[0]
        reporter.curate(con, "publink", [csv])  # identical re-run
        after = con.execute("SELECT max(snapshot_id) FROM lake.snapshots()").fetchone()[0]
        assert before == after  # idempotent on the composite key
    finally:
        con.close()


def test_ctgov_curate(lake_settings: Settings, tmp_path):
    """Full-JSON ingest: bronze parquet → typed studies (record kept) + nct↔pmid refs."""
    import shutil

    from cdsci.lake.sources import ctgov

    nd = tmp_path / "ctgov_sample.ndjson"
    shutil.copy(FIXTURES / "ctgov_sample.ndjson", nd)
    con = lake_connect(lake_settings)
    try:
        raw = ctgov.materialize_raw(con, nd)  # bronze parquet
        counts = ctgov.curate(con, raw)
        assert counts["studies"] == 2
        assert counts["references"] == 2  # NCT03691623 has 2 PMIDs; NCT00860379 none

        status, stype, enroll, rec_len = con.execute(
            "SELECT status, study_type, enrollment, length(record) "
            "FROM lake.ctgov.studies WHERE nct_id = 'NCT03691623'"
        ).fetchone()
        assert status == "COMPLETED" and stype == "INTERVENTIONAL" and enroll == 179
        assert rec_len > 1000  # the full JSON record is preserved, not lost

        pmids = {
            x[0]
            for x in con.execute(
                "SELECT pmid FROM lake.ctgov.references WHERE nct_id = 'NCT03691623'"
            ).fetchall()
        }
        assert pmids == {35172056, 39441691}
    finally:
        con.close()


def test_scp_curate_all_domains(lake_settings: Settings):
    """Curate each State Cancer Profiles domain from its .csv.gz/.csv fixture.

    Asserts: row counts, numeric TRY_CAST to DOUBLE on the tidy domains, the WIDE
    demographics table keeps ``persistent_poverty`` as text while casting a
    numeric measure to DOUBLE, and the release tag stamps every row.
    """
    con = lake_connect(lake_settings)
    try:
        tag = "test-2026-06-01"
        counts = {
            d: scp.curate(con, d, FIXTURES / f"scp_{d}.csv", tag)
            for d in ("incidence", "mortality", "risk", "demographics")
        }
        assert counts == {"incidence": 5, "mortality": 5, "risk": 5, "demographics": 4}
        for d in counts:
            assert table_exists(con, d)

        # Tidy domain: the age-adjusted rate cast to DOUBLE; tag stamped.
        rate, ver = con.execute(
            "SELECT age_adjusted_rate, snapshot_version FROM lake.scp.incidence "
            "WHERE age_adjusted_rate IS NOT NULL ORDER BY age_adjusted_rate LIMIT 1"
        ).fetchone()
        assert isinstance(rate, float)
        assert ver == tag

        # risk: percent cast to DOUBLE.
        pct = con.execute(
            "SELECT percent FROM lake.scp.risk WHERE percent IS NOT NULL LIMIT 1"
        ).fetchone()[0]
        assert isinstance(pct, float)

        # demographics column types: persistent_poverty is VARCHAR, a measure is DOUBLE.
        types = dict(
            con.execute(
                "SELECT column_name, data_type FROM information_schema.columns "
                "WHERE table_schema = 'scp' AND table_name = 'demographics'"
            ).fetchall()
        )
        assert types["persistent_poverty"] == "VARCHAR"
        assert types["people_below_poverty"] == "DOUBLE"

        # The categorical text value is preserved verbatim ('no'/'yes'), not cast.
        pp = {
            r[0]
            for r in con.execute(
                "SELECT persistent_poverty FROM lake.scp.demographics "
                "WHERE persistent_poverty IS NOT NULL"
            ).fetchall()
        }
        assert "no" in pp
        # The 'data not available' sentinel survives as text in persistent_poverty,
        # but a numeric measure on a sentinel row is NULL (TRY_CAST), never an error.
        assert "data not available" in pp

        # An idempotent re-run of all domains adds no snapshot.
        before = con.execute("SELECT max(snapshot_id) FROM lake.snapshots()").fetchone()[0]
        for d in counts:
            scp.curate(con, d, FIXTURES / f"scp_{d}.csv", tag)
        after = con.execute("SELECT max(snapshot_id) FROM lake.snapshots()").fetchone()[0]
        assert before == after
    finally:
        con.close()


def test_pmc_curate(lake_settings: Settings, tmp_path):
    """BioC-PMC: bronze parquet → pmc.documents (1/article) + pmc.passages (exploded)."""
    import shutil

    from cdsci.lake.sources import pmc

    nd = tmp_path / "pmc_sample.ndjson"
    shutil.copy(FIXTURES / "pmc_sample.ndjson", nd)
    con = lake_connect(lake_settings)
    try:
        raw = pmc.materialize_raw(con, nd)  # bronze parquet
        # 2 documents; 121 + 82 = 203 passages exploded out of the nested record.
        assert pmc.curate(con, raw, snapshot="test") == {"documents": 2, "passages": 203}

        pmid, doi, lic, npass = con.execute(
            "SELECT pmid, doi, license, n_passages "
            "FROM lake.pmc.documents WHERE pmcid = 'PMC1790863'"
        ).fetchone()
        assert pmid == 17299597  # extracted from passages[0].infons.article-id_pmid
        assert doi.startswith("10.1371")
        assert lic == "CC BY"
        assert npass == 121

        # n_passages on the document matches the passage rows for that pmcid.
        assert (
            con.execute(
                "SELECT count(*) FROM lake.pmc.passages WHERE pmcid = 'PMC1790863'"
            ).fetchone()[0]
            == 121
        )

        # The title passage (index 0) is the article title; section/type/offset kept.
        idx, off, sect, ptype, text = con.execute(
            "SELECT passage_index, passage_offset, section_type, passage_type, text "
            "FROM lake.pmc.passages WHERE pmcid = 'PMC1790863' AND passage_index = 0"
        ).fetchone()
        assert (idx, off) == (0, 0)
        assert text.startswith("Quantifying Organismal Complexity")
        # The first body passage carries BioC section metadata.
        sect1, ptype1 = con.execute(
            "SELECT section_type, passage_type FROM lake.pmc.passages "
            "WHERE pmcid = 'PMC1790863' AND passage_index = 1"
        ).fetchone()
        assert (sect1, ptype1) == ("ABSTRACT", "abstract_title_1")

        # PMC13900 has no DOI → NULL (not "").
        assert (
            con.execute(
                "SELECT doi FROM lake.pmc.documents WHERE pmcid = 'PMC13900'"
            ).fetchone()[0]
            is None
        )

        # append mode (disjoint-range bulk path): INSERT, returns rows written.
        raw2 = pmc.materialize_raw(con, nd)
        assert pmc.curate(con, raw2, snapshot="bulk", mode="append", schema="pmcx") == {
            "documents": 2,
            "passages": 203,
        }
        assert con.execute("SELECT count(*) FROM lake.pmcx.documents").fetchone()[0] == 2
        assert con.execute("SELECT count(*) FROM lake.pmcx.passages").fetchone()[0] == 203
    finally:
        con.close()


def test_census_geo_upsert_wkb(lake_settings: Settings):
    """ref.geo_state: geometry stored as WKB round-trips; tag-only change is idempotent."""
    con = lake_connect(lake_settings)
    con.execute("INSTALL spatial; LOAD spatial;")
    try:
        src = (
            "SELECT '01' AS fips, 'AL' AS abbrev, 'Alabama' AS name, "
            "ST_AsWKB(ST_Point(1.0, 2.0)) AS geom_wkb, '2023' AS snapshot_version"
        )
        upsert(con, "lake.ref.geo_state", src, key="fips", exclude_change_cols=["snapshot_version"])
        wkt = con.execute(
            "SELECT ST_AsText(ST_GeomFromWKB(geom_wkb)) FROM lake.ref.geo_state WHERE fips='01'"
        ).fetchone()[0]
        assert wkt.startswith("POINT")

        before = con.execute("SELECT max(snapshot_id) FROM lake.snapshots()").fetchone()[0]
        upsert(
            con, "lake.ref.geo_state", src.replace("'2023'", "'2024'"),
            key="fips", exclude_change_cols=["snapshot_version"],
        )
        after = con.execute("SELECT max(snapshot_id) FROM lake.snapshots()").fetchone()[0]
        assert after == before  # only the cb vintage changed → no rewrite
    finally:
        con.close()


@pytest.mark.skipif(not os.getenv("RUN_INTEGRATION"), reason="downloads a Census shapefile")
def test_census_geo_state_live(lake_settings: Settings):
    from cdsci.lake.sources import census_geo

    con = lake_connect(lake_settings)
    try:
        shp = census_geo.download_layer("geo_state", 2023)
        assert census_geo.curate(con, "geo_state", shp, year=2023) >= 50
        assert (
            con.execute("SELECT abbrev FROM lake.ref.geo_state WHERE fips='06'").fetchone()[0]
            == "CA"
        )
    finally:
        con.close()


def test_maintenance_dry_run(lake_settings: Settings):
    """expire (dry-run, bounded) + cleanup on a local lake; preview is non-destructive."""
    from cdsci.lake import maintenance

    con = lake_connect(lake_settings)
    try:
        upsert(con, "lake.main.t", "SELECT * FROM (VALUES (1,'a'),(2,'b')) v(id,val)", key="id")
        upsert(con, "lake.main.t", "SELECT * FROM (VALUES (2,'B'),(3,'c')) v(id,val)", key="id")
        n0 = con.execute("SELECT count(*) FROM lake.snapshots()").fetchone()[0]

        # Unbounded expiry is refused.
        with pytest.raises(ValueError):
            maintenance.expire_snapshots(con, older_than=None)

        # Dry-run previews without changing the catalog.
        preview = maintenance.expire_snapshots(con, older_than="2999-01-01", dry_run=True)
        assert isinstance(preview, list)
        assert con.execute("SELECT count(*) FROM lake.snapshots()").fetchone()[0] == n0

        # cleanup_files (orphans only) runs and returns a list.
        assert isinstance(maintenance.cleanup_files(con, dry_run=True), list)
    finally:
        con.close()


def test_pick_metadata_file_prefers_metadata_archive():
    files = [
        {"name": "readme.txt", "download_url": "u0"},
        {"name": "open_citation_collection.zip", "download_url": "u1"},
        {"name": "icite_metadata.zip", "download_url": "u2"},
    ]
    assert _pick_metadata_file(files)["download_url"] == "u2"


@pytest.mark.skipif(not os.getenv("RUN_INTEGRATION"), reason="set RUN_INTEGRATION=1")
def test_icite_resolve_latest_live():
    info = icite.resolve_latest()
    assert info["version"]
    assert info["files"]


@pytest.mark.skipif(not os.getenv("RUN_INTEGRATION"), reason="needs GSM creds + network")
def test_shared_lake_attach_live():
    """Read-only attach of the live Postgres+R2 lake (catalog-only; no data scan)."""
    from cdsci.lake import lake_connect

    con = lake_connect(Settings(lake_backend="postgres"), read_only=True)
    tables = {
        r[0]
        for r in con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_catalog='lake'"
        ).fetchall()
    }
    assert "pubmed_article" in tables


def _write_openalex_parts(path: Path) -> None:
    """A gzipped NDJSON works fixture: two in-domain works + one to be filtered."""
    works = [
        {  # Health Sciences (domain 4) — kept
            "id": "https://openalex.org/W100",
            "doi": "https://doi.org/10.1/x",
            "title": "Cancer study",
            "type": "article",
            "publication_year": 2020,
            "publication_date": "2020-03-01",
            "cited_by_count": 10,
            "referenced_works_count": 2,
            "ids": {
                "pmid": "https://pubmed.ncbi.nlm.nih.gov/28748124",
                "pmcid": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC5783702",
            },
            "primary_topic": {
                "id": "https://openalex.org/T10", "display_name": "Oncology",
                "subfield": {"display_name": "Cancer Research"},
                "field": {"display_name": "Medicine"},
                "domain": {"id": "https://openalex.org/domains/4",
                           "display_name": "Health Sciences"},
            },
            "open_access": {"oa_status": "gold", "is_oa": True},
            "primary_location": {"source": {
                "id": "https://openalex.org/S5", "display_name": "Cancer Journal",
                "issn_l": "1234-5678", "type": "journal"}},
            "abstract_inverted_index": {"Tumor": [0], "growth": [1], "in": [2], "mice": [3]},
            "referenced_works": ["https://openalex.org/W1", "https://openalex.org/W2"],
            "authorships": [
                {"author_position": "first", "is_corresponding": True,
                 "author": {"id": "https://openalex.org/A1", "display_name": "Jane Doe"},
                 "institutions": [
                     {"id": "https://openalex.org/I10", "ror": "https://ror.org/01",
                      "country_code": "US"},
                     {"id": "https://openalex.org/I11", "ror": "https://ror.org/02",
                      "country_code": "US"}]},
                {"author_position": "middle", "is_corresponding": False,
                 "author": {"id": "https://openalex.org/A2", "display_name": "John Roe"},
                 "institutions": []},  # no affiliation -> dropped from the edge table
            ],
        },
        {  # Life Sciences (domain 1) — kept; no PMID
            "id": "https://openalex.org/W200",
            "title": "Gene study",
            "type": "article",
            "primary_topic": {
                "id": "https://openalex.org/T20", "display_name": "Genetics",
                "domain": {"id": "https://openalex.org/domains/1",
                           "display_name": "Life Sciences"}},
            "abstract_inverted_index": {"Gene": [0], "expression": [1]},
            "referenced_works": ["https://openalex.org/W3"],
        },
        {  # Physical Sciences (domain 3) — filtered out
            "id": "https://openalex.org/W300",
            "title": "Quark study",
            "primary_topic": {"domain": {"id": "https://openalex.org/domains/3",
                                         "display_name": "Physical Sciences"}},
            "referenced_works": ["https://openalex.org/W9"],
        },
    ]
    with gzip.open(path, "wt", encoding="utf-8") as fh:
        for w in works:
            fh.write(json.dumps(w) + "\n")


def test_openalex_works_prune_and_reconstruct(lake_settings: Settings):
    part = Path(lake_settings.storage_base_uri[len("file://"):]) / "oa_works.json.gz"
    part.parent.mkdir(parents=True, exist_ok=True)
    _write_openalex_parts(part)

    con = lake_connect(lake_settings)
    try:
        out = curate_works_batch(
            con, [str(part)], schema="openalex", version="test-2026-06",
            mode="merge", domains=["1", "4"], max_obj=67_108_864,
        )
        # Physical-sciences work dropped by the domain filter.
        assert out["works"] == 2
        assert table_exists(con, "works")

        r = con.execute(
            "SELECT doi, abstract, pmid, pmcid, domain_id, source_name, is_oa, "
            "       publication_date, snapshot_version "
            "FROM lake.openalex.works WHERE id = 'W100'"
        ).fetchone()
        doi, abstract, pmid, pmcid, domain_id, source_name, is_oa, pub_date, version = r
        assert doi == "10.1/x"                       # normalized: prefix stripped, lowercased
        assert abstract == "Tumor growth in mice"    # reconstructed from inverted index
        assert pmid == 28748124                      # stripped to BIGINT
        assert pmcid == "PMC5783702"
        assert domain_id == "4"
        assert source_name == "Cancer Journal"
        assert is_oa is True
        assert str(pub_date) == "2020-03-01"
        assert version == "test-2026-06"

        # authorships JSON is promoted out of works into the edge table.
        cols = {c[0] for c in con.execute("DESCRIBE lake.openalex.works").fetchall()}
        assert "authorships" not in cols

        assert con.execute(
            "SELECT count(*) FROM lake.openalex.works WHERE id = 'W300'"
        ).fetchone()[0] == 0

        # Citation edges: 2 (W100) + 1 (W200) = 3, ids stripped to short form, no W9.
        assert out["references"] == 3
        edge = con.execute(
            "SELECT referenced_work_id FROM lake.openalex.work_references "
            "WHERE work_id = 'W100' ORDER BY referenced_work_id"
        ).fetchall()
        assert [e[0] for e in edge] == ["W1", "W2"]
        assert con.execute(
            "SELECT count(*) FROM lake.openalex.work_references WHERE referenced_work_id = 'W9'"
        ).fetchone()[0] == 0

        # Affiliation edges: A1 × {I10, I11} = 2; A2 (no institution) dropped.
        assert out["authorships"] == 2
        aff = con.execute(
            "SELECT author_id, institution_id, institution_ror, is_corresponding "
            "FROM lake.openalex.works_authorships WHERE work_id = 'W100' "
            "ORDER BY institution_id"
        ).fetchall()
        assert [(a[0], a[1], a[2]) for a in aff] == [
            ("A1", "I10", "https://ror.org/01"),
            ("A1", "I11", "https://ror.org/02"),
        ]
        assert all(a[3] is True for a in aff)        # A1 is_corresponding
        assert con.execute(
            "SELECT count(*) FROM lake.openalex.works_authorships WHERE author_id = 'A2'"
        ).fetchone()[0] == 0

        # Idempotent: a re-merge of identical data writes nothing new.
        out2 = curate_works_batch(
            con, [str(part)], schema="openalex", version="test-2026-06",
            mode="merge", domains=["1", "4"], max_obj=67_108_864,
        )
        assert out2 == {"works": 2, "references": 3, "authorships": 2}
    finally:
        con.close()
