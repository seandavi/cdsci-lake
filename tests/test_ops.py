"""Offline tests for the ``cdsci.lake.ops`` operational ledger (ADR-0006).

Exercise the ledger against the local sibling-file backend: bootstrap +
registry, the :func:`ops.run` context manager (success / idempotent / error),
``last_run``, and watermark round-trips. No network, no Postgres.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from cdsci.lake import Settings, lake_connect, ops, upsert
from cdsci.lake.publish.release import ArtifactStatus, PublicationReceipt


@pytest.fixture
def lake_settings(tmp_path: Path) -> Settings:
    return Settings(storage_base_uri=f"file://{tmp_path}")


def test_connect_creates_tables_but_does_not_seed(lake_settings: Settings):
    """bootstrap is schema-only; the substrate never force-seeds on connect (ADR-0011 §4)."""
    con = lake_connect(lake_settings)
    try:
        # All four ledger tables exist...
        tables = {
            r[0]
            for r in con.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_catalog='ops' AND table_schema='lake_ops'"
            ).fetchall()
        }
        assert {"source", "run", "watermark", "dataset_contract"} <= tables
        # ...but the registry is empty — no producer's sources are force-registered.
        assert con.execute("SELECT count(*) FROM ops.lake_ops.source").fetchone()[0] == 0

        ops.bootstrap(con)  # a second bootstrap is still schema-only — must not seed
        assert con.execute("SELECT count(*) FROM ops.lake_ops.source").fetchone()[0] == 0
    finally:
        con.close()


def test_bootstrap_migrates_pre_writer_source_table(lake_settings: Settings):
    """bootstrap adds+backfills `writer` on a pre-PR `source` table (ADR-0011 §1 review)."""
    con = lake_connect(lake_settings)
    try:
        # Simulate the old schema: drop writer, seed a legacy row without it.
        con.execute("ALTER TABLE ops.lake_ops.source DROP COLUMN writer")
        con.execute(
            "INSERT INTO ops.lake_ops.source (name, lake_schema, registered_at) "
            "VALUES ('icite', 'icite', current_timestamp)"
        )
        ops.bootstrap(con)  # must ADD COLUMN IF NOT EXISTS + backfill

        assert con.execute(
            "SELECT writer FROM ops.lake_ops.source WHERE name = 'icite'"
        ).fetchone()[0] == "cdsci"
        # register_sources now works against the migrated table.
        ops.register_sources(con, writer="cdsci", sources=ops.SOURCES)
        assert con.execute(
            "SELECT count(*) FROM ops.lake_ops.source"
        ).fetchone()[0] == len(ops.SOURCES)
    finally:
        con.close()


def test_register_sources_populates_writer_and_is_idempotent(lake_settings: Settings):
    """Explicit register_sources seeds the writer column; a re-register refreshes, not dupes."""
    con = lake_connect(lake_settings)
    try:
        ops.register_sources(con, writer="cdsci", sources=ops.SOURCES)
        rows = con.execute("SELECT name, writer FROM ops.lake_ops.source").fetchall()
        assert {name for name, _ in rows} == {s.name for s in ops.SOURCES}
        assert {writer for _, writer in rows} == {"cdsci"}

        ops.register_sources(con, writer="cdsci", sources=ops.SOURCES)
        assert con.execute(
            "SELECT count(*) FROM ops.lake_ops.source"
        ).fetchone()[0] == len(ops.SOURCES)
    finally:
        con.close()


def test_read_only_skips_ops(lake_settings: Settings):
    """Read-only consumers don't attach the ledger (a writer concern)."""
    lake_connect(lake_settings).close()  # create the catalog first
    con = lake_connect(lake_settings, read_only=True)
    try:
        rows = con.execute("SELECT database_name FROM duckdb_databases()").fetchall()
        attached = {r[0] for r in rows}
        assert "lake" in attached
        assert "ops" not in attached
    finally:
        con.close()


def test_run_records_success_then_idempotent(lake_settings: Settings):
    """A real upsert → status 'success'; an identical re-run → 'idempotent'."""
    con = lake_connect(lake_settings)
    try:
        # icite is NOT pre-registered (connect no longer seeds) — the run must
        # self-register the built-in source so attribution is cdsci:icite.
        assert con.execute(
            "SELECT count(*) FROM ops.lake_ops.source WHERE name='icite'"
        ).fetchone()[0] == 0
        src = "SELECT * FROM (VALUES (1,'a'),(2,'b')) v(id,val)"
        with ops.run(con, source="icite", target="lake.main.t", version="2026-05") as r:
            r.rows = upsert(con, "lake.main.t", src, key="id")
        assert con.execute(
            "SELECT writer FROM ops.lake_ops.source WHERE name='icite'"
        ).fetchone()[0] == "cdsci"
        assert r.status == "success"
        assert r.changed is True
        assert r.summary() == {
            "table": "lake.main.t", "version": "2026-05", "rows": 2,
            "changed": True, "snapshot": r.snapshot_after, "run_id": r.run_id,
            "status": "success",
        }

        # The snapshot upsert produced is self-describing (ADR-0009): authored by
        # the source, op = the table name, and bound back to this run_id.
        author, extra = con.execute(
            "SELECT author, commit_extra_info FROM lake.snapshots() WHERE snapshot_id = ?",
            [r.snapshot_after],
        ).fetchone()
        assert author == "cdsci:icite"
        meta = json.loads(extra)
        assert meta["source"] == "icite" and meta["op"] == "t" and meta["run_id"] == r.run_id

        with ops.run(con, source="icite", target="lake.main.t", version="2026-06") as r2:
            r2.rows = upsert(con, "lake.main.t", src, key="id")  # same data
        assert r2.status == "idempotent"
        assert r2.changed is False
        assert r2.snapshot_after == r2.snapshot_before

        # Two run rows recorded; last_run returns the idempotent one.
        assert con.execute("SELECT count(*) FROM ops.lake_ops.run").fetchone()[0] == 2
        last = ops.last_run(con, "icite")
        assert last["status"] == "idempotent" and last["version"] == "2026-06"
        assert last["finished_at"] is not None
        # last success filter skips the idempotent run.
        assert ops.last_run(con, "icite", status="success")["version"] == "2026-05"
    finally:
        con.close()


def test_run_records_error_and_reraises(lake_settings: Settings):
    """A raise inside the block is recorded as 'error' and propagated."""
    con = lake_connect(lake_settings)
    try:
        with pytest.raises(ValueError, match="boom"), \
                ops.run(con, source="scp", target="lake.scp.incidence"):
            raise ValueError("boom")
        row = ops.last_run(con, "scp")
        assert row["status"] == "error"
        assert "boom" in row["error"]
        assert row["finished_at"] is not None
    finally:
        con.close()


def test_run_derives_writer_and_merges_extra(lake_settings: Settings):
    """run() derives writer from the registry and merges extra= into commit_extra_info."""
    con = lake_connect(lake_settings)
    try:
        src = "SELECT * FROM (VALUES (1,'a')) v(id,val)"
        with ops.run(
            con, source="icite", target="lake.main.t2", version="v1",
            extra={"prefect_run_id": "abc-123"},
        ) as r:
            assert r.writer == "cdsci"  # self-registered built-in, then derived
            r.rows = upsert(con, "lake.main.t2", src, key="id")

        author, extra = con.execute(
            "SELECT author, commit_extra_info FROM lake.snapshots() WHERE snapshot_id = ?",
            [r.snapshot_after],
        ).fetchone()
        assert author == "cdsci:icite"
        meta = json.loads(extra)
        assert meta["writer"] == "cdsci"
        # Per-producer key flows through on top of the canonical keys.
        assert meta["prefect_run_id"] == "abc-123"
    finally:
        con.close()


def test_run_foreign_producer_source(lake_settings: Settings):
    """A foreign producer registers its own source; run() attributes it, touches no cdsci rows."""
    con = lake_connect(lake_settings)
    try:
        ops.register_sources(
            con, writer="omicidx",
            sources=(ops.Source("sra", "omicidx", "SRA", "daily", "ncbi", "us-public-domain"),),
        )
        src = "SELECT * FROM (VALUES (1,'a')) v(id,val)"
        with ops.run(con, source="sra", target="lake.omicidx.sra", version="v1") as r:
            assert r.writer == "omicidx"
            r.rows = upsert(con, "lake.omicidx.sra", src, key="id")
        author = con.execute(
            "SELECT author FROM lake.snapshots() WHERE snapshot_id = ?", [r.snapshot_after]
        ).fetchone()[0]
        assert author == "omicidx:sra"
        # "sra" isn't in SOURCES → no self-registration, so no cdsci rows leaked in.
        names = {n for (n,) in con.execute("SELECT name FROM ops.lake_ops.source").fetchall()}
        assert names == {"sra"}
    finally:
        con.close()


def test_extra_cannot_override_canonical_keys(lake_settings: Settings):
    """A colliding `extra` key can't clobber a canonical key; other extras still flow."""
    con = lake_connect(lake_settings)
    try:
        src = "SELECT * FROM (VALUES (1,'a')) v(id,val)"
        with ops.run(
            con, source="icite", target="lake.main.t4", version="v1",
            extra={"writer": "nope", "prefect_run_id": "x"},
        ) as r:
            r.rows = upsert(con, "lake.main.t4", src, key="id")
        (extra,) = con.execute(
            "SELECT commit_extra_info FROM lake.snapshots() WHERE snapshot_id = ?",
            [r.snapshot_after],
        ).fetchone()
        meta = json.loads(extra)
        assert meta["writer"] == "cdsci"  # canonical wins over extra's "nope"
        assert meta["prefect_run_id"] == "x"  # non-colliding extra still flows
    finally:
        con.close()


def test_writer_for_raises_on_ambiguous_registration(lake_settings: Settings):
    """One name under two writers → run() fails loud rather than picking nondeterministically."""
    con = lake_connect(lake_settings)
    try:
        ops.register_sources(
            con, writer="cdsci",
            sources=(ops.Source("shared", "a", "d", "daily", "x", "y"),),
        )
        ops.register_sources(
            con, writer="omicidx",
            sources=(ops.Source("shared", "b", "d", "daily", "x", "y"),),
        )
        with pytest.raises(ValueError, match="multiple writers"), ops.run(
            con, source="shared", target="lake.x.y"
        ):
            pass
    finally:
        con.close()


def test_run_unregistered_source_falls_back_to_source_name(lake_settings: Settings):
    """A source neither in SOURCES nor registered → warns, writer defaults to its name."""
    con = lake_connect(lake_settings)
    try:
        with ops.run(con, source="not_a_real_source", target="lake.x.y") as r:
            assert r.writer == "not_a_real_source"
    finally:
        con.close()


def test_watermark_roundtrip(lake_settings: Settings):
    """Set/get a cursor; overwrite in place; missing returns None; values JSON-typed."""
    con = lake_connect(lake_settings)
    try:
        assert ops.get_watermark(con, "openalex", "updated_date") is None

        ops.set_watermark(con, "openalex", "updated_date", "2026-05-01")
        assert ops.get_watermark(con, "openalex", "updated_date") == "2026-05-01"

        # In-place overwrite — one row per (source, name).
        ops.set_watermark(con, "openalex", "updated_date", "2026-06-01")
        assert ops.get_watermark(con, "openalex", "updated_date") == "2026-06-01"
        assert con.execute(
            "SELECT count(*) FROM ops.lake_ops.watermark "
            "WHERE source='openalex' AND name='updated_date'"
        ).fetchone()[0] == 1

        # Non-scalar cursor round-trips through JSON.
        ops.set_watermark(con, "ctgov", "page_token", {"token": "abc", "page": 7})
        assert ops.get_watermark(con, "ctgov", "page_token") == {"token": "abc", "page": 7}
    finally:
        con.close()


def test_dashboard_read_surface(lake_settings: Settings):
    """The public read API a dashboard uses: list_sources/list_runs/get_run,
    and a read-only consumer opting into ops via with_ops=True."""
    con = lake_connect(lake_settings)
    try:
        ops.register_sources(con, writer="cdsci", sources=ops.SOURCES)
        src = "SELECT * FROM (VALUES (1,'a'),(2,'b')) v(id,val)"
        with ops.run(con, source="icite", target="lake.main.t", version="2026-05") as r:
            r.rows = upsert(con, "lake.main.t", src, key="id")
    finally:
        con.close()

    # Read-only consumer: lake is read-only but ops is attached for reads.
    con = lake_connect(lake_settings, read_only=True, with_ops=True)
    try:
        attached = {row[0] for row in con.execute(
            "SELECT database_name FROM duckdb_databases()").fetchall()}
        assert {"lake", "ops"} <= attached

        sources = ops.list_sources(con)
        assert {s["name"] for s in sources} == {s.name for s in ops.SOURCES}
        assert all(s["writer"] == "cdsci" for s in sources)

        runs = ops.list_runs(con, limit=10)
        assert len(runs) == 1
        assert runs[0]["status"] == "success" and runs[0]["run_id"] == r.run_id

        assert ops.get_run(con, r.run_id)["target"] == "lake.main.t"
        assert ops.get_run(con, "no-such-run") is None
    finally:
        con.close()


def test_bootstrap_asset_lineage_tables_idempotent(lake_settings: Settings):
    """asset + lineage land alongside the existing four tables; a second bootstrap is a no-op."""
    con = lake_connect(lake_settings)
    try:
        ops.bootstrap(con)  # already ran once via lake_connect; must not error/duplicate
        tables = {
            r[0]
            for r in con.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_catalog='ops' AND table_schema='lake_ops'"
            ).fetchall()
        }
        assert {"asset", "lineage"} <= tables
        assert con.execute("SELECT count(*) FROM ops.lake_ops.asset").fetchone()[0] == 0
        assert con.execute("SELECT count(*) FROM ops.lake_ops.lineage").fetchone()[0] == 0
    finally:
        con.close()


def test_register_asset_and_lineage_round_trip(lake_settings: Settings):
    """register_asset + record_lineage write rows the read helpers can see; run_id attributed."""
    con = lake_connect(lake_settings)
    try:
        src = "SELECT * FROM (VALUES (1,'a')) v(id,val)"
        with ops.run(con, source="icite", target="lake.icite.t", version="v1") as r:
            r.rows = upsert(con, "lake.icite.t", src, key="id")
            ops.register_asset(
                con, ref="lake.icite.t", writer="cdsci", asset_type="lake_table",
                name="icite.t", current_version=str(r.snapshot_after),
            )
            ops.register_asset(
                con, ref="r2://raw/icite/t.csv", writer="cdsci", asset_type="file",
                name="icite raw",
            )
            ops.record_lineage(
                con, src_ref="r2://raw/icite/t.csv", dst_ref="lake.icite.t", edge_type="declared",
            )

        assets = {a["ref"]: a for a in ops.list_assets(con)}
        assert set(assets) == {"lake.icite.t", "r2://raw/icite/t.csv"}
        assert assets["lake.icite.t"]["last_run_id"] == r.run_id
        assert assets["lake.icite.t"]["first_seen"] is not None

        upstream = ops.lineage_for(con, "lake.icite.t", direction="upstream")
        assert len(upstream) == 1
        assert upstream[0]["src_ref"] == "r2://raw/icite/t.csv"
        assert upstream[0]["edge_type"] == "declared"
        assert upstream[0]["run_id"] == r.run_id

        downstream = ops.lineage_for(con, "r2://raw/icite/t.csv", direction="downstream")
        assert len(downstream) == 1
        assert downstream[0]["dst_ref"] == "lake.icite.t"

        assert ops.lineage_for(con, "lake.icite.t", direction="downstream") == []
        with pytest.raises(ValueError, match="direction"):
            ops.lineage_for(con, "lake.icite.t", direction="sideways")
    finally:
        con.close()


def test_register_asset_preserves_first_seen_on_re_register(lake_settings: Settings):
    """Re-registering the same (writer, ref) refreshes fields but keeps first_seen."""
    con = lake_connect(lake_settings)
    try:
        ops.register_asset(
            con, ref="lake.icite.t", writer="cdsci", asset_type="lake_table", name="icite.t",
            current_version="1",
        )
        first_seen = ops.list_assets(con)[0]["first_seen"]

        ops.register_asset(
            con, ref="lake.icite.t", writer="cdsci", asset_type="lake_table", name="icite.t",
            current_version="2",
        )
        rows = ops.list_assets(con)
        assert len(rows) == 1
        assert rows[0]["current_version"] == "2"
        assert rows[0]["first_seen"] == first_seen
    finally:
        con.close()


def test_record_lineage_is_idempotent_on_src_dst_pair(lake_settings: Settings):
    """A second record_lineage for the same (src_ref, dst_ref) is a full no-op."""
    con = lake_connect(lake_settings)
    try:
        ops.record_lineage(
            con, src_ref="r2://raw/a.csv", dst_ref="lake.a.t", edge_type="declared",
        )
        first = ops.lineage_for(con, "lake.a.t")[0]

        ops.record_lineage(
            con, src_ref="r2://raw/a.csv", dst_ref="lake.a.t", edge_type="sqlmesh",
        )
        rows = ops.lineage_for(con, "lake.a.t")
        assert len(rows) == 1  # no duplicate row
        assert rows[0] == first  # edge_type/run_id/discovered_at untouched
    finally:
        con.close()


def test_record_publication_receipt_round_trips_and_is_idempotent(lake_settings: Settings):
    """record_publication_receipt writes one row keyed by (release_id, dataset_id, asset_ref);
    publication_receipts reads it back as a PublicationReceipt; a second record for the same
    release replaces rather than duplicates (cdsci-lake#100)."""
    con = lake_connect(lake_settings)
    try:
        receipt = PublicationReceipt(
            dataset="demo-catalog", release="R1", format="parquet",
            destination="demo-catalog/R1", schema_digest="sha256:abc", run_id="r1",
            status=ArtifactStatus.PUBLISHED, row_counts={"demo.events": 3},
        )
        receipt_id = ops.record_publication_receipt(con, receipt)
        assert receipt_id

        got = ops.publication_receipts(con, "R1")
        assert got == [receipt]
        assert con.execute(
            "SELECT asset_ref FROM ops.lake_ops.publication_receipt WHERE receipt_id = ?",
            [receipt_id],
        ).fetchone()[0] == "release.demo-catalog.R1"

        # Re-recording the same release replaces the one row (new receipt_id, still one row).
        updated = PublicationReceipt(
            dataset="demo-catalog", release="R1", format="parquet",
            destination="demo-catalog/R1", schema_digest="sha256:def", run_id="r2",
            status=ArtifactStatus.PUBLISHED, row_counts={"demo.events": 4},
        )
        ops.record_publication_receipt(con, updated)
        rows = ops.publication_receipts(con, "R1")
        assert rows == [updated]
    finally:
        con.close()


def test_record_publication_receipt_falls_back_to_active_run_id(lake_settings: Settings):
    """S4: a receipt with no run_id of its own picks up the enclosing run() block's
    run_id, so it matches register_asset()'s active-run-derived last_run_id."""
    con = lake_connect(lake_settings)
    try:
        src = "SELECT * FROM (VALUES (1,'a')) v(id,val)"
        with ops.run(con, source="icite", target="lake.icite.t", version="v1") as r:
            r.rows = upsert(con, "lake.icite.t", src, key="id")
            ops.register_asset(
                con, ref="release.demo-catalog.R1", writer="cdsci", asset_type="release",
                name="demo-catalog R1",
            )
            receipt = PublicationReceipt(
                dataset="demo-catalog", release="R1", format="parquet",
                destination="demo-catalog/R1", schema_digest="sha256:abc", run_id="",
                status=ArtifactStatus.PUBLISHED,
            )
            ops.record_publication_receipt(con, receipt)
            active_run_id = r.run_id

        receipt_run_id = con.execute(
            "SELECT run_id FROM ops.lake_ops.publication_receipt WHERE release_id = 'R1'"
        ).fetchone()[0]
        asset_run_id = con.execute(
            "SELECT last_run_id FROM ops.lake_ops.asset WHERE ref = 'release.demo-catalog.R1'"
        ).fetchone()[0]
        assert receipt_run_id == asset_run_id == active_run_id
    finally:
        con.close()


def test_asset_ref_validation_rejects_a_private_dsn(lake_settings: Settings):
    """A credential-bearing DSN is rejected before it reaches the ledger."""
    con = lake_connect(lake_settings)
    try:
        with pytest.raises(ValueError, match="credentials"):
            ops.register_asset(
                con, ref="postgresql://user:hunter2@internal-db:5432/lake",
                writer="cdsci", asset_type="postgres", name="leaky",
            )
        with pytest.raises(ValueError, match="invalid asset ref"):
            ops.register_asset(
                con, ref="  lake.a.t  ", writer="cdsci", asset_type="lake_table", name="a.t",
            )
        with pytest.raises(ValueError, match="invalid asset ref"):
            ops.register_asset(con, ref="", writer="cdsci", asset_type="lake_table", name="a.t")
        assert ops.list_assets(con) == []
    finally:
        con.close()


# --- sync_sqlmesh_snapshot_attribution (ADR-0008 Amendment; cdsci-lake#89) ---


def _create_unattributed_table(con, schema: str, table: str) -> None:
    """A raw DuckLake write bypassing `ops.run`/`upsert` -- stands in for a
    SQLMesh apply, whose snapshots likewise carry no `commit_extra_info`."""
    con.execute(f"CREATE SCHEMA IF NOT EXISTS lake.{schema};")
    con.execute(f"CREATE TABLE lake.{schema}.{table} AS SELECT 1 AS id")


def test_sync_sqlmesh_snapshot_attribution_brackets_exactly_the_matched_snapshots(
    lake_settings: Settings,
):
    """Attributes only the snapshot(s) that touch the target model's own table,
    not a neighbour model's snapshot created in the same bracketed window."""
    con = lake_connect(lake_settings)
    try:
        _create_unattributed_table(con, "bugsigdb", "signature")
        _create_unattributed_table(con, "ensembl", "gene")  # the neighbour

        run_id = ops.sync_sqlmesh_snapshot_attribution(
            con, project="cdsci_lake", model="bugsigdb.signature",
            target="lake.bugsigdb.signature", version="v1",
        )
        assert run_id is not None

        attributed = con.execute(
            "SELECT snapshot_id, run_id, source FROM ops.lake_ops.snapshot_attribution"
        ).fetchall()
        assert {r[1] for r in attributed} == {run_id}
        assert {r[2] for r in attributed} == {"sqlmesh_sync"}

        # Exactly the bugsigdb.signature snapshot -- confirm by cross-checking
        # against the catalog's own `changes` map, not just trusting our count.
        changed = dict(
            con.execute("SELECT snapshot_id, changes FROM lake.snapshots()").fetchall()
        )
        attributed_ids = {r[0] for r in attributed}
        for sid in attributed_ids:
            assert "bugsigdb.signature" in (changed[sid].get("tables_created") or [])
        assert all(
            "ensembl.gene" not in (changed[sid].get("tables_created") or [])
            for sid in attributed_ids
        )

        run = con.execute(
            "SELECT source, target, version, status FROM ops.lake_ops.run WHERE run_id = ?",
            [run_id],
        ).fetchone()
        assert run == ("bugsigdb.signature", "lake.bugsigdb.signature", "v1", "success")
    finally:
        con.close()


def test_sync_sqlmesh_snapshot_attribution_is_idempotent_on_rerun(lake_settings: Settings):
    con = lake_connect(lake_settings)
    try:
        _create_unattributed_table(con, "bugsigdb", "signature")
        first = ops.sync_sqlmesh_snapshot_attribution(
            con, project="cdsci_lake", model="bugsigdb.signature",
            target="lake.bugsigdb.signature",
        )
        assert first is not None

        second = ops.sync_sqlmesh_snapshot_attribution(
            con, project="cdsci_lake", model="bugsigdb.signature",
            target="lake.bugsigdb.signature",
        )
        assert second is None  # nothing new since the watermark -- a true no-op

        run_count = con.execute(
            "SELECT count(*) FROM ops.lake_ops.run WHERE source = 'bugsigdb.signature'"
        ).fetchone()[0]
        attribution_count = con.execute(
            "SELECT count(*) FROM ops.lake_ops.snapshot_attribution"
        ).fetchone()[0]
        assert run_count == 1
        assert attribution_count == 1
    finally:
        con.close()


def test_sync_sqlmesh_snapshot_attribution_rejects_a_non_cdsci_lake_project(
    lake_settings: Settings,
):
    con = lake_connect(lake_settings)
    try:
        _create_unattributed_table(con, "bugsigdb", "signature")
        with pytest.raises(ValueError, match="cdsci_lake-only"):
            ops.sync_sqlmesh_snapshot_attribution(
                con, project="omicidx", model="sradb.study", target="lake.sradb.study",
            )
        assert con.execute(
            "SELECT count(*) FROM ops.lake_ops.snapshot_attribution"
        ).fetchone()[0] == 0
    finally:
        con.close()


def test_snapshot_run_ids_reads_back_the_side_table(lake_settings: Settings):
    con = lake_connect(lake_settings)
    try:
        _create_unattributed_table(con, "bugsigdb", "signature")
        run_id = ops.sync_sqlmesh_snapshot_attribution(
            con, project="cdsci_lake", model="bugsigdb.signature",
            target="lake.bugsigdb.signature",
        )
        attributed_id = con.execute(
            "SELECT snapshot_id FROM ops.lake_ops.snapshot_attribution"
        ).fetchone()[0]

        result = ops.snapshot_run_ids(con, [attributed_id, 999999])
        assert result == {attributed_id: run_id}
        assert ops.snapshot_run_ids(con, []) == {}
    finally:
        con.close()


def test_sync_sqlmesh_snapshot_attribution_matches_a_data_only_change_by_table_id(
    lake_settings: Settings,
):
    """An INCREMENTAL/SCD2-shaped change reports only the internal table id in
    `changes` (no dotted `schema.table`) -- `_changed_tables` must resolve it via
    the catalog's own table/schema metadata, not just match full-refresh
    replaces (P1 finding 3)."""
    con = lake_connect(lake_settings)
    try:
        _create_unattributed_table(con, "bugsigdb", "signature")
        # A data-only write after the CREATE: DuckLake's `changes` for this
        # snapshot is `{"inlined_insert": ["<table_id>"]}` -- id-only, no dot.
        con.execute("INSERT INTO lake.bugsigdb.signature VALUES (2)")
        insert_snapshot_id = con.execute(
            "SELECT max(snapshot_id) FROM lake.snapshots()"
        ).fetchone()[0]
        changes = con.execute(
            "SELECT changes FROM lake.snapshots() WHERE snapshot_id = ?", [insert_snapshot_id]
        ).fetchone()[0]
        assert all("." not in v for vs in changes.values() for v in vs), (
            f"test setup expected an id-only change, got {changes}"
        )

        run_id = ops.sync_sqlmesh_snapshot_attribution(
            con, project="cdsci_lake", model="bugsigdb.signature",
            target="lake.bugsigdb.signature",
        )
        assert run_id is not None
        attributed_ids = {
            r[0] for r in con.execute(
                "SELECT snapshot_id FROM ops.lake_ops.snapshot_attribution"
            ).fetchall()
        }
        assert insert_snapshot_id in attributed_ids
    finally:
        con.close()


def test_sync_sqlmesh_snapshot_attribution_skips_snapshots_with_commit_extra_info(
    lake_settings: Settings,
):
    """A snapshot already attributed in-catalog (ADR-0008 §1) must not also get a
    side-table row -- the bracketing query excludes `commit_extra_info IS NOT
    NULL` (P1 finding 4)."""
    con = lake_connect(lake_settings)
    try:
        _create_unattributed_table(con, "bugsigdb", "signature")
        src = "SELECT 2 AS id"
        with ops.run(con, source="icite", target="lake.bugsigdb.signature", version="v1") as r:
            r.rows = upsert(con, "lake.bugsigdb.signature", src, key="id")
        assert r.changed is True

        run_id = ops.sync_sqlmesh_snapshot_attribution(
            con, project="cdsci_lake", model="bugsigdb.signature",
            target="lake.bugsigdb.signature",
        )
        assert run_id is not None
        attributed_ids = {
            r2[0] for r2 in con.execute(
                "SELECT snapshot_id FROM ops.lake_ops.snapshot_attribution"
            ).fetchall()
        }
        assert r.snapshot_after not in attributed_ids
    finally:
        con.close()


def test_sync_sqlmesh_snapshot_attribution_no_duplicate_rows_after_watermark_reset(
    lake_settings: Settings,
):
    """A watermark reset (e.g. a manual replay) re-brackets an already-attributed
    snapshot -- the side table must replace, not duplicate, its row (P2 finding 5)."""
    con = lake_connect(lake_settings)
    try:
        _create_unattributed_table(con, "bugsigdb", "signature")
        first_run_id = ops.sync_sqlmesh_snapshot_attribution(
            con, project="cdsci_lake", model="bugsigdb.signature",
            target="lake.bugsigdb.signature",
        )
        assert first_run_id is not None

        ops.set_watermark(con, "sqlmesh:cdsci_lake", "bugsigdb.signature", 0)
        second_run_id = ops.sync_sqlmesh_snapshot_attribution(
            con, project="cdsci_lake", model="bugsigdb.signature",
            target="lake.bugsigdb.signature",
        )
        assert second_run_id is not None
        assert second_run_id != first_run_id

        rows = con.execute(
            "SELECT snapshot_id, run_id FROM ops.lake_ops.snapshot_attribution"
        ).fetchall()
        by_snapshot: dict[int, list[str]] = {}
        for sid, rid in rows:
            by_snapshot.setdefault(sid, []).append(rid)
        assert all(len(rids) == 1 for rids in by_snapshot.values())  # no duplicates
        assert all(rids == [second_run_id] for rids in by_snapshot.values())  # latest wins
    finally:
        con.close()


def test_sync_sqlmesh_snapshot_attribution_run_times_from_snapshot_time(lake_settings: Settings):
    """The synthetic run row's started_at/finished_at come from the matched
    snapshots' own snapshot_time, not the sync call's wall-clock time
    (P2 finding 6; this preempts #86's separately-tracked run-row timestamps)."""
    con = lake_connect(lake_settings)
    try:
        _create_unattributed_table(con, "bugsigdb", "signature")
        run_id = ops.sync_sqlmesh_snapshot_attribution(
            con, project="cdsci_lake", model="bugsigdb.signature",
            target="lake.bugsigdb.signature",
        )
        assert run_id is not None
        snapshot_id = con.execute(
            "SELECT snapshot_id FROM ops.lake_ops.snapshot_attribution WHERE run_id = ?",
            [run_id],
        ).fetchone()[0]
        expected_time = con.execute(
            "SELECT snapshot_time FROM lake.snapshots() WHERE snapshot_id = ?", [snapshot_id]
        ).fetchone()[0]

        started_at, finished_at = con.execute(
            "SELECT started_at, finished_at FROM ops.lake_ops.run WHERE run_id = ?", [run_id]
        ).fetchone()
        assert started_at == expected_time
        assert finished_at == expected_time
    finally:
        con.close()


def test_sync_sqlmesh_snapshot_attribution_watermark_survives_a_crash_mid_write(
    lake_settings: Settings,
):
    """A crash between the run insert and the watermark write must leave the
    watermark untouched, so a retry re-scans and recovers instead of silently
    losing attribution (P1 finding 2)."""
    con = lake_connect(lake_settings)
    try:
        _create_unattributed_table(con, "bugsigdb", "signature")

        class _CrashOnRunInsert:
            """Wraps `con`, raising once when the run-row INSERT executes."""

            def __init__(self, inner):
                self._inner = inner
                self._armed = True

            def execute(self, sql, params=None):
                if self._armed and "INSERT INTO ops.lake_ops.run" in sql:
                    self._armed = False
                    raise RuntimeError("boom")
                if params is None:
                    return self._inner.execute(sql)
                return self._inner.execute(sql, params)

            def executemany(self, sql, params):
                return self._inner.executemany(sql, params)

        with pytest.raises(RuntimeError, match="boom"):
            ops.sync_sqlmesh_snapshot_attribution(
                _CrashOnRunInsert(con), project="cdsci_lake", model="bugsigdb.signature",
                target="lake.bugsigdb.signature",
            )
        assert ops.get_watermark(con, "sqlmesh:cdsci_lake", "bugsigdb.signature") is None
        assert con.execute(
            "SELECT count(*) FROM ops.lake_ops.snapshot_attribution"
        ).fetchone()[0] == 0

        # Retry (unwrapped) succeeds and attributes the same snapshot the crash lost.
        run_id = ops.sync_sqlmesh_snapshot_attribution(
            con, project="cdsci_lake", model="bugsigdb.signature",
            target="lake.bugsigdb.signature",
        )
        assert run_id is not None
    finally:
        con.close()
