"""cdsci.publish.gate (ADR-0027 step 4c): a release bundle against the previous release,
offline. The generic cases of bioc-on-ice's and cancer-on-ice's test_gate.py, on tiny
Parquet + manifest.json bundles under tmp_path."""

import hashlib
import io
import json
import urllib.error
import urllib.request

import duckdb
import pytest
from cdsci.publish import gate as g

TAXON = (
    "SELECT 'bsdb:' || (i // 3) || '/1/1' AS bsdb_id, (i % 3 + 1)::BIGINT AS member_index, "
    "'Taxon ' || i AS taxon_name FROM range({n}) t(i)"
)
PROVENANCE = "SELECT 'bugsigdb' AS source, {n}::BIGINT AS row_count"
HISTORY = ("SELECT i::VARCHAR AS pmid, '2026.09' AS valid_from, NULL::VARCHAR AS valid_to "
           "FROM range(5) t(i)")
ROW_HISTORY = ("valid_from", "valid_to", "landed_in")


def _bundle(root, release, tables):
    """Write `tables` (name -> SELECT) as a release bundle under `root`."""
    con = duckdb.connect()
    entries = []
    for name, sql in tables.items():
        schema, table = name.split(".")
        rel = f"{schema}/{table}.parquet"
        (root / schema).mkdir(parents=True, exist_ok=True)
        n = con.execute(f"COPY ({sql}) TO '{root / rel}' (FORMAT parquet)").fetchone()[0]
        cols = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{root / rel}')").fetchall()
        with (root / rel).open("rb") as f:
            sha256 = hashlib.file_digest(f, "sha256").hexdigest()
        entries.append({"schema": schema, "table": table, "path": rel, "row_count": n,
                        "sha256": sha256, "comment": None,
                        "columns": [{"name": c[0], "type": c[1], "doc": None} for c in cols]})
    (root / "manifest.json").write_text(json.dumps({"release": release, "tables": entries}))
    return root


@pytest.fixture
def reference(tmp_path):
    return _bundle(tmp_path / "v2026-10-31", "2026-10-31", {
        "annotation.signature_taxon": TAXON.format(n=30),
        "provenance.release": PROVENANCE.format(n=1)})


def candidate(tmp_path, n=33, **tables):
    return _bundle(tmp_path / "candidate", "2026-11-01", {
        "annotation.signature_taxon": TAXON.format(n=n),
        "provenance.release": PROVENANCE.format(n=1), **tables})


KEYS = {"annotation.signature_taxon": ("bsdb_id", "member_index"),
        "provenance.release": ("source",),
        "annotation.not_in_bundle": ("x",)}


def test_grown_release_passes_and_gate_json_binds_the_manifest(tmp_path, reference, capsys):
    bundle = candidate(tmp_path)
    (bundle / g.GATE_FILE).write_text("stale")
    passed, checks = g.gate(bundle, reference, keys=KEYS, catalog="bioc", queries=[
        (7, "SELECT count(*) FROM bioc.annotation.signature_taxon"),
        (12, "SELECT * FROM annotation.signature_taxon"),
        (20, "SELECT * FROM annotation.missing")])
    assert passed
    out = capsys.readouterr().out
    assert out.startswith(f"release 2026-11-01 against {reference} (release 2026-10-31)\n")
    assert "annotation.signature_taxon\n" in out and "  ok    key_unique" in out
    assert "  ok    manifest_row_count  (parquet 33, manifest 33)" in out
    assert "  ok    row_count_bounds  (33 rows, reference 30, bounds (0.9, 1.5))" in out
    assert "  ok    readme_query_line_7  (1 rows)" in out
    assert "  ok    readme_query_line_12  (33 rows)" in out
    # A README query that fails is a warning only.
    assert "  warn  readme_query_line_20  (CatalogException)" in out
    assert out.endswith("PASS\n")
    # A key for a table not in the bundle is ignored.
    assert not any("not_in_bundle" in c.name for c in checks)
    proof = json.loads((bundle / g.GATE_FILE).read_text())
    assert proof["manifest_sha256"] == hashlib.sha256(
        (bundle / "manifest.json").read_bytes()).hexdigest()
    assert proof["release"] == "2026-11-01" and proof["reference_release"] == "2026-10-31"
    assert proof["reference"] == str(reference)
    assert proof["schema_changes"] == {} and proof["column_changes"] == {}
    assert g.check_proof(bundle) == proof


def test_check_proof_refuses_a_missing_or_stale_gate_json(tmp_path):
    bundle = candidate(tmp_path)
    with pytest.raises(SystemExit, match="no gate.json"):
        g.check_proof(bundle)
    (bundle / g.GATE_FILE).write_text('{"manifest_sha256": "0"}')
    with pytest.raises(SystemExit, match="another manifest.json"):
        g.check_proof(bundle)


def test_a_reference_not_older_than_the_bundle_is_refused(reference, capsys):
    assert g.gate(reference, reference) == (False, [])
    assert "reference release 2026-10-31 is not older than 2026-10-31" in capsys.readouterr().out
    assert not (reference / g.GATE_FILE).exists()


@pytest.mark.parametrize("bad", ["0000-00-00", None])
def test_a_malformed_release_id_is_refused(tmp_path, reference, capsys, bad):
    manifest = json.loads((reference / "manifest.json").read_text())
    (reference / "manifest.json").write_text(json.dumps({**manifest, "release": bad}))
    assert g.gate(candidate(tmp_path), reference) == (False, [])
    assert "release ids must be YYYY-MM-DD dates" in capsys.readouterr().out


def test_a_manifest_row_count_that_disagrees_with_its_parquet_fails(tmp_path, reference, capsys):
    bundle = candidate(tmp_path, n=30)
    m = json.loads((bundle / "manifest.json").read_text())
    m["tables"][0]["row_count"] = 31
    (bundle / "manifest.json").write_text(json.dumps(m))
    passed, _ = g.gate(bundle, reference)
    assert not passed
    assert "  FAIL  manifest_row_count  (parquet 30, manifest 31)" in capsys.readouterr().out
    assert not (bundle / g.GATE_FILE).exists()


def test_dropped_table_fails(tmp_path, reference, capsys):
    bundle = _bundle(tmp_path / "candidate", "2026-11-01",
                     {"annotation.signature_taxon": TAXON.format(n=30)})
    assert not g.gate(bundle, reference)[0]
    out = capsys.readouterr().out
    assert "provenance.release\n  FAIL  present  (in the previous release, missing" in out
    assert out.endswith("FAIL: 1 required check(s) failed\n")


@pytest.mark.parametrize("ref_rows, rows, growth_required, passed, expect", [
    (1000, 850, True, False, "  FAIL  row_count_bounds  (850 rows, reference 1000"),
    (1000, 920, True, True, "  ok    row_count_bounds  (920 rows, reference 1000"),
    (1000, 5000, True, False, "  FAIL  row_count_growth  (5000 rows, reference 1000"),
    (1000, 5000, False, True, "  warn  row_count_growth  (5000 rows, reference 1000"),
    (0, 5, True, True, "  ok    row_count_growth  (5 rows, reference 0"),
])
def test_row_count_bounds(tmp_path, capsys, ref_rows, rows, growth_required, passed, expect):
    ref = _bundle(tmp_path / "ref", "2026-10-01", {"geography.unit": TAXON.format(n=ref_rows)})
    bundle = _bundle(tmp_path / "candidate", "2026-11-01",
                     {"geography.unit": TAXON.format(n=rows)})
    assert g.gate(bundle, ref, growth_required=growth_required)[0] is passed
    assert expect in capsys.readouterr().out


def test_cancer_bounds_shrink_to_0_8_passes(tmp_path):
    ref = _bundle(tmp_path / "ref", "2026-10-01", {"geography.unit": TAXON.format(n=1000)})
    bundle = _bundle(tmp_path / "candidate", "2026-11-01", {"geography.unit": TAXON.format(n=850)})
    assert g.gate(bundle, ref, row_bounds=(0.8, 3.0))[0]


def test_duplicate_and_null_keys_fail(tmp_path, reference, capsys):
    bundle = candidate(tmp_path, n=30, **{"annotation.signature_taxon": (
        TAXON.format(n=30) + " UNION ALL " + TAXON.format(n=1)
        + " UNION ALL SELECT NULL, 1, 'x'")})
    assert not g.gate(bundle, reference, keys=KEYS)[0]
    out = capsys.readouterr().out
    assert "  FAIL  key_unique  (duplicated keys: 1)" in out
    assert "  FAIL  key_not_null  (rows with a null key column: 1)" in out


def test_references_covered(tmp_path, reference, capsys):
    bundle = candidate(tmp_path, **{
        "annotation.member": "SELECT 'bsdb:0/1/1' AS bsdb_id UNION ALL SELECT 'bsdb:999/1/1' "
                             "UNION ALL SELECT NULL"})
    refs = {"annotation.member": {"bsdb_id": "annotation.signature_taxon.bsdb_id"},
            "annotation.signature_taxon": {"bsdb_id": "annotation.gone.bsdb_id"}}
    assert not g.gate(bundle, reference, references=refs)[0]
    out = capsys.readouterr().out
    assert ("  FAIL  bsdb_id.references_covered  (distinct values missing from "
            "annotation.signature_taxon.bsdb_id: 1)") in out
    assert "  FAIL  bsdb_id.references_covered  (target table annotation.gone not in" in out


def _with_history(tmp_path, candidate_pubmed):
    ref = _bundle(tmp_path / "ref", "2026-10-31", {
        "annotation.pubmed__article": HISTORY, "provenance.release": PROVENANCE.format(n=1)})
    bundle = _bundle(tmp_path / "candidate", "2026-11-01", {
        "annotation.pubmed__article": candidate_pubmed,
        "provenance.release": PROVENANCE.format(n=1)})
    return bundle, ref


def test_an_allowed_table_may_drop_history_columns(tmp_path, capsys):
    bundle, ref = _with_history(tmp_path, "SELECT i::VARCHAR AS pmid FROM range(5) t(i)")
    assert g.gate(bundle, ref, allow_dropped={"annotation.pubmed__article": ROW_HISTORY})[0]
    out = capsys.readouterr().out
    assert "  ok    columns_unchanged  (schema change allowed: dropped valid_from, valid_to)" in out
    assert "annotation.pubmed__article drops valid_from, valid_to" in out
    assert json.loads((bundle / g.GATE_FILE).read_text())["schema_changes"] == {
        "annotation.pubmed__article": ["valid_from", "valid_to"]}


def test_dropping_columns_from_an_unlisted_table_fails(tmp_path, capsys):
    bundle, ref = _with_history(tmp_path, "SELECT i::VARCHAR AS pmid FROM range(5) t(i)")
    assert not g.gate(bundle, ref, allow_dropped={"provenance.release": ROW_HISTORY})[0]
    out = capsys.readouterr().out
    assert "  FAIL  columns_unchanged  (differs: removed valid_from VARCHAR, valid_to VARCHAR)" in (
        out)
    assert "provenance.release drops nothing" in out


def test_another_column_change_fails_even_when_listed_for_drops(tmp_path, capsys):
    bundle, ref = _with_history(tmp_path,
                                "SELECT i::VARCHAR AS pmid, 1 AS extra FROM range(5) t(i)")
    assert not g.gate(bundle, ref, allow_dropped={"annotation.pubmed__article": ROW_HISTORY})[0]
    assert "  FAIL  columns_unchanged  (differs: added extra INTEGER; removed" in (
        capsys.readouterr().out)


def _retyped(tmp_path, n=5):
    return _with_history(tmp_path, f"SELECT i AS pmid FROM range({n}) t(i)")


def test_an_allowed_column_change_passes_and_is_recorded(tmp_path, capsys):
    bundle, ref = _retyped(tmp_path)
    assert g.gate(bundle, ref, allow_column_change=("annotation.pubmed__article",))[0]
    out = capsys.readouterr().out
    detail = "removed valid_from VARCHAR, valid_to VARCHAR; retyped pmid VARCHAR -> BIGINT"
    assert f"  ok    columns_unchanged  (column change allowed: {detail})" in out
    assert f"column change allowed (owner approval): annotation.pubmed__article: {detail}" in out
    assert json.loads((bundle / g.GATE_FILE).read_text())["column_changes"] == {
        "annotation.pubmed__article": {
            "added": {}, "removed": {"valid_from": "VARCHAR", "valid_to": "VARCHAR"},
            "retyped": {"pmid": {"from": "VARCHAR", "to": "BIGINT"}}, "reordered": False}}


def test_a_column_change_of_an_unlisted_table_fails(tmp_path, capsys):
    bundle, ref = _retyped(tmp_path)
    assert not g.gate(bundle, ref, allow_column_change=("provenance.release",))[0]
    assert "  FAIL  columns_unchanged  (differs:" in capsys.readouterr().out
    assert not (bundle / g.GATE_FILE).exists()


def test_an_allowed_column_change_still_fails_row_bounds(tmp_path, capsys):
    bundle, ref = _retyped(tmp_path, n=50)
    assert not g.gate(bundle, ref, allow_column_change=("annotation.pubmed__article",))[0]
    out = capsys.readouterr().out
    assert "  ok    columns_unchanged  (column change allowed:" in out
    assert "  FAIL  row_count_growth  (50 rows, reference 5" in out


def test_column_diff_records_added_renamed_and_reordered():
    assert g.column_diff([("a", "INTEGER"), ("b", "VARCHAR"), ("c", "DATE")],
                         [("b", "VARCHAR"), ("a", "INTEGER"), ("d", "DATE")]) == {
        "added": {"d": "DATE"}, "removed": {"c": "DATE"}, "retyped": {}, "reordered": True}


def test_reference_none_runs_bundle_checks_only(tmp_path, capsys):
    bundle = candidate(tmp_path)
    assert g.gate(bundle, None)[0]
    out = capsys.readouterr().out
    assert "bundle checks only, no reference given" in out
    assert "present" not in out and "reference_has_tables" not in out
    assert json.loads((bundle / g.GATE_FILE).read_text())["reference"] is None


def test_an_empty_reference_fails(tmp_path, capsys):
    ref = tmp_path / "ref"
    ref.mkdir()
    (ref / "manifest.json").write_text('{"release": "2026-10-01", "tables": []}')
    assert not g.gate(candidate(tmp_path), ref)[0]
    assert "  FAIL  reference_has_tables  (reference manifest lists no tables)" in (
        capsys.readouterr().out)


def test_a_local_reference_without_a_manifest_fails_closed(tmp_path, capsys):
    bundle = candidate(tmp_path)
    assert g.gate(bundle, tmp_path / "nowhere") == (False, [])
    assert "no readable manifest.json (FileNotFoundError)" in capsys.readouterr().out
    assert not (bundle / g.GATE_FILE).exists()


def _serve(monkeypatch, status, body, seen=None):
    def urlopen(req):
        if seen is not None:
            seen.append(req)
        if status == "urlerror":
            raise urllib.error.URLError("unreachable")
        if status != 200:
            raise urllib.error.HTTPError(req.full_url, status, "x", {}, None)
        return io.BytesIO(body)

    monkeypatch.setattr(urllib.request, "urlopen", urlopen)


@pytest.mark.parametrize("status, body", [
    (200, b'["not", "a", "manifest"]'), (200, b'{"release": "2026-10-01"}'), (200, b"<html>"),
    (500, b""), ("urlerror", b"")])
def test_an_unreadable_http_reference_fails_closed(tmp_path, monkeypatch, capsys, status, body):
    _serve(monkeypatch, status, body)
    bundle = candidate(tmp_path)
    assert g.gate(bundle, "http://ref.example/latest") == (False, [])
    assert "no readable manifest.json" in capsys.readouterr().out
    assert not (bundle / g.GATE_FILE).exists()


def test_no_published_release_404_runs_bundle_checks_only(tmp_path, monkeypatch, capsys):
    _serve(monkeypatch, 404, b"")
    bundle = candidate(tmp_path)
    assert g.gate(bundle, "https://ref.example/data/latest/")[0]
    assert "bundle checks only, no published release at https://ref.example/data/latest/" in (
        capsys.readouterr().out)
    assert json.loads((bundle / g.GATE_FILE).read_text())["reference_release"] is None


def test_the_reference_is_fetched_with_our_own_user_agent(tmp_path, reference, monkeypatch):
    """Cloudflare refuses urllib's default User-Agent (HTTP 403, error 1010) on the data host."""
    seen = []
    _serve(monkeypatch, 200, (reference / "manifest.json").read_bytes(), seen)
    assert g.gate(candidate(tmp_path), "https://ref.example/latest", user_agent="me/1")[0]
    assert seen[0].full_url == "https://ref.example/latest/manifest.json"
    assert seen[0].get_header("User-agent") == "me/1"


def test_a_manifest_changed_during_the_check_is_not_certified(tmp_path, reference, capsys):
    bundle = candidate(tmp_path)

    def mutate(con, manifest, ref):
        m = json.loads((bundle / "manifest.json").read_text())
        (bundle / "manifest.json").write_text(json.dumps({**m, "release": "2026-11-02"}))
        return []

    assert not g.gate(bundle, reference, extra_checks=mutate)[0]
    assert "manifest.json changed while the gate ran" in capsys.readouterr().out
    assert not (bundle / g.GATE_FILE).exists()


def test_extra_checks_are_run_and_reported(tmp_path, reference, capsys):
    def domain(con, manifest, ref):
        n = con.execute("SELECT count(*) FROM annotation.signature_taxon "
                        "WHERE member_index > 3").fetchone()[0]
        assert ref["release"] == "2026-10-31"
        return [g.Check("annotation.signature_taxon.member_index_in_range", n == 0,
                        detail=f"{n} rows"),
                g.Check("annotation.signature_taxon.contract_declared", False, False,
                        "no contract")]

    assert g.gate(candidate(tmp_path), reference, extra_checks=domain)[0]
    out = capsys.readouterr().out
    assert "  ok    member_index_in_range  (0 rows)" in out
    assert "  warn  contract_declared  (no contract)" in out


def test_readme_queries_by_fence(tmp_path):
    readme = tmp_path / "README.md"
    readme.write_text("```sql\nATTACH 'x' AS y\n```\n\n```sql\nSELECT 1;\n```\n\n"
                      "```sql bundle\nSELECT count(*) FROM measure.observation;\n```\n")
    assert g.readme_queries(readme) == [(6, "SELECT 1")]
    assert g.readme_queries(readme, "sql bundle") == [
        (10, "SELECT count(*) FROM measure.observation")]
    assert g.readme_queries(tmp_path / "nonexistent" / "README.md") == []


def test_a_manifest_path_outside_the_bundle_is_refused(tmp_path):
    bundle = candidate(tmp_path)
    m = json.loads((bundle / "manifest.json").read_text())
    m["tables"][0]["path"] = "../secret.parquet"
    (bundle / "manifest.json").write_text(json.dumps(m))
    with pytest.raises(ValueError, match="relative to the bundle"):
        g.gate(bundle, None)
