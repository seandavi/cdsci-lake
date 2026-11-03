"""Offline tests for the IntAct source (bioc-on-ice#183).

The fixture is written at test time in the real files' dialect (probed on the
2026-01-09 and 2026-10-17 releases, 2026-11-03): a zip holding ``intact.txt``
and ``intact_negative.txt`` under one MITAB 2.7 header (starting with '#'),
tab-separated, LF, no quoting ('"' is data inside cells), '-' for missing,
lists '|'-joined.
"""

from __future__ import annotations

import importlib
import shutil
import zipfile
from pathlib import Path

import pytest

from cdsci.lake import Settings, lake_connect
from cdsci.lake.sources import intact

# the module itself: the package's `ingest` attribute is the function
INGEST = importlib.import_module("cdsci.lake.sources.intact.ingest")

SNAP = "SELECT max(snapshot_id) FROM lake.snapshots()"
ROWS = ("SELECT file_name, line_number, id_a, intact_version FROM lake.intact.interaction "
        "ORDER BY file_name, line_number")
NAMES = list(intact.COLUMNS.values())


def line(**cells: str) -> str:
    return "\t".join(cells.get(c, "-") for c in NAMES)


# cells of the first row of 2026-01-09, verbatim: '"' inside CV terms, '|'-lists
R1 = line(id_a="uniprotkb:O43426", id_b="uniprotkb:P49418",
          aliases_b="psi-mi:amph_human(display_long)|uniprotkb:AMPH(gene name)",
          detection_methods='psi-mi:"MI:0084"(phage display)', first_authors="Cestra et al. (1999)",
          publication_ids="pubmed:10542231|mint:MINT-5211933",
          taxid_a="taxid:9606(human)|taxid:9606(Homo sapiens)",
          taxid_b="taxid:9606(human)|taxid:9606(Homo sapiens)",
          interaction_types='psi-mi:"MI:0407"(direct interaction)',
          interaction_ids="intact:EBI-7121552|mint:MINT-16056",
          confidence_values="intact-miscore:0.67",
          host_organisms="taxid:-1(in vitro)|taxid:-1(In vitro)",
          interaction_annotations='comment:"Stoichiometry: 1.0"', creation_date="2001/01/10",
          update_date="2025/01/15", negative="false",
          features_a="binding-associated region:1063-1070(MINT-376288)")
# a single-participant interaction: no interactor B; "'" and a hyphenated value are data
R2 = line(id_a="uniprotkb:P04637-2", taxid_a="taxid:9606(human)",
          interaction_ids="intact:EBI-100", first_authors="O'Brien et al. (2001)",
          expansion_methods='psi-mi:"MI:1060"(spoke expansion)', negative="false")
R3 = line(id_a='chebi:"CHEBI:15422"', id_b="intact:EBI-123", interaction_ids="intact:EBI-200",
          negative="false")
NEG = line(id_a="uniprotkb:Q9Y6K9", id_b="uniprotkb:O14920", interaction_ids="intact:EBI-900",
           negative="true")


def text(path: Path, rows: list[str], header=None) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(r + "\n" for r in ["\t".join(header or intact.COLUMNS), *rows]))
    return path


def release(root: Path, rows: list[str], negative: list[str]) -> list[Path]:
    """A release's two unzipped MITAB files."""
    return [text(root / "intact.txt", rows), text(root / "intact_negative.txt", negative)]


def zipped(root: Path, version: str, rows: list[str], negative: list[str]) -> Path:
    """``<root>/<version>/psimitab/intact.zip`` holding the two files."""
    d = root / version / "psimitab"
    d.mkdir(parents=True, exist_ok=True)
    path = d / "intact.zip"
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as z:
        for name, body in (("intact.txt", rows), ("intact_negative.txt", negative)):
            z.writestr(name, "".join(r + "\n" for r in ["\t".join(intact.COLUMNS), *body]))
    return path


@pytest.fixture
def lake_settings(tmp_path: Path) -> Settings:
    # lake_backend pinned: CU_OPENALEX_LAKE_BACKEND=postgres in the environment or a
    # .env would otherwise point these writes at the shared lake.
    return Settings(storage_base_uri=f"file://{tmp_path}", lake_backend="local",
                    lake_catalog=str(tmp_path / "catalog.ducklake"))


def seed(con, tmp_path: Path) -> int:
    intact.curate(con, release(tmp_path / "seed", [R1, R2], [NEG]), "2026-01-09")
    return con.execute(SNAP).fetchone()[0]


def test_intact_lands_whole_and_a_new_release_replaces_the_last(lake_settings, tmp_path):
    # R3 twice: a whole-row duplicate, kept as published
    z = zipped(tmp_path / "in", "2026-01-09", [R1, R2, R3, R3], [NEG])
    result = intact.ingest(file=str(z), settings=lake_settings)
    assert (result["version"], result["rows"]) == ("2026-01-09", 5)

    con = lake_connect(lake_settings)
    try:
        # the key, then raw's columns, names and types, without landed_in
        assert [(c[0], c[1]) for c in con.execute(
            "DESCRIBE lake.intact.interaction").fetchall()] == [
            ("file_name", "VARCHAR"), ("line_number", "INTEGER"),
            *[(c, "VARCHAR") for c in NAMES], ("intact_version", "VARCHAR")]
        # every published row as a multiset, duplicates included: '-' is NULL,
        # '"' and "'" are data, '|'-lists stay joined
        published = [[None if v == "-" else v for v in r.split("\t")]
                     for r in (R1, R2, R3, R3, NEG)]
        con.execute(f"CREATE TEMP TABLE published ({', '.join(f'{c} VARCHAR' for c in NAMES)})")
        con.executemany(f"INSERT INTO published VALUES ({', '.join('?' * len(NAMES))})", published)
        landed = ("SELECT * EXCLUDE (file_name, line_number, intact_version) "
                  "FROM lake.intact.interaction")
        for a, b in ((landed, "SELECT * FROM published"), ("SELECT * FROM published", landed)):
            assert con.execute(f"SELECT count(*) FROM ({a} EXCEPT ALL {b})").fetchone()[0] == 0
        assert con.execute("""
            SELECT detection_methods, interaction_annotations, publication_ids, id_b
            FROM lake.intact.interaction WHERE line_number = 1 AND file_name = 'intact.txt'
        """).fetchone() == ('psi-mi:"MI:0084"(phage display)', 'comment:"Stoichiometry: 1.0"',
                            "pubmed:10542231|mint:MINT-5211933", "uniprotkb:P49418")
        assert con.execute("""
            SELECT id_b, taxid_b, first_authors FROM lake.intact.interaction
            WHERE interaction_ids = 'intact:EBI-100'""").fetchone() == (
            None, None, "O'Brien et al. (2001)")
        assert con.execute(ROWS).fetchall() == [
            ("intact.txt", 1, "uniprotkb:O43426", "2026-01-09"),
            ("intact.txt", 2, "uniprotkb:P04637-2", "2026-01-09"),
            ("intact.txt", 3, 'chebi:"CHEBI:15422"', "2026-01-09"),
            ("intact.txt", 4, 'chebi:"CHEBI:15422"', "2026-01-09"),
            ("intact_negative.txt", 1, "uniprotkb:Q9Y6K9", "2026-01-09"),
        ]

        # re-landing the same release writes nothing
        before = con.execute(SNAP).fetchone()[0]
        paths = sorted((INGEST.raw_dir("intact", lake_settings) / "2026-01-09").glob("*.txt"))
        assert [p.name for p in paths] == ["intact.txt", "intact_negative.txt"]
        assert intact.curate(con, paths, "2026-01-09") == 5
        assert con.execute(SNAP).fetchone()[0] == before

        # a limited load never prunes (and returns the table's row count, not the rows
        # written); a full load of the next release drops withdrawn lines and restamps
        nxt = release(tmp_path / "next", [R1, R3], [NEG])
        assert intact.curate(con, nxt, "2026-10-17", limit=1) == 5
        assert intact.curate(con, nxt, "2026-10-17") == 3
        assert con.execute(ROWS).fetchall() == [
            ("intact.txt", 1, "uniprotkb:O43426", "2026-10-17"),
            ("intact.txt", 2, 'chebi:"CHEBI:15422"', "2026-10-17"),
            ("intact_negative.txt", 1, "uniprotkb:Q9Y6K9", "2026-10-17"),
        ]
    finally:
        con.close()


def test_line_number_is_the_physical_line_under_parallel_scans(tmp_path):
    # cdsci-lake#143: the key relies on row_number() OVER () following file order
    # with preserve_insertion_order=false and several threads
    s = Settings(storage_base_uri=f"file://{tmp_path}", lake_backend="local",
                 lake_catalog=str(tmp_path / "catalog.ducklake"), duckdb_threads=8)
    rows = [line(id_a=f"n:{i}", xrefs_a="x" * (i % 97)) for i in range(1, 300_001)]
    paths = release(tmp_path / "big", rows, [NEG])
    con = lake_connect(s)
    try:
        assert intact.curate(con, paths, "2026-01-09") == 300_001
        assert con.execute("""
            SELECT count(*) FROM lake.intact.interaction
            WHERE file_name = 'intact.txt' AND id_a <> 'n:' || line_number""").fetchone()[0] == 0
        before = con.execute(SNAP).fetchone()[0]
        assert intact.curate(con, paths, "2026-01-09") == 300_001
        assert con.execute(SNAP).fetchone()[0] == before
    finally:
        con.close()


def test_a_malformed_file_fails_before_landing_and_keeps_the_table(lake_settings, tmp_path):
    con = lake_connect(lake_settings)
    try:
        before = seed(con, tmp_path)
        cols = list(intact.COLUMNS)
        swapped = [cols[1], cols[0], *cols[2:]]
        for name, header, match in (
            ("added", [*cols, "New Column"], "New Column"),
            ("dropped", cols[:-1], "Identification method participant B"),
            ("swapped", swapped, "column order"),
        ):
            bad = [text(tmp_path / name / "intact.txt", [R1], header),
                   text(tmp_path / name / "intact_negative.txt", [NEG])]
            with pytest.raises(ValueError, match=match):
                intact.curate(con, bad, "2026-10-17", limit=1)
        # an extra cell, a missing one, a blank line: past a limited load's rows, and in
        # the second file when the first is sound, so only the whole-input check sees it
        padding = [line(id_a=f"n:{i}", interaction_ids=f"intact:EBI-{i}") for i in range(20_000)]
        for name, row in (("extra", R3 + "\tstray"), ("short", R3.rsplit("\t", 1)[0]),
                          ("blank", "")):
            for bad in (release(tmp_path / name / "a", [R1, *padding, row], [NEG]),
                        release(tmp_path / name / "b", [R1], [NEG, row])):
                for limit in (None, 1):
                    with pytest.raises(ValueError, match="data line"):
                        intact.curate(con, bad, "2026-10-17", limit=limit)
        assert con.execute(SNAP).fetchone()[0] == before
        assert con.execute(ROWS).fetchall() == [
            ("intact.txt", 1, "uniprotkb:O43426", "2026-01-09"),
            ("intact.txt", 2, "uniprotkb:P04637-2", "2026-01-09"),
            ("intact_negative.txt", 1, "uniprotkb:Q9Y6K9", "2026-01-09"),
        ]
    finally:
        con.close()


def test_a_header_only_file_is_refused_and_the_table_kept(lake_settings, tmp_path):
    con = lake_connect(lake_settings)
    try:
        before = seed(con, tmp_path)
        for rows, negative in (([], [NEG]), ([R1], []), ([], [])):
            empty = release(tmp_path / f"empty{len(rows)}{len(negative)}", rows, negative)
            for limit in (None, 1):
                with pytest.raises(ValueError, match="zero rows"):
                    intact.curate(con, empty, "2026-10-17", limit=limit)
        with pytest.raises(ValueError, match="no MITAB files"):
            intact.curate(con, [], "2026-10-17")
        for limit in (0, -1):
            with pytest.raises(ValueError, match="limit must be positive"):
                intact.curate(con, release(tmp_path / "one", [R3], [NEG]), "2026-10-17",
                              limit=limit)
        assert con.execute(SNAP).fetchone()[0] == before
        assert [r[:2] for r in con.execute(ROWS).fetchall()] == [
            ("intact.txt", 1), ("intact.txt", 2), ("intact_negative.txt", 1)]
    finally:
        con.close()


def test_a_local_zip_must_name_its_release(lake_settings, tmp_path):
    z = zipped(tmp_path / "in", "2026-01-09", [R1], [NEG])
    undated = z.rename(tmp_path / "intact.zip")
    with pytest.raises(ValueError, match="pass version"):
        intact.ingest(file=str(undated), settings=lake_settings)
    with pytest.raises(ValueError, match="not a release date"):
        intact.ingest(file=str(undated), version="current", settings=lake_settings)
    result = intact.ingest(file=str(undated), version="2026-01-09", settings=lake_settings)
    assert (result["version"], result["rows"]) == ("2026-01-09", 2)


def test_the_default_run_downloads_the_newest_release(lake_settings, tmp_path, monkeypatch):
    upstream = zipped(tmp_path / "up", "2026-10-17", [R1, R3], [NEG])
    fetched = []

    def download(url, dest):
        fetched.append((url, dest))
        dest.parent.mkdir(parents=True, exist_ok=True)
        return Path(shutil.copy(upstream, dest))

    monkeypatch.setattr(INGEST, "download", download)
    monkeypatch.setattr(INGEST, "latest", lambda s: "2026-10-17")
    result = intact.ingest(settings=lake_settings)
    assert (result["version"], result["rows"]) == ("2026-10-17", 3)
    [(url, dest)] = fetched
    assert url == "https://ftp.ebi.ac.uk/pub/databases/intact/2026-10-17/psimitab/intact.zip"
    # keyed by version under the source's raw dir, unzipped beside the download
    assert dest.parent.name == "2026-10-17" and dest.parent.parent.name == "intact"
    assert (dest.parent / "intact.txt").exists() and (dest.parent / "intact_negative.txt").exists()
