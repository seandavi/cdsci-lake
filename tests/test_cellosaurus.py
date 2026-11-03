"""Offline tests for the Cellosaurus source (bioc-on-ice#183).

The fixture is written at test time in the real flat file's dialect: a header
(banner, a blank line, ' Version:', the line-code table), then entries of
``XX   value`` lines opened by ID and closed by ``//``, LF line ends with a final
newline.
"""

from __future__ import annotations

import importlib
import shutil
import time
from pathlib import Path

import pytest

from cdsci.lake import Settings, lake_connect
from cdsci.lake.sources import cellosaurus

# the module itself: the package's `ingest` attribute is the function
INGEST = importlib.import_module("cdsci.lake.sources.cellosaurus.ingest")

SNAP = "SELECT max(snapshot_id) FROM lake.snapshots()"
COUNT = "SELECT count(*) FROM lake.cellosaurus.line"
# the file again, from the table
UNPARSE = """
    SELECT string_agg(CASE WHEN code IS NULL THEN value WHEN code = '//' THEN '//'
                           ELSE code || '   ' || value END, chr(10) ORDER BY line_number)
    FROM lake.cellosaurus.line"""


def header(version: str = "56.0") -> list[str]:
    return [
        "-" * 76,
        "        CALIPHO group at the SIB - Swiss Institute of Bioinformatics",
        "",
        f" Version: {version}",
        " Line code  Content                         Occurrence in an entry",
        " ID         Identifier (cell line name)     Once; starts an entry",
        "_" * 76,
    ]


HELA = [
    "ID   HeLa",
    "AC   CVCL_0030",
    "SY   HELA; Hela; He La; HeLa-CCL2",
    "DR   EFO; EFO_0001185",
    "DR   Wikidata; Q847482",
    "CC   Omics: Deep exome analysis.   Three spaces inside a value stay.",
    "DI   NCIt; C27677; Human papillomavirus-related endocervical adenocarcinoma",
    "OX   NCBI_TaxID=9606; ! Homo sapiens (Human)",
    "SX   Female",
    "AG   30Y6M",
    "CA   Cancer cell line",
    "//",
]
HYBRID = [
    "ID   #16-15",
    "AC   CVCL_KA96",
    "OX   NCBI_TaxID=10090; ! Mus musculus (Mouse)",
    "OX   NCBI_TaxID=10116; ! Rattus norvegicus (Rat)",
    "HI   CVCL_4032 ! P3X63Ag8.653",
    "CA   Hybridoma",
    "//",
]


def flat(path: Path, lines: list[str]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes("".join(line + "\n" for line in lines).encode())
    return path


@pytest.fixture
def lake_settings(tmp_path: Path) -> Settings:
    # lake_backend pinned: CU_OPENALEX_LAKE_BACKEND=postgres in the environment or a
    # .env would otherwise point these writes at the shared lake.
    return Settings(storage_base_uri=f"file://{tmp_path}", lake_backend="local",
                    lake_catalog=str(tmp_path / "catalog.ducklake"))


def test_cellosaurus_lands_every_line_verbatim_and_a_new_release_replaces_the_last(
        lake_settings, tmp_path):
    lines = [*header(), *HELA, *HYBRID]
    txt = flat(tmp_path / "cellosaurus.txt", lines)
    result = cellosaurus.ingest(file=str(txt), settings=lake_settings)
    assert (result["version"], result["rows"]) == ("56.0", len(lines))

    con = lake_connect(lake_settings)
    try:
        # raw's columns, names and types, without landed_in
        assert [(c[0], c[1]) for c in con.execute("DESCRIBE lake.cellosaurus.line").fetchall()] == [
            ("line_number", "BIGINT"), ("accession", "VARCHAR"), ("code", "VARCHAR"),
            ("value", "VARCHAR"), ("cellosaurus_version", "VARCHAR")]
        # the file comes back whole, the blank header line and inner spaces included
        assert con.execute(UNPARSE).fetchone()[0] + "\n" == txt.read_text()
        # header lines have no code and no accession; the blank one is ''
        assert con.execute("SELECT line_number, value FROM lake.cellosaurus.line "
                           "WHERE code IS NULL AND value = ''").fetchall() == [(3, "")]
        assert con.execute("SELECT count(*) FROM lake.cellosaurus.line WHERE code IS NULL "
                           "AND accession IS NULL").fetchone() == (len(header()),)
        # the ID line (before AC) and the terminator carry the entry's accession
        assert con.execute("SELECT code, accession, value FROM lake.cellosaurus.line "
                           "WHERE line_number IN (8, 19, 20) ORDER BY line_number").fetchall() == [
            ("ID", "CVCL_0030", "HeLa"), ("//", "CVCL_0030", None), ("ID", "CVCL_KA96", "#16-15")]
        assert con.execute("SELECT DISTINCT cellosaurus_version FROM lake.cellosaurus.line"
                           ).fetchall() == [("56.0",)]

        # re-landing the same release writes nothing
        before = con.execute(SNAP).fetchone()[0]
        cellosaurus.curate(con, txt, "56.0")
        assert con.execute(SNAP).fetchone()[0] == before

        # a limited load never prunes (and returns the table's rows, not those written);
        # a full load of the next release drops the lines past its end and restamps the rest
        nxt = flat(tmp_path / "next.txt", [*header("57.0"), *HYBRID])
        assert cellosaurus.curate(con, nxt, "57.0", limit=3) == len(lines)
        assert cellosaurus.curate(con, nxt, "57.0") == len(header()) + len(HYBRID)
        assert con.execute("SELECT DISTINCT cellosaurus_version FROM lake.cellosaurus.line"
                           ).fetchall() == [("57.0",)]
        assert con.execute(UNPARSE).fetchone()[0] + "\n" == nxt.read_text()
    finally:
        con.close()


@pytest.mark.parametrize(("body", "error"), [
    # beyond the limited load's lines: a line that is neither 'XX   value' nor '//'
    ([*HELA, "<html>not found</html>"], "1 body lines that are neither"),
    ([*HELA, "DR  EFO; EFO_1"], "1 body lines that are neither"),
    # an entry with no AC line
    ([*HELA, *[x for x in HYBRID if not x.startswith("AC")]], "in an entry with no AC line"),
    # a terminator missing between two entries
    ([*HELA[:-1], *HYBRID], "opens an entry before the last one is closed"),
    # a truncated download
    ([*HELA, *HYBRID[:-1]], "does not end with '//'"),
    # an entry after a terminator that does not open with ID (review, cdsci-lake#145)
    ([*HELA, "AC   CVCL_BAD", "DR   EFO; EFO_BAD", "//"], "starts an entry without an ID line"),
])
def test_a_malformed_file_fails_before_landing_and_keeps_the_table(
        lake_settings, tmp_path, body, error):
    con = lake_connect(lake_settings)
    try:
        cellosaurus.curate(con, flat(tmp_path / "seed.txt", [*header(), *HELA]), "56.0")
        before = con.execute(SNAP).fetchone()[0]
        bad = flat(tmp_path / "bad.txt", [*header("57.0"), *body])
        for limit in (None, 1):
            with pytest.raises(ValueError, match=error):
                cellosaurus.curate(con, bad, "57.0", limit=limit)
        assert con.execute(SNAP).fetchone()[0] == before
        assert con.execute(COUNT).fetchone() == (len(header()) + len(HELA),)
    finally:
        con.close()


@pytest.mark.parametrize(("lines", "error"), [
    ([], "yielded no lines"),
    (header(), "has no entries"),
    ([x for x in header() if "Version" not in x] + HELA, "no ' Version:' line"),
])
def test_an_empty_or_header_only_file_is_refused_and_the_table_kept(
        lake_settings, tmp_path, lines, error):
    con = lake_connect(lake_settings)
    try:
        cellosaurus.curate(con, flat(tmp_path / "seed.txt", [*header(), *HELA]), "56.0")
        before = con.execute(SNAP).fetchone()[0]
        empty = flat(tmp_path / "empty.txt", lines)
        for limit in (None, 1):
            with pytest.raises(ValueError, match=error):
                cellosaurus.curate(con, empty, "56.0", limit=limit)
        assert con.execute(SNAP).fetchone()[0] == before
        assert con.execute(COUNT).fetchone() == (len(header()) + len(HELA),)
    finally:
        con.close()


def test_a_version_other_than_the_files_is_refused(lake_settings, tmp_path):
    txt = flat(tmp_path / "cellosaurus.txt", [*header("57.0"), *HELA])
    with pytest.raises(ValueError, match="is release 57.0, not 56.0"):
        cellosaurus.ingest(file=str(txt), version="56.0", settings=lake_settings)
    result = cellosaurus.ingest(file=str(txt), version="57.0", settings=lake_settings)
    assert (result["version"], result["rows"]) == ("57.0", len(header()) + len(HELA))


def test_the_default_run_downloads_the_rolling_file_keyed_by_its_version(
        lake_settings, tmp_path, monkeypatch):
    upstream = flat(tmp_path / "upstream" / "cellosaurus.txt", [*header("57.0"), *HELA])
    fetched = []

    def download(url, dest):
        fetched.append(url)
        return Path(shutil.copy(upstream, dest))

    monkeypatch.setattr(INGEST, "download", download)
    result = cellosaurus.ingest(settings=lake_settings)
    assert (result["version"], result["rows"]) == ("57.0", len(header()) + len(HELA))
    assert fetched == [lake_settings.cellosaurus_url]
    raw = tmp_path / "lake" / "raw" / "cellosaurus"
    assert sorted(p.name for p in raw.iterdir()) == ["57.0-cellosaurus.txt"]


def test_the_parse_is_linear_in_the_file(lake_settings, tmp_path):
    # A 1 PRECEDING window frame made the parse quadratic: 148 s on the real
    # 3.2M-line file, ~28 min under lake_connect's 4 threads. 200k lines took
    # ~6 s that way; linear, they take well under one.
    entries = 200_000 // len(HELA)
    lines = [*header()]
    for i in range(entries):
        lines += [HELA[0], f"AC   CVCL_{i:06d}", *HELA[2:]]
    txt = flat(tmp_path / "big.txt", lines)
    # line_number -> the CVCL_ accession of the entry it falls in
    expect = (f"'CVCL_' || lpad(((line_number - {len(header())} - 1) // {len(HELA)})"
              "::VARCHAR, 6, '0')")
    con = lake_connect(lake_settings)
    try:
        start = time.perf_counter()
        INGEST._stage(con, txt, "56.0")
        elapsed = time.perf_counter() - start
        assert con.execute(f"""
            SELECT count(*), count(DISTINCT accession),
                   count(*) FILTER (accession <> {expect})
            FROM {INGEST._STAGE}""").fetchone() == (len(lines), entries, 0)
        assert elapsed < 3, f"parsing {len(lines):,} lines took {elapsed:.1f}s"
    finally:
        con.close()
