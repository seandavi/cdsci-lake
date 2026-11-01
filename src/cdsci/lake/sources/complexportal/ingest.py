"""Complex Portal complextab -> ``lake.complexportal.complex`` (EL only; bioc-on-ice#183 step 7).

Complex Portal (EMBL-EBI, CC0) describes stable macromolecular complexes: a
named assembly with N participants and their stoichiometry, a GO annotation,
and the evidence for it. Ported from bioc-on-ice's
``src/bioconice/complexportal.py`` (its ``raw.complexportal__complex``
landing); bioc-on-ice derives ``annotation.complexportal__complex`` and
``__participant`` from this table at release time (bioc-on-ice ADR-0015).

It is released with IntAct, frozen under ``pub/databases/intact/complex/<date>/``;
the date is the version. ``complextab/`` holds one TSV per species plus
``9606_predicted.tsv`` (hu.MAP machine-learning predictions), all with one
19-column header: 29 files and 20,579 complexes in 2026-01-09. Every file lands,
and the file name is kept (``file``) because it is the only thing that says
'predicted'. The complex AC (``CPX-663``) is the key: unique across all files,
curated and predicted alike.

The dialect is stated, not sniffed. Cells use ``"`` as content
(``psi-mi:"MI:0469"(IntAct)``), so quoting is off; ``-`` is the missing marker
and reads as NULL. Columns are read by header name and the header is checked
whole, so a renamed, dropped or added column fails before anything lands.
"""

from __future__ import annotations

import re
from pathlib import Path

import duckdb
import httpx

from ... import ops
from ...config import Settings, get_settings
from ...connect import LAKE, csv_source, lake_connect, raw_dir, upsert
from ...download import download

_RAW = "complexportal"

# The files' header -> column name, in file order.
COLUMNS = {
    "#Complex ac": "complex_ac",
    "Recommended name": "recommended_name",
    "Aliases for complex": "aliases",
    "Taxonomy identifier": "taxonomy_identifier",
    "Identifiers (and stoichiometry) of molecules in complex": "participants",
    "Evidence Code": "evidence_code",
    "Experimental evidence": "experimental_evidence",
    "Go Annotations": "go_annotations",
    "Cross references": "cross_references",
    "Description": "description",
    "Complex properties": "complex_properties",
    "Complex assembly": "complex_assembly",
    "Ligand": "ligand",
    "Disease": "disease",
    "Agonist": "agonist",
    "Antagonist": "antagonist",
    "Comment": "comment",
    "Source": "source",
    "Expanded participant list": "expanded_participants",
}


def _index(url: str) -> list[str]:
    """The hrefs of an FTP-over-HTTPS directory listing."""
    resp = httpx.get(url, timeout=60, follow_redirects=True)
    resp.raise_for_status()
    return re.findall(r'href="([^"?/][^"]*)"', resp.text)


def latest(settings: Settings | None = None) -> str:
    """The newest dated release directory, e.g. ``2026-10-17``."""
    s = settings or get_settings()
    found = [h.rstrip("/") for h in _index(s.complexportal_base_url)
             if re.fullmatch(r"\d{4}-\d{2}-\d{2}/", h)]
    if not found:
        raise RuntimeError(f"complexportal: no dated release at {s.complexportal_base_url}")
    return max(found)


def download_release(version: str, settings: Settings | None = None) -> list[Path]:
    """Download every ``complextab/*.tsv`` of one release into the raw layer."""
    s = settings or get_settings()
    url = f"{s.complexportal_base_url}{version}/complextab/"
    dest = raw_dir(_RAW, s) / version
    files = [download(url + h, dest / h) for h in _index(url) if h.endswith(".tsv")]
    if not files:
        raise RuntimeError(f"complexportal: no .tsv files at {url}")
    return files


def _select_sql(read: str, version: str, limit: int | None) -> str:
    select = ", ".join(f'"{h}" AS {c}' for h, c in COLUMNS.items())
    limit_sql = f" LIMIT {int(limit)}" if limit else ""
    return f"""
        SELECT {select}, parse_filename(filename) AS file,
               '{version}' AS complexportal_version
        FROM {read}
        {limit_sql}
    """


def curate(
    con: duckdb.DuckDBPyConnection,
    paths: list[Path],
    version: str,
    *,
    target: str | None = None,
    limit: int | None = None,
) -> int:
    """MERGE-upsert one release's files into ``complex`` on ``complex_ac``.

    A full load prunes complexes dropped from the release; ``limit`` never prunes.
    ``complexportal_version`` is compared like any column, so a new release
    restamps every row: the table is one release, never a mix.
    """
    read = (f"read_csv({csv_source(sorted(paths))}, delim='\\t', header=true, "
            "all_varchar=true, quote='', escape='', nullstr='-', filename=true)")
    header = [r[0] for r in con.sql(f"DESCRIBE SELECT * EXCLUDE (filename) FROM {read}").fetchall()]
    if header != list(COLUMNS):
        raise ValueError(f"complexportal: header is not the declared one; "
                         f"differs in {sorted(set(header) ^ set(COLUMNS))}")
    target = target or f"{LAKE}.complexportal.complex"
    return upsert(
        con, target, _select_sql(read, version, limit),
        key="complex_ac", prune=None if limit else "true",
    )


def ingest(
    *,
    directory: str | None = None,
    version: str | None = None,
    schema: str = "complexportal",
    limit: int | None = None,
    settings: Settings | None = None,
) -> dict:
    """End-to-end: download a release (unless ``directory``) -> MERGE-upsert -> summary.

    ``directory`` is a local ``<YYYY-MM-DD>/complextab/`` copy; its dated parent
    names the version unless ``version`` is given. Otherwise ``version`` (default:
    the newest release, :func:`latest`) is downloaded.
    """
    s = settings or get_settings()
    if directory:
        d = Path(directory)
        dated = re.fullmatch(r"\d{4}-\d{2}-\d{2}", d.parent.name)
        version = version or (dated and d.parent.name)
        if not version:
            raise ValueError(f"complexportal: {d} is not <YYYY-MM-DD>/complextab/; pass version")
        paths = sorted(d.glob("*.tsv"))
    else:
        version = version or latest(s)
        paths = download_release(version, s)
    target = f"{LAKE}.{schema}.complex"
    con = lake_connect(s)
    try:
        with ops.run(con, source="complexportal", target=target, version=version) as r:
            r.rows = curate(con, paths, version, target=target, limit=limit)
    finally:
        con.close()
    return r.summary()
