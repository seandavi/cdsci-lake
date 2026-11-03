"""IntAct PSI-MI MITAB 2.7 -> ``lake.intact.interaction`` (EL only; bioc-on-ice#183).

IntAct (EMBL-EBI, with MINT, DIP, UniProt and the other IMEx curators; CC BY
4.0) curates molecular interaction evidence for every organism and molecule
type. ``psimitab/intact.zip`` holds two MITAB files with one header:
``intact.txt`` (1,787,425 rows, 11 GB, in 2026-01-09) and ``intact_negative.txt``
(983 rows, interactions shown NOT to occur). Both land, since the zip is the
file; MITAB's own ``negative`` column tells them apart. Ported from bioc-on-ice's
``src/bioconice/intact.py`` (its ``raw.intact__mitab`` landing); bioc-on-ice
derives its ``annotation.interaction`` rows from this table at release time
(bioc-on-ice ADR-0015).

**Versioned by release directory.** Each release is frozen under
``pub/databases/intact/<YYYY-MM-DD>/``; the date is ``intact_version``, so
``version`` fetches that release and with none the newest listed is fetched.
``current/`` names no release and is never read.

**A row has no identifier.** The interaction AC is shared by every binary row
spoke-expanded from one n-ary interaction, and the file holds whole-row
duplicates (1,052 groups, 2,144 rows in 2026-01-09), kept as published. So no
set of its columns can be a key: the table is keyed by ``(file_name,
line_number)``, the member file and its 1-based data line, and
``SELECT * EXCLUDE (file_name, line_number)`` reproduces ``raw.intact__mitab``
without ``landed_in``.

**The read is positional, so every file is checked whole first**: the header
must be exactly :data:`COLUMNS`, and every line must have its 42 fields, so a
malformed line (a blank one included) fails anywhere in the file, not just
within a limited load's rows. A file with no rows is refused.

The dialect is the lander's, verbatim: tab-delimited, quoting off (MITAB uses
``"`` inside cells, ``psi-mi:"MI:0018"(two hybrid)``, without it being a quote),
``-`` is MITAB's missing marker and reads as NULL, and ``max_line_size`` is
raised because an alias or xref cell can run long. Every column lands VARCHAR
under raw's names.
"""

from __future__ import annotations

import re
from pathlib import Path

import duckdb
import httpx

from ... import ops
from ...config import Settings, get_settings
from ...connect import LAKE, csv_source, lake_connect, raw_dir, upsert
from ...download import download, unzip

_RAW = "intact"
DATE = r"\d{4}-\d{2}-\d{2}"

# MITAB 2.7 header -> column name, in file order.
COLUMNS = {
    "#ID(s) interactor A": "id_a",
    "ID(s) interactor B": "id_b",
    "Alt. ID(s) interactor A": "alt_ids_a",
    "Alt. ID(s) interactor B": "alt_ids_b",
    "Alias(es) interactor A": "aliases_a",
    "Alias(es) interactor B": "aliases_b",
    "Interaction detection method(s)": "detection_methods",
    "Publication 1st author(s)": "first_authors",
    "Publication Identifier(s)": "publication_ids",
    "Taxid interactor A": "taxid_a",
    "Taxid interactor B": "taxid_b",
    "Interaction type(s)": "interaction_types",
    "Source database(s)": "source_databases",
    "Interaction identifier(s)": "interaction_ids",
    "Confidence value(s)": "confidence_values",
    "Expansion method(s)": "expansion_methods",
    "Biological role(s) interactor A": "biological_roles_a",
    "Biological role(s) interactor B": "biological_roles_b",
    "Experimental role(s) interactor A": "experimental_roles_a",
    "Experimental role(s) interactor B": "experimental_roles_b",
    "Type(s) interactor A": "interactor_types_a",
    "Type(s) interactor B": "interactor_types_b",
    "Xref(s) interactor A": "xrefs_a",
    "Xref(s) interactor B": "xrefs_b",
    "Interaction Xref(s)": "interaction_xrefs",
    "Annotation(s) interactor A": "annotations_a",
    "Annotation(s) interactor B": "annotations_b",
    "Interaction annotation(s)": "interaction_annotations",
    "Host organism(s)": "host_organisms",
    "Interaction parameter(s)": "interaction_parameters",
    "Creation date": "creation_date",
    "Update date": "update_date",
    "Checksum(s) interactor A": "checksums_a",
    "Checksum(s) interactor B": "checksums_b",
    "Interaction Checksum(s)": "interaction_checksums",
    "Negative": "negative",
    "Feature(s) interactor A": "features_a",
    "Feature(s) interactor B": "features_b",
    "Stoichiometry(s) interactor A": "stoichiometry_a",
    "Stoichiometry(s) interactor B": "stoichiometry_b",
    "Identification method participant A": "identification_methods_a",
    "Identification method participant B": "identification_methods_b",
}


def latest(settings: Settings | None = None) -> str:
    """The newest dated release directory on the FTP, e.g. ``2026-10-17``."""
    s = settings or get_settings()
    resp = httpx.get(s.intact_base_url, timeout=60, follow_redirects=True)
    resp.raise_for_status()
    found = re.findall(rf'href="({DATE})/"', resp.text)
    if not found:
        raise RuntimeError(f"intact: no dated release directory listed at {s.intact_base_url}")
    return max(found)


def download_release(version: str, settings: Settings | None = None) -> Path:
    """Download one release's ``psimitab/intact.zip`` into the raw layer, keyed by ``version``."""
    s = settings or get_settings()
    url = f"{s.intact_base_url}{version}/psimitab/intact.zip"
    return download(url, raw_dir(_RAW, s) / version / "intact.zip")


def _check(path: Path) -> None:
    """The header whole, then every line's field count; a file with no rows is refused.

    No cell holds a tab or a line break, so a line is a record and a field count
    other than the header's is a malformed file, wherever it is.
    """
    with open(path, "rb") as f:
        header = f.readline().decode("utf-8", "replace").rstrip("\r\n").split("\t")
        if header != list(COLUMNS):
            raise ValueError(f"intact: {path} header is not the declared one; differs "
                             f"in {sorted(set(header) ^ set(COLUMNS)) or 'column order'}")
        n = 0
        for n, line in enumerate(f, 1):
            if (fields := line.count(b"\t") + 1) != len(COLUMNS):
                raise ValueError(f"intact: {path} data line {n} has {fields} fields, "
                                 f"not {len(COLUMNS)}")
    if not n:
        raise ValueError(f"intact: {path} has zero rows")


def _read(path: Path) -> str:
    columns = "{" + ", ".join(f"'{c}': 'VARCHAR'" for c in COLUMNS.values()) + "}"
    name = path.name.replace("'", "''")
    # The CSV scan is order-preserving and row_number() OVER () streams in that
    # order, so the n-th row is the n-th data line (checked to be one record each);
    # verified under 4 and 16 threads on DuckDB 1.5.4 (tests/test_intact.py).
    return (f"SELECT '{name}' AS file_name, row_number() OVER ()::INTEGER AS line_number, * "
            f"FROM read_csv({csv_source([path])}, delim='\\t', header=true, auto_detect=false, "
            f"columns={columns}, quote='', escape='', nullstr='-', max_line_size=268435456)")


def curate(
    con: duckdb.DuckDBPyConnection,
    paths: list[Path],
    version: str,
    *,
    target: str | None = None,
    limit: int | None = None,
) -> int:
    """MERGE-upsert one release's MITAB files into ``interaction``; return the row count.

    Keyed ``(file_name, line_number)``. Every file is checked whole before any
    lands. A full load prunes lines the release no longer has; ``limit`` never
    prunes. ``intact_version`` is compared like any column, so a new release
    restamps every row: the table is one release, never a mix.
    """
    # limit=0 is falsy: it would read every row and prune as a full load
    if limit is not None and limit < 1:
        raise ValueError(f"intact: limit must be positive, got {limit}")
    if not paths:
        raise ValueError("intact: no MITAB files to land")
    for path in paths:
        _check(path)
    union = " UNION ALL ".join(f"({_read(p)})" for p in sorted(paths))
    limit_sql = f" LIMIT {int(limit)}" if limit else ""
    target = target or f"{LAKE}.intact.interaction"
    return upsert(
        con, target, f"SELECT *, '{version}' AS intact_version FROM ({union}){limit_sql}",
        key=["file_name", "line_number"], prune=None if limit else "true",
    )


def ingest(
    *,
    file: str | None = None,
    version: str | None = None,
    schema: str = "intact",
    limit: int | None = None,
    settings: Settings | None = None,
) -> dict:
    """End-to-end: download a release (unless ``file``) -> unzip -> MERGE-upsert -> summary.

    ``file`` is a local ``intact.zip``; a ``<YYYY-MM-DD>/psimitab/`` path names
    the version unless ``version`` is given. Otherwise ``version`` (default:
    :func:`latest`) is downloaded.
    """
    s = settings or get_settings()
    archive = Path(file) if file else None
    if archive:
        dated = re.search(rf"/({DATE})/psimitab/", str(archive.resolve()))
        version = version or (dated and dated.group(1))
        if not version:
            raise ValueError(f"intact: {archive} is not under <YYYY-MM-DD>/psimitab/; "
                             "pass version")
    else:
        version = version or latest(s)
    # the version is interpolated into a URL, a path and the SQL
    if not re.fullmatch(DATE, version):
        raise ValueError(f"intact: version {version!r} is not a release date YYYY-MM-DD")
    archive = archive or download_release(version, s)
    paths = unzip(archive, raw_dir(_RAW, s) / version)
    target = f"{LAKE}.{schema}.interaction"
    con = lake_connect(s)
    try:
        with ops.run(con, source="intact", target=target, version=version) as r:
            r.rows = curate(con, paths, version, target=target, limit=limit)
    finally:
        con.close()
    return r.summary()
