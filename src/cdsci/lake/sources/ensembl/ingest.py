"""Land an Ensembl per-species GTF verbatim into ``lake.ensembl.gtf`` (issue #35).

Ported from bioc-on-ice's ``src/bioconice/ensembl.py``, which is being retired as
that repo narrows to a publication layer. Only the **landing** half comes across
here: interpreting the GTF (attribute parsing, the CDS→exon join) is SQL under
``models/ensembl/`` now (ADR-0015), not Python, so re-reading a GTF differently
is a ``transform run``, not a re-download.

**Immutable per release.** An Ensembl release never changes, so a GTF is landed
whole per ``(ncbitaxon_id, ensembl_release)`` and a re-ingest of an already-landed
partition is a **no-op** (not a re-download, not a re-write, no snapshot) unless
``replace=True``. That's the icite-style snapshot shape the issue asks for rather
than a MERGE-on-change source: there is no per-row natural key in a GTF worth
merging on, and every row of a given release is either present or absent together.

**License.** Ensembl's own legal page (``ensembl.org/info/about/legal/
disclaimer.html``, read 2026-08-11 via the ``jun2026-plants.ensembl.org`` mirror
the ``www`` host redirects to) states verbatim: *"Ensembl imposes no restrictions
on access to, or use of, the data provided and the software used to analyse and
present it."* — with the caveat that *"Some of the data and software included in
the distribution may be subject to third-party constraints"* and code (not data)
under Apache 2.0. Ensembl's 2026 NAR paper additionally frames the data as
available under the EMBL-EBI Terms of Use, which itself *"places no additional
restrictions on the use or redistribution of the data ... other than those
provided by the original data owners"* (ebi.ac.uk/about/terms-of-use, same date).
So: **not** a CC0 grant, despite being CC0-*equivalent* in practice — there is no
license instrument named anywhere, just a no-restrictions statement plus a
third-party-constraints disclaimer. Registered as ``ensembl-no-restrictions``
rather than ``cc0`` so a downstream consumer reading the registry doesn't
mistake it for an actual public-domain dedication.

**GTF filename gotcha (deviation from the bioc-on-ice port).** bioc-on-ice found
the GTF by matching the release number in the filename
(``.../\\.{release}\\.gtf\\.gz``). Ensembl's own gtf README documents the pattern
as ``<species>.<assembly>.<version>.gtf.gz`` with version = "the version of
Ensembl from which the data was exported", but that is not what the FTP site
actually serves: in release 116, yeast is
``Saccharomyces_cerevisiae.R64-1-1.63.gtf.gz`` — the *last annotation update*,
not the release. The release-number match would raise "no GTF" for every species
whose annotation predates the current release. :func:`gtf_url` instead takes the
one ``*.gtf.gz`` that is not one of the documented variant builds
(``.abinitio.``/``.chr.``/``.chr_patch_hapl_scaff.``), which is release- and
species-agnostic.
"""

from __future__ import annotations

import re
from contextlib import nullcontext
from pathlib import Path

import duckdb
import httpx

from ... import ops
from ...config import Settings, get_settings
from ...connect import LAKE, csv_source, lake_connect, raw_dir
from ...download import download
from ...log import logger

_TABLE = "ensembl.gtf"  # lake table (schema.table)
_RAW = "ensembl"  # raw-download subdir name
_PUB = "https://ftp.ensembl.org/pub"
_TIMEOUT = 60.0

# GTF is a headerless 9-column TSV; column order and meaning per Ensembl's own
# gtf/<species>/README. Everything lands as-is — the attribute blob is parsed in
# `models/ensembl/feature.sql`, not here.
_GTF_COLUMNS = {
    "seqname": "VARCHAR", "source": "VARCHAR", "feature": "VARCHAR",
    "start": "BIGINT", "end": "BIGINT", "score": "VARCHAR",
    "strand": "VARCHAR", "frame": "VARCHAR", "attribute": "VARCHAR",
}

# The documented non-primary GTF builds: ab-initio predictions, the
# chromosomes-only subset, and the patch/haplotype/scaffold superset.
_VARIANT_BUILDS = (".abinitio.", ".chr.", ".chr_patch_hapl_scaff.")

_log = logger.bind(ctx="ensembl")


def _sql_str(value: str) -> str:
    """Quote a value as a SQL string literal."""
    return "'" + value.replace("'", "''") + "'"


def _get_text(url: str) -> str:
    with httpx.Client(timeout=_TIMEOUT, follow_redirects=True) as client:
        resp = client.get(url)
        resp.raise_for_status()
        return resp.text


def current_release() -> int:
    """The release number Ensembl's FTP site currently calls latest."""
    return int(_get_text(f"{_PUB}/VERSION").strip())


def species_info(ensembl_release: int, species: str) -> dict:
    """``{species, ensembl_release, ncbitaxon_id, assembly, genome_accession}``.

    Read from Ensembl's own per-release ``species_EnsemblVertebrates.txt`` rather
    than hard-coded, so the taxon/assembly/accession stamped onto raw rows is
    whatever that release itself says (a species can change assembly between
    releases).
    """
    url = f"{_PUB}/release-{ensembl_release}/species_EnsemblVertebrates.txt"
    for line in _get_text(url).splitlines():
        f = line.split("\t")
        # #name, species, division, taxonomy_id, assembly, assembly_accession, ...
        if len(f) > 5 and f[1] == species:
            return {
                "species": species,
                "ensembl_release": str(ensembl_release),
                "ncbitaxon_id": int(f[3]),
                "assembly": f[4],
                "genome_accession": f[5],
            }
    raise ValueError(f"{species!r} is not in Ensembl release {ensembl_release} vertebrates")


def gtf_url(ensembl_release: int, species: str) -> str:
    """URL of the primary GTF for a species — see the module docstring's gotcha."""
    listing = f"{_PUB}/release-{ensembl_release}/gtf/{species}/"
    names = sorted(
        {
            n for n in re.findall(r'href="([^"/]+\.gtf\.gz)"', _get_text(listing))
            if not any(v in n for v in _VARIANT_BUILDS)
        }
    )
    if len(names) != 1:
        raise ValueError(
            f"expected exactly one primary GTF for {species} in release "
            f"{ensembl_release}, found {names or 'none'}"
        )
    return listing + names[0]


def download_gtf(ensembl_release: int, species: str, settings: Settings | None = None) -> Path:
    """Download the primary GTF into the raw layer (bronze; not re-downloaded)."""
    url = gtf_url(ensembl_release, species)
    return download(url, raw_dir(_RAW, settings) / url.rsplit("/", 1)[-1])


def _select_sql(path: Path | str, info: dict, limit: int | None) -> str:
    """The GTF verbatim, plus the release/species stamps that key a partition."""
    columns_sql = ", ".join(f"'{k}': '{v}'" for k, v in _GTF_COLUMNS.items())
    limit_sql = f" LIMIT {int(limit)}" if limit else ""
    return f"""
        SELECT
            seqname, source, feature, "start", "end", score, strand, frame, attribute,
            CAST({info["ncbitaxon_id"]} AS INTEGER)                    AS ncbitaxon_id,
            CAST({_sql_str(info["ensembl_release"])} AS VARCHAR)       AS ensembl_release,
            CAST({_sql_str(info["species"])} AS VARCHAR)               AS species,
            CAST({_sql_str(info["assembly"])} AS VARCHAR)              AS assembly,
            CAST({_sql_str(info["genome_accession"])} AS VARCHAR)      AS genome_accession
        FROM read_csv(
            {csv_source([Path(path)])}, sep = '\\t', header = false, comment = '#',
            auto_detect = false, columns = {{{columns_sql}}}
        ){limit_sql}
    """


def curate(
    con: duckdb.DuckDBPyConnection,
    path: Path | str,
    info: dict,
    *,
    target: str | None = None,
    replace: bool = False,
    limit: int | None = None,
) -> int:
    """Land ``path`` as the ``(ncbitaxon_id, ensembl_release)`` partition of ``target``.

    Already-landed partition + ``replace=False`` → returns immediately, writing
    nothing (an Ensembl release is immutable; see the module docstring). Otherwise
    the partition is deleted and re-inserted wholesale inside one attributed
    snapshot. Returns the target's full row count either way.
    """
    target = target or f"{LAKE}.{_TABLE}"
    catalog, schema, _ = target.split(".")
    select = _select_sql(path, info, limit)
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema};")
    con.execute(f"CREATE TABLE IF NOT EXISTS {target} AS SELECT * FROM ({select}) WHERE false;")

    where = "ncbitaxon_id = ? AND ensembl_release = ?"
    params = [info["ncbitaxon_id"], info["ensembl_release"]]
    landed = con.execute(f"SELECT count(*) FROM {target} WHERE {where}", params).fetchone()[0]
    if landed and not replace:
        _log.info(
            "release {} / taxon {} already landed ({:,} rows) — skipping (pass replace=True "
            "to force)", info["ensembl_release"], info["ncbitaxon_id"], landed,
        )
        return con.execute(f"SELECT count(*) FROM {target}").fetchone()[0]

    run = ops.active_run()
    with (run.attribute("gtf") if run is not None else nullcontext()):
        con.execute(f"DELETE FROM {target} WHERE {where}", params)
        con.execute(f"INSERT INTO {target} {select}")
    return con.execute(f"SELECT count(*) FROM {target}").fetchone()[0]


def ingest(
    *,
    species: str,
    ensembl_release: int | None = None,
    file: str | None = None,
    schema: str = "ensembl",
    replace: bool = False,
    limit: int | None = None,
    settings: Settings | None = None,
) -> dict:
    """End-to-end: resolve the species/release → download the GTF → land it → summary.

    ``ensembl_release`` defaults to whatever Ensembl's FTP ``VERSION`` currently
    says. ``file`` overrides the download with a local GTF (the fixture/mirror
    case, same idiom as ``icite``/``uniprot``) — the species metadata is still
    resolved from Ensembl, since the taxon/assembly stamps aren't in the file.
    """
    s = settings or get_settings()
    release = ensembl_release or current_release()
    info = species_info(release, species)
    target = f"{LAKE}.{schema}.gtf"

    con = lake_connect(s)
    try:
        with ops.run(
            con, source="ensembl", target=target, version=f"{release}:{species}"
        ) as r:
            path = Path(file) if file else download_gtf(release, species, s)
            r.rows = curate(con, path, info, target=target, replace=replace, limit=limit)
    finally:
        con.close()
    return r.summary()
