"""Ingest NLM MeSH (descriptors + tree hierarchy + qualifiers) into ``mesh.*``.

Medallion flow (ADR-0012, mirrors ctgov), driven from the annual descriptor +
qualifier **XML** (no maintained Python MeSH library exists, and the SPARQL
endpoint caps at 1000 rows / the RDF dump is ~2 GB — see ADR-0010):

1. **download** ``desc{year}.gz`` + ``qual{year}.xml`` to the raw layer.
2. **stream → NDJSON** — ``xml.etree.iterparse`` over each record (stdlib, no new
   dependency), one compact structured JSON object per descriptor / qualifier.
3. **materialize** a bronze Parquet from the NDJSON (read_json infers the nested
   ``tree_numbers`` / ``qualifiers`` / ``entry_terms`` shapes).
4. **curate** five silver tables, all MERGE-upsert (idempotent, attributed):
   * ``mesh.descriptor`` — one row per ``descriptor_ui`` (name + scope note).
   * ``mesh.tree`` — the **hierarchy**, exploded ``(descriptor_ui, tree_number)``
     with a derived ``parent_tree_number`` (polyhierarchy is native — a descriptor
     has 0..N tree numbers).
   * ``mesh.qualifier`` — the ~80 subheadings.
   * ``mesh.descriptor_qualifier`` — allowable-qualifier bridge.
   * ``mesh.entry_term`` — synonyms (``is_preferred`` flags the descriptor name).

The literature edge (``mesh.article_heading`` from ``omicidx.pubmed_article``) is
Phase 1b — not built here. Supplementary Concept Records (``supp``) are deferred.
"""

from __future__ import annotations

import gzip
import json
import xml.etree.ElementTree as ET
from datetime import date
from pathlib import Path

import duckdb

from ... import ops
from ...config import Settings, get_settings
from ...connect import LAKE, csv_source, lake_connect, raw_dir, upsert
from ...download import download
from ...log import logger

_RAW = "mesh"
_log = logger.bind(ctx="mesh")


# --- download ---


def download_descriptors(year: int, settings: Settings | None = None) -> Path:
    """Download the gzipped descriptor XML (``desc{year}.gz``) to the raw layer."""
    s = settings or get_settings()
    return download(f"{s.mesh_xml_base}desc{year}.gz", raw_dir(_RAW, s) / f"desc{year}.xml.gz")


def download_qualifiers(year: int, settings: Settings | None = None) -> Path:
    """Download the qualifier XML (``qual{year}.xml``, ~0.3 MB) to the raw layer."""
    s = settings or get_settings()
    return download(f"{s.mesh_xml_base}qual{year}.xml", raw_dir(_RAW, s) / f"qual{year}.xml")


# --- stream XML → NDJSON (stdlib iterparse; clears each record to bound memory) ---


def _open_xml(path: Path):
    return gzip.open(path, "rb") if str(path).endswith(".gz") else open(path, "rb")


def _text(elem: ET.Element, path: str) -> str | None:
    found = elem.findtext(path)
    return found.strip() if found else None


def _descriptor_record(elem: ET.Element) -> dict | None:
    """Pull (ui, name, scope_note, tree_numbers, qualifiers, entry_terms) from a record."""
    ui = _text(elem, "DescriptorUI")
    name = _text(elem, "DescriptorName/String")
    if not ui or not name:
        return None
    trees = [t.text.strip() for t in elem.findall("TreeNumberList/TreeNumber") if t.text]
    quals = [
        q.findtext("QualifierReferredTo/QualifierUI")
        for q in elem.findall("AllowableQualifiersList/AllowableQualifier")
    ]
    scope = next(
        (_text(c, "ScopeNote") for c in elem.findall("ConceptList/Concept")
         if c.get("PreferredConceptYN") == "Y"),
        None,
    )
    return {
        "descriptor_ui": ui,
        "name": name,
        "scope_note": scope,
        "tree_numbers": trees,
        "qualifiers": [q for q in quals if q],
        "entry_terms": _entry_terms(elem),
    }


def _entry_terms(elem: ET.Element) -> list[dict]:
    """All Concept/Term strings as ``{term, is_preferred}`` (preferred = the record name)."""
    out: list[dict] = []
    for concept in elem.findall("ConceptList/Concept"):
        pref_concept = concept.get("PreferredConceptYN") == "Y"
        for term in concept.findall("TermList/Term"):
            s = _text(term, "String")
            if s:
                out.append({
                    "term": s,
                    "is_preferred": pref_concept and term.get("ConceptPreferredTermYN") == "Y",
                })
    return out


def _qualifier_record(elem: ET.Element) -> dict | None:
    ui = _text(elem, "QualifierUI")
    name = _text(elem, "QualifierName/String")
    if not ui or not name:
        return None
    return {"qualifier_ui": ui, "name": name, "abbreviation": _text(elem, "Abbreviation")}


def _to_ndjson(xml_path: Path, ndjson_path: Path, record_tag: str, extract) -> int:
    """Stream ``record_tag`` elements from the XML → gzipped NDJSON via ``extract``."""
    n = 0
    with _open_xml(xml_path) as fh, gzip.open(ndjson_path, "wt") as out:
        for _, elem in ET.iterparse(fh, events=("end",)):
            if elem.tag != record_tag:
                continue
            rec = extract(elem)
            if rec:
                out.write(json.dumps(rec, separators=(",", ":")) + "\n")
                n += 1
            elem.clear()
    return n


def descriptors_to_ndjson(xml_path: Path | str, ndjson_path: Path | str) -> int:
    """Stream the descriptor XML → NDJSON (one structured object per descriptor)."""
    n = _to_ndjson(Path(xml_path), Path(ndjson_path), "DescriptorRecord", _descriptor_record)
    _log.info("parsed {:,} descriptors → {}", n, Path(ndjson_path).name)
    return n


def qualifiers_to_ndjson(xml_path: Path | str, ndjson_path: Path | str) -> int:
    """Stream the qualifier XML → NDJSON (one object per qualifier)."""
    n = _to_ndjson(Path(xml_path), Path(ndjson_path), "QualifierRecord", _qualifier_record)
    _log.info("parsed {:,} qualifiers → {}", n, Path(ndjson_path).name)
    return n


def materialize_raw(con: duckdb.DuckDBPyConnection, ndjson: Path | str) -> Path:
    """Bronze: a Parquet from the structured NDJSON (nested lists/structs preserved)."""
    ndjson = Path(ndjson)
    raw = ndjson.with_name(ndjson.name.split(".")[0] + ".parquet")
    con.execute(f"COPY (SELECT * FROM read_json_auto('{ndjson}')) TO '{raw}' (FORMAT parquet);")
    return raw


# --- curate (silver) ---


def _descriptor_sql(src: str, version: str, limit: int | None) -> str:
    limit_sql = f" LIMIT {int(limit)}" if limit else ""
    return f"""
        SELECT descriptor_ui, name, scope_note,
               CAST('{version}' AS VARCHAR) AS snapshot_version
        FROM read_parquet({src}){limit_sql}
    """


def _tree_sql(src: str, version: str, limit: int | None) -> str:
    """Explode tree numbers; ``parent_tree_number`` drops the last dotted segment."""
    limit_sql = f" LIMIT {int(limit)}" if limit else ""
    return f"""
        WITH d AS (SELECT descriptor_ui, tree_numbers FROM read_parquet({src}){limit_sql}),
             ex AS (SELECT descriptor_ui, unnest(tree_numbers) AS tree_number FROM d)
        SELECT DISTINCT descriptor_ui, tree_number,
               CASE WHEN tree_number LIKE '%.%'
                    THEN regexp_replace(tree_number, '\\.[^.]+$', '')
                    ELSE NULL END AS parent_tree_number,
               CAST('{version}' AS VARCHAR) AS snapshot_version
        FROM ex
    """


def _qualifier_sql(src: str, version: str) -> str:
    return f"""
        SELECT qualifier_ui, name, abbreviation,
               CAST('{version}' AS VARCHAR) AS snapshot_version
        FROM read_parquet({src})
    """


def _descriptor_qualifier_sql(src: str, version: str, limit: int | None) -> str:
    limit_sql = f" LIMIT {int(limit)}" if limit else ""
    return f"""
        WITH d AS (SELECT descriptor_ui, qualifiers FROM read_parquet({src}){limit_sql}),
             ex AS (SELECT descriptor_ui, unnest(qualifiers) AS qualifier_ui FROM d)
        SELECT DISTINCT descriptor_ui, qualifier_ui,
               CAST('{version}' AS VARCHAR) AS snapshot_version
        FROM ex WHERE qualifier_ui IS NOT NULL
    """


def _entry_term_sql(src: str, version: str, limit: int | None) -> str:
    """Explode entry terms; collapse duplicate terms, OR-ing the preferred flag."""
    limit_sql = f" LIMIT {int(limit)}" if limit else ""
    return f"""
        WITH d AS (SELECT descriptor_ui, entry_terms FROM read_parquet({src}){limit_sql}),
             ex AS (SELECT descriptor_ui, unnest(entry_terms) AS et FROM d)
        SELECT descriptor_ui, et.term AS term,
               bool_or(et.is_preferred) AS is_preferred,
               CAST('{version}' AS VARCHAR) AS snapshot_version
        FROM ex WHERE et.term IS NOT NULL
        GROUP BY descriptor_ui, et.term
    """


def curate(
    con: duckdb.DuckDBPyConnection,
    descriptor_parquet: Path | str,
    qualifier_parquet: Path | str,
    *,
    schema: str = "mesh",
    version: str = "0",
    limit: int | None = None,
) -> dict[str, int]:
    """Upsert the five MeSH tables from the bronze descriptor + qualifier Parquets."""
    d = csv_source(str(descriptor_parquet))
    q = csv_source(str(qualifier_parquet))
    ex = ["snapshot_version"]
    counts = {
        "descriptor": upsert(
            con, f"{LAKE}.{schema}.descriptor", _descriptor_sql(d, version, limit),
            "descriptor_ui", exclude_change_cols=ex,
        ),
        "tree": upsert(
            con, f"{LAKE}.{schema}.tree", _tree_sql(d, version, limit),
            ["descriptor_ui", "tree_number"], exclude_change_cols=ex,
        ),
        "qualifier": upsert(
            con, f"{LAKE}.{schema}.qualifier", _qualifier_sql(q, version),
            "qualifier_ui", exclude_change_cols=ex,
        ),
        "descriptor_qualifier": upsert(
            con, f"{LAKE}.{schema}.descriptor_qualifier",
            _descriptor_qualifier_sql(d, version, limit),
            ["descriptor_ui", "qualifier_ui"], exclude_change_cols=ex,
        ),
        "entry_term": upsert(
            con, f"{LAKE}.{schema}.entry_term", _entry_term_sql(d, version, limit),
            ["descriptor_ui", "term"], exclude_change_cols=ex,
        ),
    }
    for table, n in counts.items():
        _log.info("mesh.{} <- {:,} rows", table, n)
    return counts


def ingest(
    *,
    year: int | None = None,
    descriptor_file: str | None = None,
    qualifier_file: str | None = None,
    schema: str = "mesh",
    limit: int | None = None,
    settings: Settings | None = None,
) -> dict:
    """End-to-end: download → stream NDJSON → bronze Parquet → upsert five tables.

    ``descriptor_file`` / ``qualifier_file`` load a local ``.xml``/``.gz`` (or an
    already-materialized ``.parquet``), skipping the download for that artifact.
    """
    s = settings or get_settings()
    year = year or s.mesh_year
    version = str(year)
    con = lake_connect(s)
    try:
        desc_raw = _bronze(con, descriptor_file, lambda: download_descriptors(year, s),
                           descriptors_to_ndjson, f"desc{year}", s)
        qual_raw = _bronze(con, qualifier_file, lambda: download_qualifiers(year, s),
                           qualifiers_to_ndjson, f"qual{year}", s)
        with ops.run(con, source="mesh", target=f"{LAKE}.{schema}", version=version) as r:
            counts = curate(con, desc_raw, qual_raw, schema=schema, version=version, limit=limit)
            r.rows = sum(counts.values())
    finally:
        con.close()
    return {**r.summary(), "schema": f"{LAKE}.{schema}", **counts}


def _bronze(con, file, fetch, to_ndjson, stem, settings) -> Path:
    """Resolve one artifact to a bronze Parquet: parquet passthrough, else XML→NDJSON→Parquet."""
    if file and str(file).endswith(".parquet"):
        return Path(file)
    xml = Path(file) if file else fetch()
    ndjson = raw_dir(_RAW, settings) / f"{stem}.ndjson.gz"
    to_ndjson(xml, ndjson)
    return materialize_raw(con, ndjson)


# --- Phase 1b: the literature edge (article ↔ MeSH), derived from omicidx PubMed ---

_PUBMED = "lake.omicidx.pubmed_article"


def _today_version() -> str:
    """Default snapshot label (the pull month) — the edge tracks an omicidx snapshot."""
    return date.today().strftime("%Y-%m")


def _article_heading_sql(source_table: str, version: str, limit: int | None) -> str:
    """Explode ``mesh_terms`` (``D…:Name [/ Q…:qual]*; …``) → (pmid, descriptor, qualifier).

    A heading with no qualifier emits one row with ``qualifier_ui = ''`` (a sentinel,
    never NULL, so the MERGE key stays matchable and re-runs are idempotent); a
    heading with N qualifiers emits N rows. The major/minor-topic flag is not present
    in the flattened ``mesh_terms`` (ADR-0010 known gap). ``limit`` caps source rows.
    """
    limit_sql = f" LIMIT {int(limit)}" if limit else ""
    return rf"""
        WITH src AS (
            SELECT TRY_CAST(pmid AS BIGINT) AS pmid, mesh_terms
            FROM {source_table}
            WHERE mesh_terms IS NOT NULL AND mesh_terms <> ''{limit_sql}
        ),
        headings AS (
            SELECT pmid, trim(h) AS heading
            FROM src, unnest(string_split(mesh_terms, ';')) AS t(h)
            WHERE trim(h) <> ''
        ),
        parsed AS (
            SELECT pmid,
                   regexp_extract(heading, '^(D\d+)', 1)  AS descriptor_ui,
                   regexp_extract_all(heading, '(Q\d+)')  AS quals
            FROM headings
        ),
        valid AS (SELECT * FROM parsed WHERE pmid IS NOT NULL AND descriptor_ui <> '')
        SELECT pmid, descriptor_ui, '' AS qualifier_ui,
               CAST('{version}' AS VARCHAR) AS snapshot_version
        FROM valid WHERE len(quals) = 0
        UNION
        SELECT pmid, descriptor_ui, unnest(quals) AS qualifier_ui,
               CAST('{version}' AS VARCHAR) AS snapshot_version
        FROM valid WHERE len(quals) > 0
    """


def curate_article_headings(
    con: duckdb.DuckDBPyConnection,
    *,
    schema: str = "mesh",
    version: str = "0",
    source_table: str = _PUBMED,
    limit: int | None = None,
) -> int:
    """Upsert ``mesh.article_heading`` (pmid ↔ descriptor ↔ qualifier) from PubMed MeSH.

    Reads the in-lake ``omicidx.pubmed_article.mesh_terms`` (which carries the
    ``D…``/``Q…`` UIs) and explodes it; ``descriptor_ui`` joins ``mesh.tree`` for
    hierarchy rollups and ``pmid`` joins ``icite``/``reporter.publink``. This is a
    **large** table (one row per article × heading × qualifier — hundreds of M);
    the MERGE re-reads the target, so a full rebuild is heavy by design.
    """
    n = upsert(
        con,
        f"{LAKE}.{schema}.article_heading",
        _article_heading_sql(source_table, version, limit),
        ["pmid", "descriptor_ui", "qualifier_ui"],
        exclude_change_cols=["snapshot_version"],
    )
    _log.info("mesh.article_heading <- {:,} rows", n)
    return n


def ingest_headings(
    *,
    schema: str = "mesh",
    version: str | None = None,
    source_table: str = _PUBMED,
    limit: int | None = None,
    settings: Settings | None = None,
) -> dict:
    """Build ``mesh.article_heading`` from omicidx PubMed (Phase 1b). No download."""
    s = settings or get_settings()
    version = version or _today_version()
    con = lake_connect(s)
    try:
        with ops.run(
            con, source="mesh", target=f"{LAKE}.{schema}.article_heading", version=version
        ) as r:
            r.rows = curate_article_headings(
                con, schema=schema, version=version, source_table=source_table, limit=limit
            )
    finally:
        con.close()
    return {**r.summary(), "table": f"{LAKE}.{schema}.article_heading"}


# --- Phase 2: Supplementary Concept Records (chemicals/substances) + Pharm Actions ---


def download_supplemental(year: int, settings: Settings | None = None) -> Path:
    """Download the gzipped SCR XML (``supp{year}.gz``, ~786 MB raw) to the raw layer."""
    s = settings or get_settings()
    return download(f"{s.mesh_xml_base}supp{year}.gz", raw_dir(_RAW, s) / f"supp{year}.xml.gz")


def download_pharmacological_actions(year: int, settings: Settings | None = None) -> Path:
    """Download the pharmacological-action XML (``pa{year}.xml``) to the raw layer."""
    s = settings or get_settings()
    return download(f"{s.mesh_xml_base}pa{year}.xml", raw_dir(_RAW, s) / f"pa{year}.xml")


def _supplemental_record(elem: ET.Element) -> dict | None:
    """SCR → ui, name, class, registry number, heading-mapped-to descriptors, terms."""
    ui = _text(elem, "SupplementalRecordUI")
    name = _text(elem, "SupplementalRecordName/String")
    if not ui or not name:
        return None
    mapped: list[dict] = []
    for hm in elem.findall("HeadingMappedToList/HeadingMappedTo"):
        d = hm.findtext("DescriptorReferredTo/DescriptorUI")
        if not d:
            continue
        q = hm.findtext("QualifierReferredTo/QualifierUI")
        mapped.append({
            "descriptor_ui": d.lstrip("*"),  # '*' marks the primary mapping
            "qualifier_ui": (q.lstrip("*") if q else ""),
            "is_primary": d.startswith("*"),
        })
    reg = next(
        (c.findtext("RegistryNumberList/RegistryNumber")
         for c in elem.findall("ConceptList/Concept") if c.get("PreferredConceptYN") == "Y"),
        None,
    )
    return {
        "supplemental_ui": ui,
        "name": name,
        "scr_class": elem.get("SCRClass"),
        "registry_number": reg if reg not in ("0", "") else None,
        "mapped_to": mapped,
        "entry_terms": _entry_terms(elem),
    }


def _pharmacological_action_record(elem: ET.Element) -> dict | None:
    """PharmacologicalAction → action descriptor UI + the substance (D/C) UIs it covers."""
    action = elem.findtext("DescriptorReferredTo/DescriptorUI")
    if not action:
        return None
    subs = [
        s.text.strip()
        for s in elem.findall("PharmacologicalActionSubstanceList/Substance/RecordUI")
        if s.text
    ]
    return {"action_ui": action, "substances": subs}


def supplemental_to_ndjson(xml_path: Path | str, ndjson_path: Path | str) -> int:
    """Stream the SCR XML → NDJSON (one structured object per supplemental record)."""
    n = _to_ndjson(Path(xml_path), Path(ndjson_path), "SupplementalRecord", _supplemental_record)
    _log.info("parsed {:,} supplemental records → {}", n, Path(ndjson_path).name)
    return n


def pharmacological_actions_to_ndjson(xml_path: Path | str, ndjson_path: Path | str) -> int:
    """Stream the pharmacological-action XML → NDJSON (one object per action)."""
    n = _to_ndjson(
        Path(xml_path), Path(ndjson_path), "PharmacologicalAction", _pharmacological_action_record
    )
    _log.info("parsed {:,} pharmacological actions → {}", n, Path(ndjson_path).name)
    return n


def _supplemental_sql(src: str, version: str, limit: int | None) -> str:
    limit_sql = f" LIMIT {int(limit)}" if limit else ""
    return f"""
        SELECT supplemental_ui, name, scr_class, registry_number,
               CAST('{version}' AS VARCHAR) AS snapshot_version
        FROM read_parquet({src}){limit_sql}
    """


def _supplemental_descriptor_sql(src: str, version: str, limit: int | None) -> str:
    """Explode the heading-mapped-to bridge (SCR → descriptor, with the primary flag)."""
    limit_sql = f" LIMIT {int(limit)}" if limit else ""
    return f"""
        WITH d AS (SELECT supplemental_ui, mapped_to FROM read_parquet({src}){limit_sql}),
             ex AS (SELECT supplemental_ui, unnest(mapped_to) AS m FROM d)
        SELECT supplemental_ui, m.descriptor_ui AS descriptor_ui,
               m.qualifier_ui AS qualifier_ui, bool_or(m.is_primary) AS is_primary,
               CAST('{version}' AS VARCHAR) AS snapshot_version
        FROM ex WHERE m.descriptor_ui IS NOT NULL
        GROUP BY supplemental_ui, m.descriptor_ui, m.qualifier_ui
    """


def _supplemental_term_sql(src: str, version: str, limit: int | None) -> str:
    limit_sql = f" LIMIT {int(limit)}" if limit else ""
    return f"""
        WITH d AS (SELECT supplemental_ui, entry_terms FROM read_parquet({src}){limit_sql}),
             ex AS (SELECT supplemental_ui, unnest(entry_terms) AS et FROM d)
        SELECT supplemental_ui, et.term AS term, bool_or(et.is_preferred) AS is_preferred,
               CAST('{version}' AS VARCHAR) AS snapshot_version
        FROM ex WHERE et.term IS NOT NULL
        GROUP BY supplemental_ui, et.term
    """


def _pharmacological_action_sql(src: str, version: str) -> str:
    return f"""
        WITH d AS (SELECT action_ui, substances FROM read_parquet({src})),
             ex AS (SELECT action_ui, unnest(substances) AS substance_ui FROM d)
        SELECT DISTINCT substance_ui, action_ui,
               CAST('{version}' AS VARCHAR) AS snapshot_version
        FROM ex WHERE substance_ui IS NOT NULL
    """


def curate_supplemental(
    con: duckdb.DuckDBPyConnection,
    supplemental_parquet: Path | str,
    *,
    schema: str = "mesh",
    version: str = "0",
    limit: int | None = None,
) -> dict[str, int]:
    """Upsert ``mesh.supplemental`` + ``supplemental_descriptor`` + ``supplemental_term``.

    SCRs are the specific chemicals/substances (~330k); each is **heading-mapped** to
    one or more descriptors (the link into the tree), not placed in it. ``scr_class``:
    1 chemical · 2 disease · 3 protocol · 4 organism.
    """
    s = csv_source(str(supplemental_parquet))
    ex = ["snapshot_version"]
    counts = {
        "supplemental": upsert(
            con, f"{LAKE}.{schema}.supplemental", _supplemental_sql(s, version, limit),
            "supplemental_ui", exclude_change_cols=ex,
        ),
        "supplemental_descriptor": upsert(
            con, f"{LAKE}.{schema}.supplemental_descriptor",
            _supplemental_descriptor_sql(s, version, limit),
            ["supplemental_ui", "descriptor_ui", "qualifier_ui"], exclude_change_cols=ex,
        ),
        "supplemental_term": upsert(
            con, f"{LAKE}.{schema}.supplemental_term", _supplemental_term_sql(s, version, limit),
            ["supplemental_ui", "term"], exclude_change_cols=ex,
        ),
    }
    for table, n in counts.items():
        _log.info("mesh.{} <- {:,} rows", table, n)
    return counts


def curate_pharmacological_actions(
    con: duckdb.DuckDBPyConnection,
    pa_parquet: Path | str,
    *,
    schema: str = "mesh",
    version: str = "0",
) -> int:
    """Upsert ``mesh.pharmacological_action`` (substance ↔ action descriptor).

    The curated cross-reference of what each drug/substance *does* — query literature
    by mechanism/effect class (``action_ui`` is a D27 descriptor; ``substance_ui`` is a
    descriptor or SCR).
    """
    n = upsert(
        con, f"{LAKE}.{schema}.pharmacological_action",
        _pharmacological_action_sql(csv_source(str(pa_parquet)), version),
        ["substance_ui", "action_ui"], exclude_change_cols=["snapshot_version"],
    )
    _log.info("mesh.pharmacological_action <- {:,} rows", n)
    return n


def ingest_supplemental(
    *,
    year: int | None = None,
    file: str | None = None,
    schema: str = "mesh",
    limit: int | None = None,
    settings: Settings | None = None,
) -> dict:
    """Load the Supplementary Concept Records (Phase 2). ``file`` skips the download."""
    s = settings or get_settings()
    year = year or s.mesh_year
    version = str(year)
    con = lake_connect(s)
    try:
        raw = _bronze(con, file, lambda: download_supplemental(year, s),
                      supplemental_to_ndjson, f"supp{year}", s)
        with ops.run(con, source="mesh", target=f"{LAKE}.{schema}", version=version) as r:
            counts = curate_supplemental(con, raw, schema=schema, version=version, limit=limit)
            r.rows = sum(counts.values())
    finally:
        con.close()
    return {**r.summary(), "schema": f"{LAKE}.{schema}", **counts}


def ingest_pharmacological_actions(
    *,
    year: int | None = None,
    file: str | None = None,
    schema: str = "mesh",
    settings: Settings | None = None,
) -> dict:
    """Load the pharmacological-action cross-reference (Phase 2). ``file`` skips download."""
    s = settings or get_settings()
    year = year or s.mesh_year
    version = str(year)
    con = lake_connect(s)
    try:
        raw = _bronze(con, file, lambda: download_pharmacological_actions(year, s),
                      pharmacological_actions_to_ndjson, f"pa{year}", s)
        with ops.run(
            con, source="mesh", target=f"{LAKE}.{schema}.pharmacological_action", version=version
        ) as r:
            r.rows = curate_pharmacological_actions(con, raw, schema=schema, version=version)
    finally:
        con.close()
    return {**r.summary(), "table": f"{LAKE}.{schema}.pharmacological_action"}


# --- Literature edge: PubMed ChemicalList → substance (descriptor or SCR) ---


def _article_chemical_sql(source_table: str, version: str, limit: int | None) -> str:
    """Explode ``chemical_list`` (``UI:Name; …``) → (pmid, substance_ui).

    ``substance_ui`` is a ``D…`` descriptor-chemical or a ``C…`` SCR — it joins
    ``mesh.descriptor``/``mesh.supplemental`` for the substance and, crucially,
    ``mesh.pharmacological_action`` to reach the drug's mechanism/effect classes.
    No qualifiers in this field, so the key ``(pmid, substance_ui)`` needs no
    sentinel. ``limit`` caps source rows.
    """
    limit_sql = f" LIMIT {int(limit)}" if limit else ""
    return rf"""
        WITH src AS (
            SELECT TRY_CAST(pmid AS BIGINT) AS pmid, chemical_list
            FROM {source_table}
            WHERE chemical_list IS NOT NULL AND chemical_list <> ''{limit_sql}
        ),
        items AS (
            SELECT pmid, trim(c) AS item
            FROM src, unnest(string_split(chemical_list, ';')) AS t(c)
            WHERE trim(c) <> ''
        )
        SELECT DISTINCT pmid, regexp_extract(item, '^([CD]\d+)', 1) AS substance_ui,
               CAST('{version}' AS VARCHAR) AS snapshot_version
        FROM items
        WHERE pmid IS NOT NULL AND regexp_extract(item, '^([CD]\d+)', 1) <> ''
    """


def curate_article_chemicals(
    con: duckdb.DuckDBPyConnection,
    *,
    schema: str = "mesh",
    version: str = "0",
    source_table: str = _PUBMED,
    limit: int | None = None,
) -> int:
    """Upsert ``mesh.article_chemical`` (pmid ↔ substance) from PubMed's ChemicalList.

    The chemical analogue of :func:`curate_article_headings`: completes
    article → substance → ``pharmacological_action`` → mechanism, and article →
    substance → ``supplemental_descriptor`` → descriptor → tree. Large (one row per
    article × chemical); the MERGE re-reads the target, so a full rebuild is heavy.
    """
    n = upsert(
        con,
        f"{LAKE}.{schema}.article_chemical",
        _article_chemical_sql(source_table, version, limit),
        ["pmid", "substance_ui"],
        exclude_change_cols=["snapshot_version"],
    )
    _log.info("mesh.article_chemical <- {:,} rows", n)
    return n


def ingest_chemicals(
    *,
    schema: str = "mesh",
    version: str | None = None,
    source_table: str = _PUBMED,
    limit: int | None = None,
    settings: Settings | None = None,
) -> dict:
    """Build ``mesh.article_chemical`` from omicidx PubMed ChemicalList. No download."""
    s = settings or get_settings()
    version = version or _today_version()
    con = lake_connect(s)
    try:
        with ops.run(
            con, source="mesh", target=f"{LAKE}.{schema}.article_chemical", version=version
        ) as r:
            r.rows = curate_article_chemicals(
                con, schema=schema, version=version, source_table=source_table, limit=limit
            )
    finally:
        con.close()
    return {**r.summary(), "table": f"{LAKE}.{schema}.article_chemical"}
