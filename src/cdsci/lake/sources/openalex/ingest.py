"""Ingest the OpenAlex snapshot into the ``openalex`` schema — DuckDB-direct-from-S3.

The OpenAlex snapshot lives in a public S3 bucket read anonymously over its https
endpoint (no credentials, no AWS account). It IS the durable raw layer — an
immutable monthly release partitioned by ``updated_date`` — so we read, project,
filter, and reconstruct directly from it and never re-stage the 639 GB locally
(ADR-0005). The ``works`` corpus (492 M records) is pruned three ways:

* **row filter** — keep only ``primary_topic.domain`` in ``openalex_domains``
  (Life + Health Sciences ≈ 116 M of 492 M); Physical/Social are dropped.
* **field prune** — drop ``mesh`` (recover via PMID ↔ omicidx), ``concepts``
  (OpenAlex-deprecated, superseded by ``topics``), the full ``locations`` array
  (keep ``primary``/``best_oa`` subsets), and ``related_works`` (derived).
* **transform** — the ``abstract_inverted_index`` is reconstructed to plaintext on
  read (smaller than the position-list JSON, and FTS-ready).

``referenced_works`` becomes a separate ``openalex.work_references`` edge table
(``work_id`` ↔ ``referenced_work_id``). The small reference entities
(``institutions`` / ``sources`` / ``funders`` / ``topics``) load as their own
tables — the ROR-keyed institution graph is the peer-center benchmarking unlock.

A laptop subset and the full server run are the *same* code: ``max_files`` caps
the number of part files; parts are processed in batches of ``batch_files`` so a
temp table never holds the whole corpus.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import duckdb

from ... import ops
from ...config import Settings, get_settings
from ...connect import LAKE, lake_connect, upsert


def _today_version() -> str:
    """Default snapshot label (the pull month) when the operator gives none."""
    return date.today().strftime("%Y-%m")


# --- abstract reconstruction (inverted index -> plaintext), correlated on alias t ---
_ABSTRACT_SQL = """(
    SELECT string_agg(_w.key, ' ' ORDER BY _p.p)
    FROM json_each(t.abstract_inverted_index) AS _w,
         unnest(CAST(_w.value AS BIGINT[])) AS _p(p)
)"""

# Columns forced on read_json: only these keys are parsed (so mesh/concepts/
# locations are never even decoded), and nested objects are kept as JSON.
_WORKS_COLUMNS: dict[str, str] = {
    "id": "VARCHAR",
    "doi": "VARCHAR",
    "title": "VARCHAR",
    "display_name": "VARCHAR",
    "publication_year": "BIGINT",
    "publication_date": "VARCHAR",
    "language": "VARCHAR",
    "type": "VARCHAR",
    "cited_by_count": "BIGINT",
    "fwci": "DOUBLE",
    "referenced_works_count": "BIGINT",
    "is_retracted": "BOOLEAN",
    "is_paratext": "BOOLEAN",
    "updated_date": "VARCHAR",
    "ids": "JSON",
    "primary_topic": "JSON",
    "open_access": "JSON",
    "primary_location": "JSON",
    "best_oa_location": "JSON",
    "authorships": "JSON",
    "topics": "JSON",
    "keywords": "JSON",
    "grants": "JSON",
    "referenced_works": "VARCHAR[]",
    "abstract_inverted_index": "JSON",
}

# domain id expressed once; used in both the projection and the row filter.
_DOMAIN_ID = "split_part(t.primary_topic->'domain'->>'id', '/', -1)"

_WORKS_PROJECTION = f"""
    split_part(t.id, '/', -1)                                       AS id,
    lower(replace(t.doi, 'https://doi.org/', ''))                   AS doi,
    TRY_CAST(regexp_extract(t.ids->>'pmid', '(\\d+)$', 1) AS BIGINT) AS pmid,
    regexp_extract(t.ids->>'pmcid', '(PMC\\d+)', 1)                  AS pmcid,
    t.ids->>'mag'                                                   AS mag_id,
    t.title,
    t.display_name,
    t.publication_year,
    TRY_CAST(t.publication_date AS DATE)                            AS publication_date,
    t.language,
    t.type,
    {_ABSTRACT_SQL}                                                 AS abstract,
    split_part(t.primary_topic->>'id', '/', -1)                     AS topic_id,
    t.primary_topic->>'display_name'                                AS topic_name,
    t.primary_topic->'subfield'->>'display_name'                    AS subfield_name,
    t.primary_topic->'field'->>'display_name'                       AS field_name,
    {_DOMAIN_ID}                                                    AS domain_id,
    t.primary_topic->'domain'->>'display_name'                      AS domain_name,
    t.open_access->>'oa_status'                                     AS oa_status,
    TRY_CAST(t.open_access->>'is_oa' AS BOOLEAN)                    AS is_oa,
    split_part(t.primary_location->'source'->>'id', '/', -1)        AS source_id,
    t.primary_location->'source'->>'display_name'                   AS source_name,
    t.primary_location->'source'->>'issn_l'                         AS source_issn_l,
    t.primary_location->'source'->>'type'                           AS source_type,
    t.best_oa_location->>'pdf_url'                                  AS oa_pdf_url,
    t.cited_by_count,
    t.fwci,
    t.is_retracted,
    t.referenced_works_count,
    t.authorships,
    t.topics,
    t.keywords,
    t.grants,
    list_transform(t.referenced_works, x -> split_part(x, '/', -1)) AS referenced_works,
    t.updated_date,
    '{{version}}'                                                   AS snapshot_version
"""


@dataclass(frozen=True)
class Entity:
    """A small OpenAlex reference entity → one ``openalex`` table."""

    name: str  # snapshot entity dir / manifest name
    table: str
    key: tuple[str, ...]
    columns: dict[str, str]  # read_json forced columns
    projection: str  # SELECT body over alias t


_INSTITUTIONS = Entity(
    name="institutions",
    table="institutions",
    key=("id",),
    columns={
        "id": "VARCHAR", "ror": "VARCHAR", "display_name": "VARCHAR",
        "country_code": "VARCHAR", "type": "VARCHAR", "works_count": "BIGINT",
        "cited_by_count": "BIGINT", "ids": "JSON", "geo": "JSON",
        "lineage": "VARCHAR[]",
    },
    projection="""
        split_part(t.id, '/', -1)                                   AS id,
        t.ror,
        t.display_name,
        t.country_code,
        t.type,
        t.works_count,
        t.cited_by_count,
        t.ids->>'grid'                                              AS grid_id,
        t.geo->>'city'                                              AS city,
        t.geo->>'region'                                            AS region,
        t.geo->>'country'                                           AS country,
        TRY_CAST(t.geo->>'latitude' AS DOUBLE)                      AS latitude,
        TRY_CAST(t.geo->>'longitude' AS DOUBLE)                     AS longitude,
        list_transform(t.lineage, x -> split_part(x, '/', -1))      AS lineage,
        '{version}'                                                 AS snapshot_version
    """,
)

_SOURCES = Entity(
    name="sources",
    table="sources",
    key=("id",),
    columns={
        "id": "VARCHAR", "issn_l": "VARCHAR", "display_name": "VARCHAR",
        "type": "VARCHAR", "host_organization": "VARCHAR",
        "host_organization_name": "VARCHAR", "country_code": "VARCHAR",
        "is_oa": "BOOLEAN", "is_in_doaj": "BOOLEAN", "works_count": "BIGINT",
        "cited_by_count": "BIGINT", "issn": "JSON",
    },
    projection="""
        split_part(t.id, '/', -1)                                   AS id,
        t.issn_l,
        t.display_name,
        t.type,
        split_part(t.host_organization, '/', -1)                    AS host_org_id,
        t.host_organization_name,
        t.country_code,
        t.is_oa,
        t.is_in_doaj,
        t.works_count,
        t.cited_by_count,
        t.issn,
        '{version}'                                                 AS snapshot_version
    """,
)

_FUNDERS = Entity(
    name="funders",
    table="funders",
    key=("id",),
    columns={
        "id": "VARCHAR", "display_name": "VARCHAR", "country_code": "VARCHAR",
        "works_count": "BIGINT", "grants_count": "BIGINT",
        "cited_by_count": "BIGINT", "ids": "JSON",
    },
    projection="""
        split_part(t.id, '/', -1)                                   AS id,
        t.display_name,
        t.country_code,
        t.ids->>'ror'                                               AS ror,
        t.ids->>'crossref'                                          AS crossref_id,
        t.works_count,
        t.grants_count,
        t.cited_by_count,
        '{version}'                                                 AS snapshot_version
    """,
)

_TOPICS = Entity(
    name="topics",
    table="topics",
    key=("id",),
    columns={
        "id": "VARCHAR", "display_name": "VARCHAR", "description": "VARCHAR",
        "keywords": "VARCHAR[]", "subfield": "JSON", "field": "JSON",
        "domain": "JSON", "works_count": "BIGINT",
    },
    projection="""
        split_part(t.id, '/', -1)                                   AS id,
        t.display_name,
        t.description,
        split_part(t.subfield->>'id', '/', -1)                      AS subfield_id,
        t.subfield->>'display_name'                                 AS subfield_name,
        split_part(t.field->>'id', '/', -1)                         AS field_id,
        t.field->>'display_name'                                    AS field_name,
        split_part(t.domain->>'id', '/', -1)                        AS domain_id,
        t.domain->>'display_name'                                   AS domain_name,
        t.keywords,
        t.works_count,
        '{version}'                                                 AS snapshot_version
    """,
)

ENTITIES: dict[str, Entity] = {
    e.table: e for e in (_INSTITUTIONS, _SOURCES, _FUNDERS, _TOPICS)
}


# --- SQL helpers ---------------------------------------------------------------

def _columns_clause(cols: dict[str, str]) -> str:
    """Render a ``read_json`` ``columns = {...}`` argument from a type map."""
    body = ", ".join(f"'{name}': '{typ}'" for name, typ in cols.items())
    return "{" + body + "}"


def _read_json(urls: list[str], cols: dict[str, str], max_obj: int) -> str:
    """A ``read_json`` source over a list of gzipped NDJSON part URLs, aliased ``t``."""
    url_list = "[" + ", ".join(f"'{u}'" for u in urls) + "]"
    return (
        f"read_json({url_list}, format='newline_delimited', compression='gzip', "
        f"columns = {_columns_clause(cols)}, maximum_object_size={max_obj}) AS t"
    )


def part_urls(
    con: duckdb.DuckDBPyConnection,
    entity: str,
    *,
    base: str,
    max_files: int | None = None,
) -> list[str]:
    """Resolve an entity's part-file URLs from its snapshot manifest (https).

    The manifest lists ``s3://openalex/...`` URLs; we rewrite them onto the public
    https endpoint so the whole pipeline runs anonymously. ``max_files`` caps the
    count for a laptop subset (deterministic order so a subset is reproducible).
    """
    limit = f"LIMIT {max_files}" if max_files else ""
    rows = con.execute(
        f"""
        SELECT replace(e.entry.url, 's3://openalex', '{base}') AS url
        FROM read_json_auto('{base}/data/{entity}/manifest') AS m,
             unnest(m.entries) AS e(entry)
        ORDER BY url {limit}
        """
    ).fetchall()
    return [r[0] for r in rows]


def _chunks(items: list[str], size: int) -> list[list[str]]:
    return [items[i : i + size] for i in range(0, len(items), size)]


def _load(
    con: duckdb.DuckDBPyConnection,
    target: str,
    source_sql: str,
    key: list[str],
    *,
    mode: str,
    exclude_change_cols: list[str] | None = None,
) -> int:
    """Dispatch to MERGE-upsert (idempotent) or append-INSERT (fast bulk).

    ``mode="merge"`` is idempotent and the right default; ``mode="append"`` skips
    reading the target (write-only) — essential for the initial bulk where part
    files are disjoint by id within a snapshot, so re-reading the growing target
    on every batch would be O(n²) (the lesson from the PMC load).
    """
    if mode == "merge":
        return upsert(con, target, source_sql, key, exclude_change_cols=exclude_change_cols)
    if mode != "append":
        raise ValueError(f"mode must be 'merge' or 'append', got {mode!r}")
    catalog, schema, _ = target.split(".")
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema};")
    con.execute(f"CREATE OR REPLACE TEMP TABLE _oa_stage AS {source_sql};")
    con.execute(f"CREATE TABLE IF NOT EXISTS {target} AS SELECT * FROM _oa_stage WHERE false;")
    con.execute(f"INSERT INTO {target} SELECT * FROM _oa_stage;")
    return con.execute(f"SELECT count(*) FROM {target}").fetchone()[0]


# --- works + references --------------------------------------------------------

# Unnest authorships JSON → (work, author, institution) affiliation edges. The
# inner UNNEST over ``institutions`` naturally drops authorships with no institution
# (an empty list yields no rows), so every emitted row has a non-null institution —
# the ROR key for benchmarking — and a non-null author, a stable composite key for
# idempotent MERGE. Authors with no affiliation are not represented here
# (re-derivable from the snapshot if ever needed). ``from_json`` is the *lenient*
# transform: it pulls only the keys named below and ignores the rest, so OpenAlex
# adding authorship/institution fields (or a decade of schema drift across the
# historical partitions) does not break the load — unlike a strict ``CAST``.
_AUTHORSHIPS_STRUCT = (
    '[{"author_position":"VARCHAR","is_corresponding":"BOOLEAN",'
    '"author":{"id":"VARCHAR","display_name":"VARCHAR"},'
    '"institutions":[{"id":"VARCHAR","ror":"VARCHAR","country_code":"VARCHAR"}]}]'
)
_AUTHORSHIPS_SQL = f"""
    SELECT
        work_id,
        split_part(a.author.id, '/', -1)        AS author_id,
        a.author.display_name                   AS author_name,
        a.author_position                       AS author_position,
        a.is_corresponding                      AS is_corresponding,
        split_part(i.id, '/', -1)               AS institution_id,
        i.ror                                   AS institution_ror,
        i.country_code                          AS institution_country,
        snapshot_version
    FROM (
        SELECT
            id AS work_id, snapshot_version,
            unnest(from_json(authorships, '{_AUTHORSHIPS_STRUCT}')) AS a
        FROM _oa_raw WHERE authorships IS NOT NULL
    ) sub, unnest(sub.a.institutions) AS t(i)
    WHERE a.author.id IS NOT NULL AND i.id IS NOT NULL
"""


def curate_works_batch(
    con: duckdb.DuckDBPyConnection,
    urls: list[str],
    *,
    schema: str,
    version: str,
    mode: str,
    domains: list[str],
    max_obj: int,
) -> dict:
    """Project + filter one batch of works part files; load works + both edge tables.

    Reads the parts **once** into a temp table (``_oa_raw``), then derives three
    targets from it so the S3 bytes are read once: the ``works`` row (minus the
    ``referenced_works`` list and the ``authorships`` JSON — both promoted to edge
    tables), the ``work_references`` citation edges, and the ``works_authorships``
    affiliation edges. Returns row-count totals keyed by table.
    """
    domain_in = ", ".join(f"'{d}'" for d in domains)
    projection = _WORKS_PROJECTION.format(version=version)
    con.execute(
        f"CREATE OR REPLACE TEMP TABLE _oa_raw AS "
        f"SELECT {projection} FROM {_read_json(urls, _WORKS_COLUMNS, max_obj)} "
        f"WHERE {_DOMAIN_ID} IN ({domain_in});"
    )
    works = _load(
        con,
        f"{LAKE}.{schema}.works",
        "SELECT * EXCLUDE (referenced_works, authorships) FROM _oa_raw",
        ["id"],
        mode=mode,
        exclude_change_cols=["snapshot_version"],
    )
    refs = _load(
        con,
        f"{LAKE}.{schema}.work_references",
        "SELECT id AS work_id, unnest(referenced_works) AS referenced_work_id, "
        "snapshot_version FROM _oa_raw "
        "WHERE referenced_works IS NOT NULL AND len(referenced_works) > 0",
        ["work_id", "referenced_work_id"],
        mode=mode,
        exclude_change_cols=["snapshot_version"],
    )
    auth = _load(
        con,
        f"{LAKE}.{schema}.works_authorships",
        _AUTHORSHIPS_SQL,
        ["work_id", "author_id", "institution_id"],
        mode=mode,
        exclude_change_cols=["snapshot_version"],
    )
    con.execute("DROP TABLE IF EXISTS _oa_raw;")
    return {"works": works, "references": refs, "authorships": auth}


def ingest_works(
    *,
    schema: str = "openalex",
    version: str | None = None,
    mode: str = "merge",
    max_files: int | None = None,
    batch_files: int | None = None,
    settings: Settings | None = None,
    con: duckdb.DuckDBPyConnection | None = None,
) -> dict:
    """Load the works corpus (pruned + abstract-reconstructed) and the edge table."""
    s = settings or get_settings()
    version = version or _today_version()
    max_files = max_files if max_files is not None else s.openalex_max_files
    batch = batch_files or s.openalex_batch_files
    owns = con is None
    con = con or lake_connect(s)
    try:
        with ops.run(con, source="openalex", target=f"{LAKE}.{schema}.works", version=version) as r:
            urls = part_urls(con, "works", base=s.openalex_s3_base, max_files=max_files)
            batches = _chunks(urls, batch)
            totals = {"works": 0, "references": 0, "authorships": 0}
            for n, chunk in enumerate(batches, 1):
                totals = curate_works_batch(
                    con, chunk, schema=schema, version=version, mode=mode,
                    domains=s.openalex_domains, max_obj=s.openalex_max_object_bytes,
                )
                print(f"  works batch {n}/{len(batches)} ({len(chunk)} parts): "
                      f"works={totals['works']:,} refs={totals['references']:,} "
                      f"authorships={totals['authorships']:,}")
            r.rows = totals["works"]
    finally:
        if owns:
            con.close()
    return {**r.summary(), "schema": schema, "parts": len(urls), **totals}


def ingest_entity(
    entity: str,
    *,
    schema: str = "openalex",
    version: str | None = None,
    mode: str = "merge",
    max_files: int | None = None,
    settings: Settings | None = None,
    con: duckdb.DuckDBPyConnection | None = None,
) -> int:
    """Load one small reference entity (institutions/sources/funders/topics)."""
    s = settings or get_settings()
    version = version or _today_version()
    spec = ENTITIES[entity]
    owns = con is None
    con = con or lake_connect(s)
    try:
        urls = part_urls(con, spec.name, base=s.openalex_s3_base, max_files=max_files)
        projection = spec.projection.format(version=version)
        source_sql = (
            f"SELECT {projection} "
            f"FROM {_read_json(urls, spec.columns, s.openalex_max_object_bytes)}"
        )
        return _load(
            con, f"{LAKE}.{schema}.{spec.table}", source_sql, list(spec.key),
            mode=mode, exclude_change_cols=["snapshot_version"],
        )
    finally:
        if owns:
            con.close()


def run(
    *,
    entities: list[str] | None = None,
    works: bool = True,
    schema: str = "openalex",
    version: str | None = None,
    mode: str = "merge",
    max_files: int | None = None,
    settings: Settings | None = None,
) -> dict:
    """Load reference entities first (the benchmarking unlock), then works."""
    s = settings or get_settings()
    version = version or _today_version()
    con = lake_connect(s)
    counts: dict[str, int] = {}
    try:
        for entity in entities if entities is not None else list(ENTITIES):
            counts[entity] = ingest_entity(
                entity, schema=schema, version=version, mode=mode,
                max_files=max_files, settings=s, con=con,
            )
            print(f"  {entity}: {counts[entity]:,}")
        if works:
            w = ingest_works(
                schema=schema, version=version, mode=mode,
                max_files=max_files, settings=s, con=con,
            )
            counts["works"], counts["work_references"] = w["works"], w["references"]
    finally:
        con.close()
    return {"schema": schema, "version": version, "counts": counts}
