"""OBO Foundry ontologies' OBO Graphs JSON -> ``lake.obo.element`` (EL only; bioc-on-ice#183).

Eight ontologies, one file each: CL, UBERON, MONDO, EFO, HsapDv, MmusDv, GO
(``go-basic``) and DOID. OBO Graphs JSON is the JSON serialization of the OWL
graph: ``graphs[0].nodes`` (classes, properties) and ``graphs[0].edges``
(``sub``, ``pred``, ``obj``). Ported from bioc-on-ice's ``src/bioconice/obo.py``
(its ``raw.obo__<ontology>`` landings); bioc-on-ice derives ``ontology.term``
and ``ontology.relationship`` from this table at release time (bioc-on-ice
ADR-0015), and publishes ``raw.obo__<o>`` as ``SELECT * EXCLUDE (ontology)
... WHERE ontology = '<o>'``.

**One row per node and one per edge, the whole file.** Imported classes from
other namespaces (BFO, RO, PR in CL) land too. ``meta`` is the node's or
edge's ``meta`` object as JSON text, verbatim. A node is identified by its
``id``, an edge by ``(sub, pred, obj)``, so the key is ``(ontology, kind, id,
sub, pred, obj)``, null-safe; it held, with no NULL id or edge end, in all
eight Iceberg tables (2026-11-03), and a file that breaks it is refused.

**Versioned in-band.** ``release_version`` is the file's own
``graphs[0].meta.version`` (a version IRI), never invented. ``version`` names
the upstream release to download (a GitHub tag such as ``v2026-06-08`` or
``v3.94.0``, or GO's release directory ``2026-08-05``), which is not always
the in-band date: UBERON's 2026-06-19 build sits under tag ``v2026-06-23``.
Each ontology is its own slice: a full load prunes that ontology's rows only.

**The graph is checked whole before anything lands**: one JSON document whose
``graphs[0]`` has a version, a ``nodes`` array with at least one node and an
``edges`` array; every node has an ``id`` and every edge its three ends.

The read is the lander's, verbatim: the document is read as one JSON value and
walked with JSON paths, not as an inferred struct (a file whose edges or nodes
carry no ``meta`` would then fail to bind), and the arrays are pulled out once
(extracting them per row re-parses the document each time).
"""

from __future__ import annotations

import re
from datetime import date
from pathlib import Path

import duckdb

from ... import ops
from ...config import Settings, get_settings
from ...connect import LAKE, csv_source, lake_connect, raw_dir, upsert
from ...download import download

_RAW = "obo"
KEY = ["ontology", "kind", "id", "sub", "pred", "obj"]

# EFO's release is ~175 MB of one JSON object; DuckDB's default is 16 MB. ponytail:
# bump this if a release ever exceeds it, none is within 2.5x of it today.
MAX_OBJECT_SIZE = 500_000_000


def _graph_sql(path: Path) -> str:
    return f"""
        SELECT json_extract(graphs, '$[0].nodes')::JSON[] AS nodes,
               json_extract(graphs, '$[0].edges')::JSON[] AS edges,
               graphs->>'$[0].meta.version' AS version
        FROM read_json({csv_source([path])}, columns={{'graphs': 'JSON'}},
                       maximum_object_size={MAX_OBJECT_SIZE})
    """


def file_version(con: duckdb.DuckDBPyConnection, path: Path) -> str | None:
    """The file's ``graphs[0].meta.version`` (one read of the whole document)."""
    row = con.execute(f"SELECT version FROM ({_graph_sql(path)})").fetchone()
    return row and row[0]


def download_json(ontology: str, version: str | None = None,
                  settings: Settings | None = None) -> Path:
    """Download one ontology's OBO Graphs JSON into the raw layer, keyed by ``version``.

    ``version`` is upstream's name for a release; with none the newest is
    fetched and keyed by the retrieval date.
    """
    s = settings or get_settings()
    url = s.obo_release_urls[ontology].format(version=version) if version else s.obo_urls[ontology]
    dest = raw_dir(_RAW, s) / ontology / (version or date.today().isoformat())
    return download(url, dest / url.rsplit("/", 1)[-1])


def _stage(con: duckdb.DuckDBPyConnection, ontology: str, path: Path) -> str:
    """Parse the whole file into ``_obo_element``, checking its structure; return its version."""
    con.execute(f"CREATE OR REPLACE TEMP TABLE _obo_graph AS {_graph_sql(path)}")
    graphs, version, nodes, edges = con.execute(
        "SELECT count(*), any_value(version), any_value(len(nodes)), any_value(len(edges)) "
        "FROM _obo_graph").fetchone()
    if graphs != 1:
        raise ValueError(f"obo {ontology}: {path} is {graphs} JSON documents, not one")
    if not version:
        raise ValueError(f"obo {ontology}: {path} carries no graphs[0].meta.version")
    if nodes is None or edges is None:
        raise ValueError(f"obo {ontology}: {path} has no graphs[0].nodes or graphs[0].edges array")
    if not nodes:
        raise ValueError(f"obo {ontology}: {path} has zero nodes")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE _obo_element AS
        SELECT '{ontology}' AS ontology, 'node' AS kind, n->>'id' AS id, n->>'lbl' AS lbl,
               NULL::VARCHAR AS sub, NULL::VARCHAR AS pred, NULL::VARCHAR AS obj,
               (n->'meta')::VARCHAR AS meta, version AS release_version
        FROM _obo_graph AS g, unnest(g.nodes) AS t(n)
      UNION ALL
        SELECT '{ontology}', 'edge', NULL, NULL, e->>'sub', e->>'pred', e->>'obj',
               (e->'meta')::VARCHAR, version
        FROM _obo_graph AS g, unnest(g.edges) AS t(e)
    """)
    con.execute("DROP TABLE _obo_graph")
    count = "SELECT count(*) FROM _obo_element WHERE "
    checks = {
        "a node without an id": count + "kind = 'node' AND id IS NULL",
        "an edge without sub, pred or obj":
            count + "kind = 'edge' AND (sub IS NULL OR pred IS NULL OR obj IS NULL)",
        "a repeated node or edge": f"SELECT count(*) FROM (SELECT 1 FROM _obo_element "
                                   f"GROUP BY {', '.join(KEY)} HAVING count(*) > 1)",
    }
    failed = {k: n for k, q in checks.items() if (n := con.execute(q).fetchone()[0])}
    if failed:
        raise ValueError(f"obo {ontology}: {path} has "
                         + "; ".join(f"{k} ({n:,})" for k, n in failed.items()))
    return version


def curate(
    con: duckdb.DuckDBPyConnection,
    ontology: str,
    path: Path,
    version: str,
    *,
    target: str | None = None,
    limit: int | None = None,
) -> int:
    """MERGE-upsert one ontology's file into ``element``; return the table's row count.

    ``version`` must be the file's own ``graphs[0].meta.version``. The file is
    checked whole before any row lands. A full load prunes that ontology's nodes
    and edges the release no longer has; ``limit`` never prunes.
    ``release_version`` is compared like any column, so a new release restamps
    every row of the ontology: its slice is one release, never a mix.
    """
    # the name is interpolated into the SQL and the prune
    if not re.fullmatch(r"[a-z][a-z0-9_]*", ontology):
        raise ValueError(f"obo: {ontology!r} is not an ontology name")
    # limit=0 is falsy: it would read every row and prune as a full load
    if limit is not None and limit < 1:
        raise ValueError(f"obo: limit must be positive, got {limit}")
    if (found := _stage(con, ontology, path)) != version:
        raise ValueError(f"obo {ontology}: {path} is version {found!r}, not {version!r}")
    limit_sql = f" LIMIT {int(limit)}" if limit else ""
    target = target or f"{LAKE}.obo.element"
    return upsert(
        con, target, f"SELECT * FROM _obo_element{limit_sql}",
        key=KEY, prune=None if limit else f"ontology = '{ontology}'",
    )


def ingest(
    *,
    ontology: str | None = None,
    file: str | None = None,
    version: str | None = None,
    schema: str = "obo",
    limit: int | None = None,
    settings: Settings | None = None,
) -> list[dict]:
    """End-to-end, one ``ops.run`` per ontology: download (unless ``file``) -> MERGE-upsert.

    ``ontology`` picks one; with none, every ontology in ``Settings.obo_urls``
    lands, newest release each. ``version`` (upstream's name for a release) or
    ``file`` (a local OBO Graphs JSON) needs ``ontology``.
    """
    s = settings or get_settings()
    if (file or version) and not ontology:
        raise ValueError("obo: file and version are one ontology's; pass ontology")
    if file and version:
        raise ValueError("obo: version names a release to download; a file carries its own")
    if ontology and ontology not in s.obo_urls:
        raise ValueError(f"obo: {ontology!r} is not one of {sorted(s.obo_urls)}")
    # the version is interpolated into a URL and a path
    if version and not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", version):
        raise ValueError(f"obo: version {version!r} is not a release name")
    target = f"{LAKE}.{schema}.element"
    summaries = []
    for name in [ontology] if ontology else list(s.obo_urls):
        path = Path(file) if file else download_json(name, version, s)
        con = lake_connect(s)
        try:
            in_band = file_version(con, path)
            if not in_band:
                raise ValueError(f"obo {name}: {path} carries no graphs[0].meta.version")
            with ops.run(con, source="obo", target=target, version=in_band) as r:
                r.rows = curate(con, name, path, in_band, target=target, limit=limit)
        finally:
            con.close()
        summaries.append({**r.summary(), "ontology": name})
    return summaries
