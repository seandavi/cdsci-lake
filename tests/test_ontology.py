"""Offline tests for the ontology source — curate a tiny hand-built semsql DB.

Builds a minimal SQLite database shaped like a semantic-sql build (a ``statements``
triple table + an ``edge`` relation), runs ``curate`` into a temp DuckLake, and
asserts the four projections, idempotency, and the recursive-CTE ancestor closure
that replaces a materialized ``entailed_edge``. No network.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from cdsci.lake import Settings, lake_connect, snapshots
from cdsci.lake.sources.ontology import curate

# (subject, predicate, object, value) — literals in value, IRI objects in object,
# mirroring the real semsql encoding verified against hancestro.db.
_STATEMENTS = [
    ("XO:0000001", "rdfs:label", None, "anatomical entity"),
    ("XO:0000001", "IAO:0000115", None, "A material anatomical entity."),
    ("XO:0000002", "rdfs:label", None, "organ"),
    ("XO:0000002", "oio:hasExactSynonym", None, "body organ"),
    ("XO:0000002", "oio:hasBroadSynonym", None, "structure"),
    ("XO:0000002", "oio:hasDbXref", None, "UMLS:C0178784"),
    ("XO:0000003", "rdfs:label", None, "heart"),
    ("XO:0000003", "oio:hasDbXref", None, "FMA:7088"),
    ("XO:0000099", "rdfs:label", None, "obsolete ventricle"),
    ("XO:0000099", "owl:deprecated", None, "true"),
    ("XO:0000099", "IAO:0100001", None, "XO:0000003"),
]
# asserted direct edges: organ is_a entity, heart is_a organ
_EDGES = [
    ("XO:0000002", "rdfs:subClassOf", "XO:0000001"),
    ("XO:0000003", "rdfs:subClassOf", "XO:0000002"),
]


@pytest.fixture
def semsql_db(tmp_path: Path) -> Path:
    """A minimal semantic-sql-shaped SQLite DB (statements + edge + prefix)."""
    db = tmp_path / "xo.db"
    con = sqlite3.connect(db)
    con.execute(
        "CREATE TABLE statements (stanza TEXT, subject TEXT, predicate TEXT, "
        "object TEXT, value TEXT, datatype TEXT, language TEXT, graph TEXT)"
    )
    con.executemany(
        "INSERT INTO statements (subject, predicate, object, value) VALUES (?, ?, ?, ?)",
        _STATEMENTS,
    )
    con.execute("CREATE TABLE edge (subject TEXT, predicate TEXT, object TEXT)")
    con.executemany("INSERT INTO edge VALUES (?, ?, ?)", _EDGES)
    con.execute("CREATE TABLE prefix (prefix TEXT, base TEXT)")
    con.commit()
    con.close()
    return db


@pytest.fixture
def lake_settings(tmp_path: Path) -> Settings:
    return Settings(storage_base_uri=f"file://{tmp_path}")


def test_ontology_curate(semsql_db: Path, lake_settings: Settings):
    con = lake_connect(lake_settings)
    try:
        counts = curate(con, "xo", semsql_db, "2026-06-26")
        assert counts == {"terms": 4, "synonyms": 2, "xrefs": 2, "edges": 2}

        # terms: label + definition; obsolete + replaced_by captured.
        heart = con.execute(
            "SELECT label, obsolete, replaced_by FROM lake.ontology.terms "
            "WHERE ontology = 'xo' AND curie = 'XO:0000003'"
        ).fetchone()
        assert heart == ("heart", False, None)

        obs = con.execute(
            "SELECT obsolete, replaced_by FROM lake.ontology.terms "
            "WHERE curie = 'XO:0000099'"
        ).fetchone()
        assert obs == (True, "XO:0000003")

        # synonyms carry scope; xrefs captured.
        scopes = dict(
            con.execute(
                "SELECT synonym, scope FROM lake.ontology.synonyms WHERE curie = 'XO:0000002'"
            ).fetchall()
        )
        assert scopes == {"body organ": "exact", "structure": "broad"}

        # the discriminator is present on every row.
        assert con.execute(
            "SELECT count(*) FROM lake.ontology.terms WHERE ontology = 'xo'"
        ).fetchone()[0] == 4

        # recursive-CTE ancestor closure over edges (replaces entailed_edge):
        # heart -> organ -> entity = 2 ancestors.
        ancestors = con.execute(
            """
            WITH RECURSIVE anc(start, node) AS (
                SELECT subject, object FROM lake.ontology.edges
                  WHERE predicate = 'rdfs:subClassOf'
                UNION
                SELECT a.start, e.object FROM anc a
                  JOIN lake.ontology.edges e
                    ON e.predicate = 'rdfs:subClassOf' AND e.subject = a.node
            )
            SELECT node FROM anc WHERE start = 'XO:0000003' ORDER BY node
            """
        ).fetchall()
        assert [r[0] for r in ancestors] == ["XO:0000001", "XO:0000002"]

        assert len(snapshots(con)) >= 1
    finally:
        con.close()


def test_ontology_curate_idempotent(semsql_db: Path, lake_settings: Settings):
    """A second curate of unchanged data adds no rows (MERGE is a no-op)."""
    con = lake_connect(lake_settings)
    try:
        first = curate(con, "xo", semsql_db, "2026-06-26")
        again = curate(con, "xo", semsql_db, "2026-06-26")
        assert first == again
    finally:
        con.close()
