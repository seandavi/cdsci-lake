"""Shared M0 conformance fixture (cdsci-lake#96).

A tiny non-domain dataset contract + release sequence.

Two tables: ``demo.events`` (``append_immutable``) and ``demo.entities``
(``scd2_release``, two writer scopes: ``writer_a`` is the scope under test,
``writer_b`` is a sentinel that must stay untouched across every release).
Column names are deliberately domain-neutral -- no biological or cancer
vocabulary (AGENTS.md).

Both a "producer" test (build a manifest from ``DATASET_CONTRACT`` + this
fixture) and a "DuckDock-style validator" test (cold-load
``golden_manifest.json``) consume these same files.
"""

from __future__ import annotations

from cdsci.lake.contracts import ColumnContract, DatasetContract, TableContract, TemporalModel

EVENTS_TABLE = TableContract(
    name="demo.events",
    description="Append-only event log.",
    grain="one row per event_id",
    primary_key=("event_id",),
    temporal_model=TemporalModel.APPEND_IMMUTABLE,
    owner="cdsci-lake",
    license="cc0",
    columns=(
        ColumnContract("event_id", "string", "Opaque event identifier.", nullable=False),
        ColumnContract("occurred_at", "string", "ISO-8601 event timestamp.", nullable=False),
        ColumnContract("payload", "string", "Free-form event payload.", nullable=True),
    ),
)

ENTITIES_TABLE = TableContract(
    name="demo.entities",
    description="SCD2-release entity catalog spanning two writer scopes.",
    grain="one row per entity_id and validity interval",
    primary_key=("entity_id", "valid_from"),
    temporal_model=TemporalModel.SCD2_RELEASE,
    owner="cdsci-lake",
    license="cc0",
    columns=(
        ColumnContract("entity_id", "string", "Business key.", nullable=False),
        ColumnContract("label", "string", "Tracked attribute.", nullable=False),
        ColumnContract("source", "string", "Owning writer scope.", nullable=False),
        ColumnContract("valid_from", "string", "Release this interval opened.", nullable=False),
        ColumnContract(
            "valid_to", "string", "Release this interval closed, or null if current.", nullable=True
        ),
    ),
)

DATASET_CONTRACT = DatasetContract(
    id="demo-catalog",
    title="Demo Catalog",
    description="M0 conformance fixture dataset -- not a real product.",
    publisher="cdsci-lake",
    tables={"demo.events": EVENTS_TABLE, "demo.entities": ENTITIES_TABLE},
)

BUSINESS_KEY = ("entity_id",)
WRITER_A_SCOPE_SQL = "source = 'writer_a'"


def release_key(value: str) -> int:
    """This fixture's own release ordering: ``"R9"`` -> ``9``, never a raw string compare."""
    return int(value.removeprefix("R"))

# Pre-existing writer_b row, seeded directly (as if written by writer_b's own
# earlier release) before writer_a's R1 plan runs -- must stay untouched
# through every writer_a release below (§11.4 "another writer's scope").
SEED_WRITER_B_ROW = {
    "entity_id": "w1",
    "label": "zed",
    "source": "writer_b",
    "valid_from": "R0",
    "valid_to": None,
}

# New key (e1, e2, e3).
R1_INCOMING = [
    {"entity_id": "e1", "label": "alpha", "source": "writer_a"},
    {"entity_id": "e2", "label": "beta", "source": "writer_a"},
    {"entity_id": "e3", "label": "gamma", "source": "writer_a"},
]

# Same shape as R1_INCOMING plus a duplicate incoming key (e_dup x2) and a row
# outside declared scope (e_out, source=writer_c) -- used only to prove ANY
# rejection empties the whole plan (cdsci-lake#96 review F1); never chained
# into the R1->R2->R3 narrative, since a rejected release writes nothing.
R1_INCOMING_WITH_REJECTIONS = R1_INCOMING + [
    {"entity_id": "e_dup", "label": "x", "source": "writer_a"},
    {"entity_id": "e_dup", "label": "y", "source": "writer_a"},
    {"entity_id": "e_out", "label": "q", "source": "writer_c"},
]

# Identical row (e1); attribute change (e3 gamma->delta); missing from scope
# (e2 omitted -> retired); new key (e4).
R2_INCOMING = [
    {"entity_id": "e1", "label": "alpha", "source": "writer_a"},
    {"entity_id": "e3", "label": "delta", "source": "writer_a"},
    {"entity_id": "e4", "label": "new", "source": "writer_a"},
]

# Retired key reappears (e2); missing from scope (e4 omitted -> retired); new
# key (e5, drafted as "draft1"); e1/e3 carried forward unchanged.
R3_PASS1_INCOMING = [
    {"entity_id": "e1", "label": "alpha", "source": "writer_a"},
    {"entity_id": "e2", "label": "beta-again", "source": "writer_a"},
    {"entity_id": "e3", "label": "delta", "source": "writer_a"},
    {"entity_id": "e5", "label": "draft1", "source": "writer_a"},
]

# Same-release correction: e5's draft is replaced ("draft1" -> "draft2") and
# e2's identical draft is replaced with itself -- both without closing/
# reopening at valid_from="R3" (no zero-length interval).
R3_PASS2_INCOMING = [
    {"entity_id": "e1", "label": "alpha", "source": "writer_a"},
    {"entity_id": "e2", "label": "beta-again", "source": "writer_a"},
    {"entity_id": "e3", "label": "delta", "source": "writer_a"},
    {"entity_id": "e5", "label": "draft2", "source": "writer_a"},
]
