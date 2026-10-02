"""Shared M0 conformance fixture (cdsci-lake#96).

A tiny non-domain dataset contract + release sequence.

Two tables: ``demo.events`` (``append_immutable``, an append log) and
``demo.entities`` (``upsert_latest_snapshot``, a current-state catalog: one
current row per entity across two writer scopes).
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
    description="Entity catalog: one current row per entity across two writer scopes.",
    grain="one row per entity_id",
    primary_key=("entity_id",),
    temporal_model=TemporalModel.UPSERT_LATEST_SNAPSHOT,
    owner="cdsci-lake",
    license="cc0",
    columns=(
        ColumnContract("entity_id", "string", "Business key.", nullable=False),
        ColumnContract("label", "string", "Tracked attribute.", nullable=False),
        ColumnContract("source", "string", "Owning writer scope.", nullable=False),
    ),
)

DATASET_CONTRACT = DatasetContract(
    id="demo-catalog",
    title="Demo Catalog",
    description="M0 conformance fixture dataset -- not a real product.",
    publisher="cdsci-lake",
    tables={"demo.events": EVENTS_TABLE, "demo.entities": ENTITIES_TABLE},
)
