# Demo Catalog

M0 conformance fixture dataset -- not a real product.

- **Publisher:** cdsci-lake
- **Required artifacts:** ducklake, parquet

# demo.entities

Entity catalog: one current row per entity across two writer scopes.

- **Grain:** one row per entity_id
- **Primary key:** entity_id
- **Temporal model:** `upsert_latest_snapshot` — One mutable current row per natural key, updated only when tracked values change.
- **Owner:** cdsci-lake
- **License:** cc0

| Column | Type | Nullable | Description | Identifier Namespace | Units | Coordinate System | Null Meaning | Enum |
|---|---|---|---|---|---|---|---|---|
| entity_id | string | No | Business key. |  |  |  |  |  |
| label | string | No | Tracked attribute. |  |  |  |  |  |
| source | string | No | Owning writer scope. |  |  |  |  |  |

# demo.events

Append-only event log.

- **Grain:** one row per event_id
- **Primary key:** event_id
- **Temporal model:** `append_immutable` — Existing records are never revised; used for immutable event/artifact sources.
- **Owner:** cdsci-lake
- **License:** cc0

| Column | Type | Nullable | Description | Identifier Namespace | Units | Coordinate System | Null Meaning | Enum |
|---|---|---|---|---|---|---|---|---|
| event_id | string | No | Opaque event identifier. |  |  |  |  |  |
| occurred_at | string | No | ISO-8601 event timestamp. |  |  |  |  |  |
| payload | string | Yes | Free-form event payload. |  |  |  |  |  |
