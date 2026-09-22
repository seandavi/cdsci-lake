# Demo Catalog

M0 conformance fixture dataset -- not a real product.

- **Publisher:** cdsci-lake
- **Required artifacts:** ducklake, parquet

# demo.entities

SCD2-release entity catalog spanning two writer scopes.

- **Grain:** one row per entity_id and validity interval
- **Primary key:** entity_id, valid_from
- **Temporal model:** `scd2_release` — Type-2 history: an attribute change closes the old [valid_from, valid_to) interval and opens a new one; at most one current row per business key.
- **Owner:** cdsci-lake
- **License:** cc0

| Column | Type | Nullable | Description | Identifier Namespace | Units | Coordinate System | Null Meaning | Enum |
|---|---|---|---|---|---|---|---|---|
| entity_id | string | No | Business key. |  |  |  |  |  |
| label | string | No | Tracked attribute. |  |  |  |  |  |
| source | string | No | Owning writer scope. |  |  |  |  |  |
| valid_from | string | No | Release this interval opened. |  |  |  |  |  |
| valid_to | string | Yes | Release this interval closed, or null if current. |  |  |  |  |  |

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
