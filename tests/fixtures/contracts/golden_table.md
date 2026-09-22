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
