//! Columnar engine base operations dispatched to the Data Plane.
//!
//! `ColumnarOp` is the base for all columnar-profile collections:
//! - **Plain columnar**: analytics collections without time semantics.
//! - **Timeseries**: extends with `time_range` + `bucket_interval_ms` (via `TimeseriesOp`).
//! - **Spatial**: extends with R-tree + OGC predicates (via `SpatialOp`).
//!
//! All profiles share the same `ColumnarMemtable` → `SegmentWriter` infrastructure.

/// Intent carried on `ColumnarOp::Insert` — see enum docs.
#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum ColumnarInsertIntent {
    /// Plain `INSERT`. On columnar, duplicate PK is **not** an error —
    /// the prior row is tombstoned via the segment's delete bitmap and
    /// the new row is appended. Cross-engine SQL consistency is kept on
    /// the read side (`SELECT WHERE pk = X` returns one row) rather than
    /// the insert-error side, matching ClickHouse's append + dedup model.
    Insert,
    /// `INSERT ... ON CONFLICT DO NOTHING`: silent no-op on duplicate.
    InsertIfAbsent,
    /// `UPSERT` / `INSERT ... ON CONFLICT (pk) DO UPDATE`. Behaves like
    /// `Insert` when `on_conflict_updates` is empty (overwrite); when
    /// non-empty, the handler reads the existing row, applies the
    /// assignments (with `EXCLUDED.col` bound to the incoming row), and
    /// writes the merged result.
    Put,
}

/// Base columnar physical operations.
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum ColumnarOp {
    /// Read rows from columnar memtable + segments.
    ///
    /// Applies filters, projects columns, respects limit.
    /// No time-range semantics — that's `TimeseriesOp::Scan`.
    Scan {
        collection: String,
        projection: Vec<String>,
        limit: usize,
        filters: Vec<u8>,
        rls_filters: Vec<u8>,
        /// ORDER BY clause: `(field_name, ascending)` pairs, applied
        /// against matching rows before the `limit` is enforced. Empty
        /// for scans with no ORDER BY.
        sort_keys: Vec<(String, bool)>,
        /// Bitemporal system-time cutoff: read rows whose `_ts_system`
        /// is ≤ this value. `None` = current-state read. Only populated
        /// for collections created `WITH BITEMPORAL`.
        #[serde(default)]
        #[msgpack(default)]
        system_as_of_ms: Option<i64>,
        /// Bitemporal valid-time predicate: keep rows whose
        /// `[_ts_valid_from, _ts_valid_until)` interval contains this
        /// point. `None` = no valid-time filter.
        #[serde(default)]
        #[msgpack(default)]
        valid_at_ms: Option<i64>,
    },

    /// Insert rows into a columnar memtable.
    ///
    /// Accepts JSON or MessagePack payload. The memtable is created on
    /// first insert with schema inferred from the payload.
    ///
    /// `intent` distinguishes plain `INSERT` (upsert-semantics on columnar:
    /// duplicate PK tombstones the prior row and appends the new one) from
    /// `ON CONFLICT DO NOTHING` (`InsertIfAbsent` — silent skip on dup) and
    /// `UPSERT` / `ON CONFLICT (pk) DO UPDATE` (`Put` — optionally with
    /// per-row merges in `on_conflict_updates`).
    Insert {
        collection: String,
        /// Row data. Format determined by `format` field.
        payload: Vec<u8>,
        /// "json" for JSON array of objects, "msgpack" for MessagePack,
        /// "ilp" for InfluxDB Line Protocol (delegated to timeseries path).
        format: String,
        /// INSERT / INSERT IF ABSENT / UPSERT distinction. See
        /// `ColumnarInsertIntent` for semantics per variant.
        intent: ColumnarInsertIntent,
        /// `ON CONFLICT (pk) DO UPDATE SET field = expr` assignments.
        /// Carried only when `intent == Put`; empty for `Insert`,
        /// `InsertIfAbsent`, and plain `UPSERT` (whole-row overwrite).
        /// Each `UpdateValue` is either a literal msgpack bytes payload
        /// or a `SqlExpr` the handler evaluates against the existing row
        /// plus the would-be-inserted row (with `EXCLUDED.col` resolution).
        on_conflict_updates: Vec<(String, super::document::UpdateValue)>,
    },

    /// Update rows matching filter predicates.
    ///
    /// Uses `MutationEngine` for plain/spatial profiles.
    /// `updates` is a list of (field_name, json_value_bytes) pairs.
    Update {
        collection: String,
        /// Serialized `Vec<ScanFilter>` (MessagePack).
        filters: Vec<u8>,
        /// Field assignments: `(column_name, json_value_bytes)`.
        updates: Vec<(String, Vec<u8>)>,
    },

    /// Delete rows matching filter predicates.
    ///
    /// Uses `MutationEngine` for plain/spatial profiles.
    Delete {
        collection: String,
        /// Serialized `Vec<ScanFilter>` (MessagePack).
        filters: Vec<u8>,
    },
}
