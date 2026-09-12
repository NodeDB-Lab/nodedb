// SPDX-License-Identifier: Apache-2.0

//! Columnar engine base operations dispatched to the Data Plane.
//!
//! `ColumnarOp` is the base for all columnar-profile collections:
//! - **Plain columnar**: analytics collections without time semantics.
//! - **Timeseries**: extends with `time_range` + `bucket_interval_ms` (via `TimeseriesOp`).
//! - **Spatial**: extends with R-tree + OGC predicates (via `SpatialOp`).
//!
//! All profiles share the same `ColumnarMemtable` → `SegmentWriter` infrastructure.

use nodedb_types::{
    QualifiedCollection, RlsWriteCheck, Surrogate, SurrogateBitmap, SystemTimeScope,
};

use crate::physical_plan::document::ReturningSpec;

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
    /// Plain `INSERT` on a collection whose `PRIMARY KEY` is declared on a
    /// natural key column (not `id` / `document_id`). Duplicate PK refuses
    /// the row with `RejectedConstraint` (SQLSTATE 23505) instead of the
    /// `Insert` tombstone-and-append behavior above.
    InsertUnique,
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
        collection: QualifiedCollection,
        projection: Vec<String>,
        limit: usize,
        filters: Vec<u8>,
        rls_filters: Vec<u8>,
        /// ORDER BY terms, each an expression, applied against matching
        /// rows before the `limit` is enforced. Empty for scans with no
        /// ORDER BY.
        sort_keys: Vec<crate::physical_plan::SortKeySpec>,
        /// System-time selection. `Current` = current-state read; `AsOf(ms)` =
        /// rows whose `_ts_system` is ≤ ms; `AllVersions` = every `_ts_system`
        /// row ordered ascending (audit log), system-time column projected.
        /// Only meaningful for collections created `WITH BITEMPORAL`.
        #[serde(default)]
        system_time: SystemTimeScope,
        /// Bitemporal valid-time predicate: keep rows whose
        /// `[_ts_valid_from, _ts_valid_until)` interval contains this
        /// point. `None` = no valid-time filter.
        #[serde(default)]
        valid_at_ms: Option<i64>,
        /// Optional surrogate prefilter injected by a cross-engine sub-plan.
        /// When present, the scan skips rows whose surrogate is absent from
        /// this bitmap. `None` = no prefilter; full collection is scanned.
        #[serde(default)]
        prefilter: Option<SurrogateBitmap>,
        /// MessagePack-serialized `Vec<ComputedColumn>` for scalar projection
        /// expressions (e.g. JSON arrow operators). Empty when no computed
        /// columns are present in the query.
        #[serde(default)]
        computed_columns: Vec<u8>,
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
        collection: QualifiedCollection,
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
        /// Per-row stable cross-engine identities, parallel to the rows
        /// in `payload`. CP-side assigner populates this in row order
        /// before dispatch. `vec![]` only in test fixtures (and length
        /// must equal the row count when populated).
        surrogates: Vec<Surrogate>,
        /// MessagePack-serialized `ColumnarSchema` from the DDL catalog.
        /// When non-empty, the Data Plane uses this schema to initialize the
        /// memtable engine instead of inferring the schema from the payload.
        /// This is required for columns whose SQL type is ambiguous at the
        /// Value level (e.g. JSON arrives as `Value::String` but must be
        /// stored in `ColumnData::Json`, not `ColumnData::String`).
        /// Empty `vec![]` only in legacy test fixtures that do not carry schema.
        #[serde(default)]
        schema_bytes: Vec<u8>,
        /// Sync provenance: identifies the originating peer and sequence for idempotency.
        #[serde(default)]
        provenance: Option<nodedb_types::sync::wire::SyncProvenance>,
        /// WAL record LSN for deduplication. Set by the WAL catch-up task so the
        /// Data Plane can skip records that have already been ingested or flushed
        /// to disk. `None` for live ingest (always accepted).
        #[serde(default)]
        wal_lsn: Option<u64>,
        /// Compiled row-level-security WRITE predicate (`Vec<ScanFilter>` as
        /// MessagePack), or the reason no predicate is attached. Carried only
        /// for the ON CONFLICT DO UPDATE shape, whose merged post-image
        /// exists only inside the handler; a plain insert's rows are decided
        /// at plan time instead.
        rls_write_check: RlsWriteCheck,
        /// When `Some`, return the STORED post-image of each written row
        /// projected per spec — the row assembled from the values that reached
        /// the engine, in schema order, so it matches what a `SELECT` on the
        /// same key produces. Never the caller's submitted body: an `ON
        /// CONFLICT DO UPDATE` merges against the stored row, so an echo of the
        /// request would report a row that does not exist.
        #[serde(default)]
        returning: Option<ReturningSpec>,
        /// Read filters gating the rows `returning` emits. Distinct from
        /// `rls_write_check` above, which decides whether the write happens at
        /// all: this one bounds what may be shown back, so a `RETURNING` row set
        /// never exceeds a `SELECT` by the same principal. A collection can
        /// carry a read policy and no write policy, in which case the write is
        /// unrestricted and only the visible row set shrinks.
        #[serde(default)]
        rls_filters: Vec<u8>,
    },

    /// Update rows matching filter predicates.
    ///
    /// Uses `MutationEngine` for plain/spatial profiles.
    /// `updates` is a list of (field_name, json_value_bytes) pairs.
    Update {
        collection: QualifiedCollection,
        /// Serialized `Vec<ScanFilter>` (MessagePack).
        filters: Vec<u8>,
        /// Field assignments: `(column_name, json_value_bytes)`.
        updates: Vec<(String, Vec<u8>)>,
        /// Compiled row-level-security WRITE predicate, evaluated against each
        /// row's post-image once the assignments have been applied, or the
        /// reason no predicate is attached.
        rls_write_check: RlsWriteCheck,
    },

    /// Delete rows matching filter predicates.
    ///
    /// Uses `MutationEngine` for plain/spatial profiles.
    Delete {
        collection: QualifiedCollection,
        /// Serialized `Vec<ScanFilter>` (MessagePack).
        filters: Vec<u8>,
        /// Compiled row-level-security WRITE predicate, evaluated against the
        /// pre-image of every row this removes, or the reason no predicate is
        /// attached.
        rls_write_check: RlsWriteCheck,
    },

    /// Update rows the Control Plane already resolved to concrete images.
    ///
    /// Built only for a collection carrying a write policy. The Control
    /// Plane resolved the predicate to a row set and decided the policy
    /// against each row's exact post-image while the writing identity was
    /// live, so this carries the verdict, not a predicate to re-evaluate.
    /// The Data Plane applies exactly these rows and evaluates nothing.
    ResolvedUpdate {
        collection: QualifiedCollection,
        /// (primary key, full post-image) for each row the Control Plane
        /// resolved and the write policy admitted.
        rows: Vec<(nodedb_types::Value, Vec<nodedb_types::Value>)>,
        rls_write_check: RlsWriteCheck,
    },

    /// Delete rows the Control Plane already resolved to concrete keys.
    ///
    /// Built only for a collection carrying a write policy. The Control
    /// Plane resolved the predicate to a row set and decided the policy
    /// against each row's pre-image while the writing identity was live,
    /// so this carries the verdict, not a predicate to re-evaluate. The
    /// Data Plane removes exactly these rows and evaluates nothing.
    ResolvedDelete {
        collection: QualifiedCollection,
        /// Primary key of each row the write policy admitted for removal.
        pks: Vec<nodedb_types::Value>,
        rls_write_check: RlsWriteCheck,
    },

    /// Resolve a predicate UPDATE/DELETE to the concrete rows it would write,
    /// deciding the write policy against those exact images.
    ///
    /// Read-only: it mutates nothing. The Control Plane runs this before
    /// proposing, then proposes the resolved rows instead of the predicate,
    /// because a follower has no writing identity to evaluate a predicate
    /// against.
    ResolveDml {
        collection: QualifiedCollection,
        /// Serialized `Vec<ScanFilter>` — the statement's WHERE clause.
        filters: Vec<u8>,
        /// Field assignments for an UPDATE. Empty for a DELETE.
        updates: Vec<(String, Vec<u8>)>,
        /// True for UPDATE, false for DELETE.
        is_update: bool,
        rls_write_check: RlsWriteCheck,
    },

    /// Cursor-paginated raw scan for the clone materializer.
    ///
    /// Returns `(surrogate, row_value_bytes)` pairs plus next-cursor in one
    /// payload. Honors `system_as_of_ms` so the materializer reads source
    /// as-of the clone's `as_of_lsn` for bitemporal collections.
    MaterializeScan {
        collection: QualifiedCollection,
        cursor: Vec<u8>,
        count: usize,
        system_as_of_ms: Option<i64>,
    },
}
