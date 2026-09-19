// SPDX-License-Identifier: Apache-2.0

//! Parameter structs passed into `EngineRules` methods.

use crate::types::*;

/// Parameters for planning an INSERT operation.
pub struct InsertParams {
    pub collection: String,
    pub columns: Vec<String>,
    /// Every declared DEFAULT already materialized and every literal already
    /// coerced to its declared column type. An engine stores these as they
    /// are; none re-reads the catalog's DEFAULT text.
    pub rows: Vec<Vec<(String, SqlValue)>>,
    /// Whether a DEFAULT materialized into `rows` was volatile. A plan that
    /// carries one is never admitted to the plan cache.
    pub volatile_defaults: bool,
    /// `ON CONFLICT DO NOTHING` semantics: duplicate-PK rows are skipped
    /// silently. `false` for plain `INSERT` (raises `unique_violation`).
    pub if_absent: bool,
    /// Raw column type strings from the catalog: `(column_name, type_str)`.
    /// Used by columnar converters to reconstruct the exact `ColumnType` for
    /// columns whose `SqlDataType` is ambiguous (e.g. both JSON and Bytes map
    /// to `SqlDataType::Bytes`). Empty for engines that don't need it.
    pub column_schema: Vec<(String, String)>,
    /// Declared `PRIMARY KEY` column name (if any), from `CollectionInfo::primary_key`.
    /// Used by the conversion layer to extract the document id from the
    /// correct column instead of guessing at `id`/`document_id`/`key`.
    pub primary_key: Option<String>,
}

/// Parameters for planning a SCAN operation.
pub struct ScanParams {
    pub collection: String,
    pub alias: Option<String>,
    pub filters: Vec<Filter>,
    pub projection: Vec<Projection>,
    pub sort_keys: Vec<SortKey>,
    pub limit: Option<usize>,
    pub offset: usize,
    pub distinct: bool,
    pub window_functions: Vec<WindowSpec>,
    /// Secondary indexes available on the scan's collection. Document
    /// engines consult this to rewrite equality-on-indexed-field into
    /// [`SqlPlan::DocumentIndexLookup`]. Other engines ignore it today.
    pub indexes: Vec<IndexSpec>,
    /// Bitemporal qualifier propagated from `plan_sql`. Engines without
    /// bitemporal storage support reject a non-default scope via
    /// `SqlError::Unsupported` — silently ignoring it would return
    /// current-state data when the user asked for history.
    pub temporal: crate::temporal::TemporalScope,
    /// Whether this collection was created with bitemporal storage. When
    /// `true`, engines that support bitemporal reads route the scan
    /// through versioned storage; when `false`, a non-default
    /// [`Self::temporal`] is rejected.
    pub bitemporal: bool,
}

/// Parameters for planning a POINT GET operation.
pub struct PointGetParams {
    pub collection: String,
    pub alias: Option<String>,
    pub key_column: String,
    pub key_value: SqlValue,
    /// Resolved SELECT target list, propagated onto [`SqlPlan::PointGet`] so the
    /// physical plan self-describes its output columns.
    pub projection: Vec<Projection>,
}

/// Parameters for planning an UPDATE operation.
pub struct UpdateParams {
    pub collection: String,
    pub assignments: Vec<(String, SqlExpr)>,
    pub filters: Vec<Filter>,
    pub target_keys: Vec<SqlValue>,
    pub returning: bool,
}

/// Parameters for planning an `UPDATE target SET ... FROM src WHERE ...` operation.
pub struct UpdateFromParams {
    pub collection: String,
    /// The FROM source plan (Scan, Join, …).
    pub source: Box<SqlPlan>,
    /// Column in target used as the equi-join key.
    pub target_join_col: String,
    /// Column in source used as the equi-join key.
    pub source_join_col: String,
    /// SET assignments — RHS may reference source columns.
    pub assignments: Vec<(String, SqlExpr)>,
    /// Filters that apply only to the target.
    pub target_filters: Vec<Filter>,
    pub returning: bool,
}

/// Parameters for planning a DELETE operation.
pub struct DeleteParams {
    pub collection: String,
    pub filters: Vec<Filter>,
    pub target_keys: Vec<SqlValue>,
}

/// Parameters for planning a TRUNCATE operation.
pub struct TruncateParams {
    pub collection: String,
    pub restart_identity: bool,
}

/// Parameters for planning a MERGE operation.
pub struct MergeParams {
    pub collection: String,
    pub source: Box<SqlPlan>,
    pub target_join_col: String,
    pub source_join_col: String,
    pub source_alias: String,
    pub clauses: Vec<crate::types::MergePlanClause>,
    pub returning: bool,
}

/// Parameters for planning an UPSERT operation.
pub struct UpsertParams {
    pub collection: String,
    pub columns: Vec<String>,
    /// Defaults materialized and literals coerced, as in
    /// `InsertParams::rows`.
    pub rows: Vec<Vec<(String, SqlValue)>>,
    /// Mirrors `InsertParams::volatile_defaults`.
    pub volatile_defaults: bool,
    /// `ON CONFLICT (...) DO UPDATE SET` assignments. Empty for plain
    /// `UPSERT INTO ...`; populated when the caller is
    /// `INSERT ... ON CONFLICT ... DO UPDATE SET`.
    pub on_conflict_updates: Vec<(String, SqlExpr)>,
    /// Raw column type strings from the catalog: `(column_name, type_str)`.
    /// Mirrors `InsertParams::column_schema` — see that field for rationale.
    pub column_schema: Vec<(String, String)>,
    /// Declared `PRIMARY KEY` column name (if any). See `InsertParams::primary_key`.
    pub primary_key: Option<String>,
}

/// Parameters for planning an AGGREGATE operation.
pub struct AggregateParams {
    pub collection: String,
    pub alias: Option<String>,
    pub filters: Vec<Filter>,
    pub group_by: Vec<SqlExpr>,
    pub aggregates: Vec<AggregateExpr>,
    pub having: Vec<Filter>,
    pub limit: usize,
    /// Timeseries-specific: bucket interval from time_bucket() call.
    pub bucket_interval_ms: Option<i64>,
    /// Timeseries-specific: non-time GROUP BY columns.
    pub group_columns: Vec<String>,
    /// Whether the collection has auto-tiering enabled.
    pub has_auto_tier: bool,
    /// Whether this collection was created with bitemporal storage.
    /// When `true`, the base scan inside the aggregate is allowed to
    /// carry a non-default temporal scope.
    pub bitemporal: bool,
    /// System-time / valid-time scope to propagate into the underlying
    /// scan so bitemporal aggregate queries project an as-of snapshot
    /// before grouping.
    pub temporal: crate::temporal::TemporalScope,
}
