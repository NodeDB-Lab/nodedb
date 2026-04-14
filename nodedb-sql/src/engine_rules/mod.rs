pub mod columnar;
pub mod document_schemaless;
pub mod document_strict;
pub mod kv;
pub mod spatial;
pub mod timeseries;

use crate::error::Result;
use crate::types::*;

/// Parameters for planning an INSERT operation.
pub struct InsertParams {
    pub collection: String,
    pub columns: Vec<String>,
    pub rows: Vec<Vec<(String, SqlValue)>>,
    pub column_defaults: Vec<(String, String)>,
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
}

/// Parameters for planning a POINT GET operation.
pub struct PointGetParams {
    pub collection: String,
    pub alias: Option<String>,
    pub key_column: String,
    pub key_value: SqlValue,
}

/// Parameters for planning an UPDATE operation.
pub struct UpdateParams {
    pub collection: String,
    pub assignments: Vec<(String, SqlExpr)>,
    pub filters: Vec<Filter>,
    pub target_keys: Vec<SqlValue>,
    pub returning: bool,
}

/// Parameters for planning a DELETE operation.
pub struct DeleteParams {
    pub collection: String,
    pub filters: Vec<Filter>,
    pub target_keys: Vec<SqlValue>,
}

/// Parameters for planning an UPSERT operation.
pub struct UpsertParams {
    pub collection: String,
    pub columns: Vec<String>,
    pub rows: Vec<Vec<(String, SqlValue)>>,
    pub column_defaults: Vec<(String, String)>,
    /// `ON CONFLICT (...) DO UPDATE SET` assignments. Empty for plain
    /// `UPSERT INTO ...`; populated when the caller is
    /// `INSERT ... ON CONFLICT ... DO UPDATE SET`.
    pub on_conflict_updates: Vec<(String, SqlExpr)>,
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
}

/// Engine-specific planning rules.
///
/// Each engine type implements this trait to produce the correct `SqlPlan`
/// variant for each operation, or return an error if the operation is not
/// supported. This is the single source of truth for operation routing —
/// no downstream code should ever check engine type to decide routing.
pub trait EngineRules {
    /// Plan an INSERT. Returns `Err` if the engine does not support inserts
    /// (e.g. timeseries routes to `TimeseriesIngest` instead).
    fn plan_insert(&self, params: InsertParams) -> Result<Vec<SqlPlan>>;
    /// Plan an UPSERT (insert-or-merge). Returns `Err` for append-only or
    /// columnar engines that don't support merge semantics.
    fn plan_upsert(&self, params: UpsertParams) -> Result<Vec<SqlPlan>>;
    /// Plan a table scan (SELECT without point-get optimization).
    fn plan_scan(&self, params: ScanParams) -> Result<SqlPlan>;
    /// Plan a point lookup by primary key. Returns `Err` for engines
    /// that don't support O(1) key lookups (e.g. timeseries).
    fn plan_point_get(&self, params: PointGetParams) -> Result<SqlPlan>;
    /// Plan an UPDATE. Returns `Err` for append-only engines.
    fn plan_update(&self, params: UpdateParams) -> Result<Vec<SqlPlan>>;
    /// Plan a DELETE (point or bulk).
    fn plan_delete(&self, params: DeleteParams) -> Result<Vec<SqlPlan>>;
    /// Plan a GROUP BY / aggregate query.
    fn plan_aggregate(&self, params: AggregateParams) -> Result<SqlPlan>;
}

/// Resolve the engine rules for a given engine type.
///
/// No catch-all — compiler enforces exhaustiveness.
pub fn resolve_engine_rules(engine: EngineType) -> &'static dyn EngineRules {
    match engine {
        EngineType::DocumentSchemaless => &document_schemaless::SchemalessRules,
        EngineType::DocumentStrict => &document_strict::StrictRules,
        EngineType::KeyValue => &kv::KvRules,
        EngineType::Columnar => &columnar::ColumnarRules,
        EngineType::Timeseries => &timeseries::TimeseriesRules,
        EngineType::Spatial => &spatial::SpatialRules,
    }
}
