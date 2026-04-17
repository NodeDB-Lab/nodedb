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
    /// Secondary indexes available on the scan's collection. Document
    /// engines consult this to rewrite equality-on-indexed-field into
    /// [`SqlPlan::DocumentIndexLookup`]. Other engines ignore it today.
    pub indexes: Vec<IndexSpec>,
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

/// Attempt to rewrite `ScanParams` into a [`SqlPlan::DocumentIndexLookup`]
/// when exactly one of the filters is an equality predicate on a `Ready`
/// indexed field. Returns `None` to fall through to a generic `Scan`.
///
/// Shared by the schemaless and strict document engines so the
/// index-rewrite rule has one source of truth. Normalizes strict column
/// names to `$.column` before matching against index fields because the
/// catalog stores every document index in JSON-path canonical form.
pub(crate) fn try_document_index_lookup(
    params: &ScanParams,
    engine: EngineType,
) -> Option<SqlPlan> {
    // Sort / distinct / window functions are not yet supported on the
    // indexed-fetch path — fall back to a full scan so existing behavior
    // stays correct. Extending the handler later is additive and doesn't
    // invalidate the rewrite.
    if !params.sort_keys.is_empty() || params.distinct || !params.window_functions.is_empty() {
        return None;
    }

    // Iterate filters to find the first equality candidate that lines up
    // with a Ready index. Keep the remaining filters as post-filters.
    // Two predicate shapes appear in practice: the resolver-emitted
    // `FilterExpr::Comparison` (compact) and the generic
    // `FilterExpr::Expr(SqlExpr::BinaryOp { Column, Eq, Literal })`
    // wrapper the default path produces. Both unambiguously express
    // equality on a column — handle both.
    for (i, f) in params.filters.iter().enumerate() {
        let Some((field, value)) = extract_equality(&f.expr) else {
            continue;
        };
        let canonical = canonical_index_field(&field);
        let Some(idx) = params
            .indexes
            .iter()
            .find(|i| i.state == IndexState::Ready && i.field == canonical)
        else {
            continue;
        };

        let mut remaining = params.filters.clone();
        remaining.remove(i);

        let lookup_value = if idx.case_insensitive {
            lowercase_string_value(&value)
        } else {
            value
        };

        return Some(SqlPlan::DocumentIndexLookup {
            collection: params.collection.clone(),
            alias: params.alias.clone(),
            engine,
            field: idx.field.clone(),
            value: lookup_value,
            filters: remaining,
            projection: params.projection.clone(),
            sort_keys: params.sort_keys.clone(),
            limit: params.limit,
            offset: params.offset,
            distinct: params.distinct,
            window_functions: params.window_functions.clone(),
            case_insensitive: idx.case_insensitive,
        });
    }
    None
}

/// Pull `(column_name, equality_value)` out of a filter expression if it
/// is a column-equals-literal predicate in either of the planner's two
/// encodings.
fn extract_equality(expr: &FilterExpr) -> Option<(String, SqlValue)> {
    match expr {
        FilterExpr::Comparison {
            field,
            op: CompareOp::Eq,
            value,
        } => Some((field.clone(), value.clone())),
        FilterExpr::Expr(SqlExpr::BinaryOp { left, op, right }) => {
            let (col, lit) = match (left.as_ref(), op, right.as_ref()) {
                (SqlExpr::Column { name, .. }, BinaryOp::Eq, SqlExpr::Literal(v)) => {
                    (name.clone(), v.clone())
                }
                (SqlExpr::Literal(v), BinaryOp::Eq, SqlExpr::Column { name, .. }) => {
                    (name.clone(), v.clone())
                }
                _ => return None,
            };
            Some((col, lit))
        }
        _ => None,
    }
}

fn canonical_index_field(field: &str) -> String {
    if field.starts_with("$.") || field.starts_with('$') {
        field.to_string()
    } else {
        format!("$.{field}")
    }
}

fn lowercase_string_value(v: &SqlValue) -> SqlValue {
    if let SqlValue::String(s) = v {
        SqlValue::String(s.to_lowercase())
    } else {
        v.clone()
    }
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
