//! Parameter structs for scan/search plan conversion functions.

use nodedb_sql::types::{AggregateExpr, EngineType, Filter, Projection, SortKey, SqlPlan};

use crate::types::TenantId;

use super::convert::ConvertContext;

/// Parameters for `convert_scan`.
pub(super) struct ScanParams<'a> {
    pub collection: &'a str,
    pub engine: &'a EngineType,
    pub filters: &'a [Filter],
    pub projection: &'a [Projection],
    pub sort_keys: &'a [SortKey],
    pub limit: &'a Option<usize>,
    pub offset: &'a usize,
    pub distinct: &'a bool,
    pub window_functions: &'a [nodedb_sql::types::WindowSpec],
    pub tenant_id: TenantId,
    pub temporal: &'a nodedb_sql::TemporalScope,
}

/// Parameters for `convert_join`.
pub(super) struct JoinPlanParams<'a> {
    pub left: &'a SqlPlan,
    pub right: &'a SqlPlan,
    pub on: &'a [(String, String)],
    pub join_type: &'a nodedb_sql::types::JoinType,
    pub condition: &'a Option<nodedb_sql::types::SqlExpr>,
    pub limit: &'a usize,
    pub projection: &'a [Projection],
    pub filters: &'a [Filter],
    pub tenant_id: TenantId,
    pub ctx: &'a ConvertContext,
}

/// Parameters for `convert_recursive_scan`.
pub(super) struct RecursiveScanParams<'a> {
    pub collection: &'a str,
    pub base_filters: &'a [Filter],
    pub recursive_filters: &'a [Filter],
    pub join_link: &'a Option<(String, String)>,
    pub max_iterations: &'a usize,
    pub distinct: &'a bool,
    pub limit: &'a usize,
    pub tenant_id: TenantId,
}

/// Parameters for `convert_timeseries_scan`.
pub(super) struct TimeseriesScanParams<'a> {
    pub collection: &'a str,
    pub time_range: &'a (i64, i64),
    pub bucket_interval_ms: &'a i64,
    pub group_by: &'a [String],
    pub aggregates: &'a [AggregateExpr],
    pub filters: &'a [Filter],
    pub projection: &'a [Projection],
    pub gap_fill: &'a str,
    pub limit: &'a usize,
    pub tiered: &'a bool,
    pub tenant_id: TenantId,
    pub ctx: &'a ConvertContext,
    pub temporal: &'a nodedb_sql::TemporalScope,
}

/// Parameters for `convert_hybrid_search`.
pub(super) struct HybridSearchParams<'a> {
    pub collection: &'a str,
    pub query_vector: &'a [f32],
    pub query_text: &'a str,
    pub top_k: &'a usize,
    pub ef_search: &'a usize,
    pub vector_weight: &'a f32,
    pub fuzzy: &'a bool,
    pub tenant_id: TenantId,
}

/// Parameters for `convert_spatial_scan`.
pub(super) struct SpatialScanParams<'a> {
    pub collection: &'a str,
    pub field: &'a str,
    pub predicate: &'a nodedb_sql::types::SpatialPredicate,
    pub query_geometry: &'a [u8],
    pub distance_meters: &'a f64,
    pub attribute_filters: &'a [Filter],
    pub limit: &'a usize,
    pub projection: &'a [Projection],
    pub tenant_id: TenantId,
}
