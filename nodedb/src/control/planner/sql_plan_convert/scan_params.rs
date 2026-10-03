// SPDX-License-Identifier: BUSL-1.1

//! Parameter structs for scan/search plan conversion functions.

use nodedb_sql::types::{
    AggregateExpr, DistanceMetric, EngineType, Filter, Projection, SortKey, SqlPlan,
};

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
    pub database_id: crate::types::DatabaseId,
}

/// Parameters for `convert_join`.
pub(super) struct JoinPlanParams<'a> {
    pub left: &'a SqlPlan,
    pub right: &'a SqlPlan,
    pub on: &'a [(String, String)],
    pub join_type: &'a nodedb_sql::types::JoinType,
    pub condition: &'a Option<nodedb_sql::types::SqlExpr>,
    /// `None` = no SQL `LIMIT` (bounded by the byte budget in the handler);
    /// `Some(n)` = explicit `LIMIT n`.
    pub limit: &'a Option<usize>,
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
    pub database_id: crate::types::DatabaseId,
}

/// Parameters for `convert_recursive_value`.
pub(super) struct RecursiveValueParams<'a> {
    pub cte_name: &'a str,
    pub columns: &'a [String],
    pub init_exprs: &'a [String],
    pub step_exprs: &'a [String],
    pub condition: &'a Option<String>,
    pub max_depth: &'a usize,
    pub distinct: &'a bool,
    pub tenant_id: TenantId,
    pub database_id: crate::types::DatabaseId,
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
    pub sort_keys: &'a [nodedb_sql::types::SortKey],
    pub tiered: &'a bool,
    pub tenant_id: TenantId,
    pub ctx: &'a ConvertContext,
    pub temporal: &'a nodedb_sql::TemporalScope,
}

/// Parameters for `convert_vector_search`.
pub(super) struct VectorSearchParams<'a> {
    pub collection: &'a str,
    pub field: &'a str,
    pub query_vector: &'a [f32],
    pub top_k: &'a usize,
    pub ef_search: &'a usize,
    /// Per-query distance metric override (from `<->`, `<=>`, or `<#>`).
    pub metric: &'a DistanceMetric,
    pub filters: &'a [Filter],
    pub array_prefilter: Option<&'a nodedb_sql::types::ArrayPrefilter>,
    pub ann_options: &'a nodedb_sql::types::VectorAnnOptions,
    pub tenant_id: TenantId,
    pub ctx: &'a ConvertContext,
    /// Propagated from `SqlPlan::VectorSearch::skip_payload_fetch`.
    pub skip_payload_fetch: bool,
    /// Predicate atoms (Eq / In / Range) against payload-indexed columns.
    /// Translated to `nodedb_types::PayloadAtom` and emitted as
    /// `VectorOp::Search::payload_filters`.
    pub payload_filters: &'a [nodedb_sql::types::SqlPayloadAtom],
    /// Primary keys the search ranks among, lowered to
    /// `VectorOp::Search::filter_bitmap`. `None`: every row.
    pub pk_prefilter: Option<&'a [nodedb_sql::types::SqlValue]>,
}

/// Parameters for `convert_sparse_search`.
pub(super) struct SparseSearchParams<'a> {
    pub collection: &'a str,
    pub field: &'a str,
    /// Query sparse vector as `(dimension, weight)` entries, parsed at plan time.
    pub query_entries: &'a [(u32, f32)],
    pub top_k: &'a usize,
    pub tenant_id: TenantId,
    pub database_id: crate::types::DatabaseId,
}

/// Parameters for `convert_text_search`.
pub(super) struct TextSearchConvertParams<'a> {
    pub collection: &'a str,
    pub shape: &'a nodedb_sql::types::TextSearchShape,
    /// Residual WHERE predicates, applied before ranking.
    pub filters: &'a [Filter],
    pub scores: &'a [nodedb_sql::types::TextScoreColumn],
    pub tenant_id: TenantId,
    pub database_id: crate::types::DatabaseId,
}

/// Parameters for `convert_hybrid_search`.
pub(super) struct HybridSearchParams<'a> {
    pub collection: &'a str,
    pub vector_field: &'a str,
    pub query_vector: &'a [f32],
    /// `None` reads the whole-document index.
    pub text_field: Option<&'a str>,
    pub query_text: &'a str,
    /// Residual WHERE predicates, applied to both legs before fusion.
    pub filters: &'a [Filter],
    pub top_k: &'a usize,
    pub ef_search: &'a usize,
    pub vector_weight: &'a f32,
    pub mode: nodedb_types::text_search::QueryMode,
    pub fuzzy: &'a bool,
    /// SELECT-list alias for the RRF score column. Forwarded to
    /// `TextOp::HybridSearch.score_alias` so the executor renames the
    /// response field.
    pub score_alias: Option<&'a str>,
    pub tenant_id: TenantId,
    pub database_id: crate::types::DatabaseId,
}

/// Parameters for `convert_hybrid_search_triple`.
pub(super) struct HybridSearchTripleParams<'a> {
    pub collection: &'a str,
    pub vector_field: &'a str,
    pub query_vector: &'a [f32],
    /// `None` reads the whole-document index.
    pub text_field: Option<&'a str>,
    pub query_text: &'a str,
    /// Residual WHERE predicates, applied to every leg before fusion.
    pub filters: &'a [Filter],
    pub graph_seed_id: &'a str,
    pub graph_depth: &'a usize,
    pub graph_edge_label: &'a Option<String>,
    pub top_k: &'a usize,
    pub ef_search: &'a usize,
    pub mode: nodedb_types::text_search::QueryMode,
    pub fuzzy: &'a bool,
    pub rrf_k: &'a (f64, f64, f64),
    pub score_alias: Option<&'a str>,
    pub tenant_id: TenantId,
    pub database_id: crate::types::DatabaseId,
}

/// Parameters for `convert_spatial_scan`.
pub(super) struct SpatialScanParams<'a> {
    pub collection: &'a str,
    pub field: &'a str,
    pub predicate: &'a nodedb_sql::types::SpatialPredicate,
    pub query_geometry: &'a nodedb_types::geometry::Geometry,
    pub distance_meters: &'a f64,
    pub attribute_filters: &'a [Filter],
    pub limit: &'a usize,
    pub projection: &'a [Projection],
    pub tenant_id: TenantId,
    pub database_id: crate::types::DatabaseId,
}
