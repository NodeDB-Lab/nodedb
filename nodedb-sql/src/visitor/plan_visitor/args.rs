// SPDX-License-Identifier: Apache-2.0

//! Parameter structs for [`super::trait_def::PlanVisitor`] methods whose
//! `SqlPlan` variant carries enough fields to exceed clippy's
//! `too_many_arguments` threshold. Each struct bundles one method's
//! parameters (minus `&mut self`) so the trait method itself stays under
//! the arg-count cap while still exposing every field to implementors.

use crate::temporal::TemporalScope;
use crate::types::SqlPlan;
use crate::types::filter::Filter;
use crate::types::plan::{ArrayPrefilter, MergePlanClause, VectorAnnOptions, WriteRoute};
use crate::types::query::{
    AggregateExpr, EngineType, JoinType, Projection, SortKey, SpatialPredicate, WindowSpec,
};
use crate::types_array::{ArrayAttrAst, ArrayCellOrderAst, ArrayDimAst, ArrayTileOrderAst};
use crate::types_expr::{SqlExpr, SqlPayloadAtom, SqlValue};
use nodedb_types::vector_distance::DistanceMetric;

/// Parameters for [`super::trait_def::PlanVisitor::scan`].
pub struct ScanVisitArgs<'a> {
    pub collection: &'a str,
    pub alias: Option<&'a str>,
    pub engine: EngineType,
    pub filters: &'a [Filter],
    pub projection: &'a [Projection],
    pub sort_keys: &'a [SortKey],
    pub limit: Option<usize>,
    pub offset: usize,
    pub distinct: bool,
    pub window_functions: &'a [WindowSpec],
    pub temporal: &'a TemporalScope,
}

/// Parameters for [`super::trait_def::PlanVisitor::subquery`].
pub struct SubqueryVisitArgs<'a> {
    pub input: &'a SqlPlan,
    pub filters: &'a [Filter],
    pub projection: &'a [Projection],
    pub sort_keys: &'a [SortKey],
    pub offset: usize,
    pub distinct: bool,
    pub limit: Option<usize>,
}

/// Parameters for [`super::trait_def::PlanVisitor::document_index_lookup`].
pub struct DocumentIndexLookupVisitArgs<'a> {
    pub collection: &'a str,
    pub alias: Option<&'a str>,
    pub engine: EngineType,
    pub field: &'a str,
    pub value: &'a SqlValue,
    pub filters: &'a [Filter],
    pub projection: &'a [Projection],
    pub sort_keys: &'a [SortKey],
    pub limit: Option<usize>,
    pub offset: usize,
    pub distinct: bool,
    pub window_functions: &'a [WindowSpec],
    pub case_insensitive: bool,
    pub temporal: &'a TemporalScope,
}

/// Parameters for [`super::trait_def::PlanVisitor::insert`].
pub struct InsertVisitArgs<'a> {
    pub collection: &'a str,
    pub engine: EngineType,
    /// The lowering these rows take, decided by the engine's `EngineRules`.
    pub route: WriteRoute,
    pub rows: &'a [Vec<(String, SqlValue)>],
    pub column_defaults: &'a [(String, String)],
    pub if_absent: bool,
    pub column_schema: &'a [(String, String)],
    pub primary_key: Option<&'a str>,
}

/// Parameters for [`super::trait_def::PlanVisitor::upsert`].
pub struct UpsertVisitArgs<'a> {
    pub collection: &'a str,
    pub engine: EngineType,
    /// The lowering these rows take, decided by the engine's `EngineRules`.
    pub route: WriteRoute,
    pub rows: &'a [Vec<(String, SqlValue)>],
    pub column_defaults: &'a [(String, String)],
    pub on_conflict_updates: &'a [(String, SqlExpr)],
    pub column_schema: &'a [(String, String)],
    pub primary_key: Option<&'a str>,
}

/// Parameters for [`super::trait_def::PlanVisitor::update_from`].
pub struct UpdateFromVisitArgs<'a> {
    pub collection: &'a str,
    pub engine: EngineType,
    pub source: &'a SqlPlan,
    pub target_join_col: &'a str,
    pub source_join_col: &'a str,
    pub assignments: &'a [(String, SqlExpr)],
    pub target_filters: &'a [Filter],
    pub returning: bool,
}

/// Parameters for [`super::trait_def::PlanVisitor::join`].
pub struct JoinVisitArgs<'a> {
    pub left: &'a SqlPlan,
    pub right: &'a SqlPlan,
    pub on: &'a [(String, String)],
    pub join_type: JoinType,
    pub condition: Option<&'a SqlExpr>,
    pub limit: Option<usize>,
    pub projection: &'a [Projection],
    pub filters: &'a [Filter],
}

/// Parameters for [`super::trait_def::PlanVisitor::aggregate`].
pub struct AggregateVisitArgs<'a> {
    pub input: &'a SqlPlan,
    pub group_by: &'a [SqlExpr],
    pub aggregates: &'a [AggregateExpr],
    pub having: &'a [Filter],
    pub limit: usize,
    pub grouping_sets: Option<&'a [Vec<usize>]>,
    pub sort_keys: &'a [SortKey],
}

/// Parameters for [`super::trait_def::PlanVisitor::timeseries_scan`].
pub struct TimeseriesScanVisitArgs<'a> {
    pub collection: &'a str,
    pub time_range: (i64, i64),
    pub bucket_interval_ms: i64,
    pub group_by: &'a [String],
    pub aggregates: &'a [AggregateExpr],
    pub filters: &'a [Filter],
    pub projection: &'a [Projection],
    pub gap_fill: &'a str,
    pub limit: usize,
    pub sort_keys: &'a [SortKey],
    pub tiered: bool,
    pub temporal: &'a TemporalScope,
}

/// Parameters for [`super::trait_def::PlanVisitor::vector_search`].
pub struct VectorSearchVisitArgs<'a> {
    pub collection: &'a str,
    pub field: &'a str,
    pub query_vector: &'a [f32],
    pub top_k: usize,
    pub ef_search: usize,
    pub metric: DistanceMetric,
    pub filters: &'a [Filter],
    pub array_prefilter: Option<&'a ArrayPrefilter>,
    pub ann_options: &'a VectorAnnOptions,
    pub skip_payload_fetch: bool,
    pub payload_filters: &'a [SqlPayloadAtom],
}

/// Parameters for [`super::trait_def::PlanVisitor::hybrid_search`].
pub struct HybridSearchVisitArgs<'a> {
    pub collection: &'a str,
    pub query_vector: &'a [f32],
    pub query_text: &'a str,
    pub top_k: usize,
    pub ef_search: usize,
    pub vector_weight: f32,
    pub fuzzy: bool,
    pub score_alias: Option<&'a str>,
}

/// Parameters for [`super::trait_def::PlanVisitor::hybrid_search_triple`].
pub struct HybridSearchTripleVisitArgs<'a> {
    pub collection: &'a str,
    pub query_vector: &'a [f32],
    pub query_text: &'a str,
    pub graph_seed_id: &'a str,
    pub graph_depth: usize,
    pub graph_edge_label: Option<&'a str>,
    pub top_k: usize,
    pub ef_search: usize,
    pub fuzzy: bool,
    pub rrf_k: (f64, f64, f64),
    pub score_alias: Option<&'a str>,
}

/// Parameters for [`super::trait_def::PlanVisitor::spatial_scan`].
pub struct SpatialScanVisitArgs<'a> {
    pub collection: &'a str,
    pub field: &'a str,
    pub predicate: &'a SpatialPredicate,
    pub query_geometry: &'a nodedb_types::geometry::Geometry,
    pub distance_meters: f64,
    pub attribute_filters: &'a [Filter],
    pub limit: usize,
    pub projection: &'a [Projection],
}

/// Parameters for [`super::trait_def::PlanVisitor::recursive_scan`].
pub struct RecursiveScanVisitArgs<'a> {
    pub collection: &'a str,
    pub base_filters: &'a [Filter],
    pub recursive_filters: &'a [Filter],
    pub join_link: Option<&'a (String, String)>,
    pub max_iterations: usize,
    pub distinct: bool,
    pub limit: usize,
}

/// Parameters for [`super::trait_def::PlanVisitor::recursive_value`].
pub struct RecursiveValueVisitArgs<'a> {
    pub cte_name: &'a str,
    pub columns: &'a [String],
    pub init_exprs: &'a [String],
    pub step_exprs: &'a [String],
    pub condition: Option<&'a str>,
    pub max_depth: usize,
    pub distinct: bool,
}

/// Parameters for [`super::trait_def::PlanVisitor::create_array`].
pub struct CreateArrayVisitArgs<'a> {
    pub name: &'a str,
    pub dims: &'a [ArrayDimAst],
    pub attrs: &'a [ArrayAttrAst],
    pub tile_extents: &'a [i64],
    pub cell_order: ArrayCellOrderAst,
    pub tile_order: ArrayTileOrderAst,
    pub prefix_bits: u8,
    pub audit_retain_ms: Option<u64>,
    pub minimum_audit_retain_ms: Option<u64>,
}

/// Parameters for [`super::trait_def::PlanVisitor::merge`].
pub struct MergeVisitArgs<'a> {
    pub target: &'a str,
    pub engine: EngineType,
    pub source: &'a SqlPlan,
    pub target_join_col: &'a str,
    pub source_join_col: &'a str,
    pub source_alias: &'a str,
    pub clauses: &'a [MergePlanClause],
    pub returning: bool,
}

/// Parameters for [`super::trait_def::PlanVisitor::lateral_top_k`].
pub struct LateralTopKVisitArgs<'a> {
    pub outer: &'a SqlPlan,
    pub outer_alias: Option<&'a str>,
    pub inner_collection: &'a str,
    pub inner_filters: &'a [Filter],
    pub inner_order_by: &'a [SortKey],
    pub inner_limit: usize,
    pub correlation_keys: &'a [(String, String)],
    pub lateral_alias: &'a str,
    pub projection: &'a [Projection],
    pub left_join: bool,
}

/// Parameters for [`super::trait_def::PlanVisitor::lateral_loop`].
pub struct LateralLoopVisitArgs<'a> {
    pub outer: &'a SqlPlan,
    pub outer_alias: Option<&'a str>,
    pub inner: &'a SqlPlan,
    pub correlation_predicates: &'a [(String, String)],
    pub lateral_alias: &'a str,
    pub projection: &'a [Projection],
    pub outer_row_cap: usize,
    pub left_join: bool,
}
