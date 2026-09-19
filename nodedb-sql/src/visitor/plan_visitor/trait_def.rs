// SPDX-License-Identifier: Apache-2.0

//! Executor parity contract for [`SqlPlan`]: one abstract method per variant.
//! Trait method arity mirrors `SqlPlan` variant field counts and is not a code smell.
//! Variants whose field count would exceed clippy's `too_many_arguments` cap take a
//! bundled params struct from [`super::args`] instead of raw positional arguments.

use super::args::{
    AggregateVisitArgs, CreateArrayVisitArgs, DocumentIndexLookupVisitArgs,
    HybridSearchTripleVisitArgs, HybridSearchVisitArgs, InsertVisitArgs, JoinVisitArgs,
    LateralLoopVisitArgs, LateralTopKVisitArgs, MergeVisitArgs, RecursiveScanVisitArgs,
    RecursiveValueVisitArgs, ScanVisitArgs, SpatialScanVisitArgs, SubqueryVisitArgs,
    TimeseriesScanVisitArgs, UpdateFromVisitArgs, UpsertVisitArgs, VectorPrimaryDeleteVisitArgs,
    VectorPrimaryInsertVisitArgs, VectorPrimaryUpdateVisitArgs, VectorSearchVisitArgs,
};
use crate::fts_types::FtsQuery;
use crate::temporal::TemporalScope;
use crate::types::SqlPlan;
use crate::types::filter::Filter;
use crate::types::plan::KvInsertIntent;
use crate::types::query::EngineType;
use crate::types_array::{
    ArrayBinaryOpAst, ArrayCoordLiteral, ArrayInsertRow, ArrayReducerAst, ArraySliceAst,
};
use crate::types_expr::{SqlExpr, SqlValue};

/// Executor parity contract: every [`SqlPlan`] variant must be handled.
/// Implement this trait and call [`dispatch`](super::dispatch) to route plans.
pub trait PlanVisitor {
    /// The successful result type returned by each visit method.
    type Output;
    /// The error type returned by each visit method.
    type Error;

    /// Handle [`SqlPlan::ConstantResult`].
    fn constant_result(
        &mut self,
        columns: &[String],
        values: &[SqlValue],
    ) -> Result<Self::Output, Self::Error>;

    /// Handle [`SqlPlan::Scan`].
    fn scan(&mut self, args: ScanVisitArgs<'_>) -> Result<Self::Output, Self::Error>;

    /// Handle [`SqlPlan::PointGet`].
    fn point_get(
        &mut self,
        collection: &str,
        alias: Option<&str>,
        engine: EngineType,
        key_column: &str,
        key_value: &SqlValue,
    ) -> Result<Self::Output, Self::Error>;

    /// Handle [`SqlPlan::DocumentIndexLookup`].
    fn document_index_lookup(
        &mut self,
        args: DocumentIndexLookupVisitArgs<'_>,
    ) -> Result<Self::Output, Self::Error>;

    /// Handle [`SqlPlan::RangeScan`].
    fn range_scan(
        &mut self,
        collection: &str,
        field: &str,
        lower: Option<&SqlValue>,
        upper: Option<&SqlValue>,
        limit: usize,
    ) -> Result<Self::Output, Self::Error>;

    /// Handle [`SqlPlan::Insert`].
    fn insert(&mut self, args: InsertVisitArgs<'_>) -> Result<Self::Output, Self::Error>;

    /// Handle [`SqlPlan::KvInsert`].
    fn kv_insert(
        &mut self,
        collection: &str,
        entries: &[(SqlValue, Vec<(String, SqlValue)>)],
        ttl_secs: u64,
        intent: KvInsertIntent,
        on_conflict_updates: &[(String, SqlExpr)],
    ) -> Result<Self::Output, Self::Error>;

    /// Handle [`SqlPlan::Upsert`].
    fn upsert(&mut self, args: UpsertVisitArgs<'_>) -> Result<Self::Output, Self::Error>;

    /// Handle [`SqlPlan::InsertSelect`].
    fn insert_select(
        &mut self,
        target: &str,
        source: &SqlPlan,
        limit: usize,
        column_map: &[(String, SqlExpr)],
    ) -> Result<Self::Output, Self::Error>;

    /// Handle [`SqlPlan::Update`].
    fn update(
        &mut self,
        collection: &str,
        engine: EngineType,
        assignments: &[(String, SqlExpr)],
        filters: &[Filter],
        target_keys: &[SqlValue],
        returning: bool,
    ) -> Result<Self::Output, Self::Error>;

    /// Handle [`SqlPlan::UpdateFrom`].
    fn update_from(&mut self, args: UpdateFromVisitArgs<'_>) -> Result<Self::Output, Self::Error>;

    /// Handle [`SqlPlan::Delete`].
    fn delete(
        &mut self,
        collection: &str,
        engine: EngineType,
        filters: &[Filter],
        target_keys: &[SqlValue],
    ) -> Result<Self::Output, Self::Error>;

    /// Handle [`SqlPlan::Truncate`].
    fn truncate(
        &mut self,
        collection: &str,
        restart_identity: bool,
    ) -> Result<Self::Output, Self::Error>;

    /// Handle [`SqlPlan::Join`].
    ///
    /// `limit` is `None` when the join carries no SQL `LIMIT` clause (output
    /// bounded downstream by the memory byte budget) and `Some(n)` for an
    /// explicit `LIMIT n`.
    fn join(&mut self, args: JoinVisitArgs<'_>) -> Result<Self::Output, Self::Error>;

    /// Handle [`SqlPlan::Aggregate`].
    fn aggregate(&mut self, args: AggregateVisitArgs<'_>) -> Result<Self::Output, Self::Error>;

    /// Handle [`SqlPlan::TimeseriesScan`].
    fn timeseries_scan(
        &mut self,
        args: TimeseriesScanVisitArgs<'_>,
    ) -> Result<Self::Output, Self::Error>;

    /// Handle [`SqlPlan::TimeseriesIngest`].
    fn timeseries_ingest(
        &mut self,
        collection: &str,
        rows: &[Vec<(String, SqlValue)>],
    ) -> Result<Self::Output, Self::Error>;

    /// Handle [`SqlPlan::VectorSearch`].
    fn vector_search(
        &mut self,
        args: VectorSearchVisitArgs<'_>,
    ) -> Result<Self::Output, Self::Error>;

    /// Handle [`SqlPlan::MultiVectorSearch`].
    fn multi_vector_search(
        &mut self,
        collection: &str,
        query_vector: &[f32],
        top_k: usize,
        ef_search: usize,
    ) -> Result<Self::Output, Self::Error>;

    /// Handle [`SqlPlan::SparseSearch`].
    fn sparse_search(
        &mut self,
        collection: &str,
        field: &str,
        query_entries: &[(u32, f32)],
        top_k: usize,
    ) -> Result<Self::Output, Self::Error>;

    /// Handle [`SqlPlan::TextSearch`].
    fn text_search(
        &mut self,
        collection: &str,
        query: &FtsQuery,
        top_k: usize,
        filters: &[Filter],
        score_alias: Option<&str>,
    ) -> Result<Self::Output, Self::Error>;

    /// Handle [`SqlPlan::HybridSearch`].
    fn hybrid_search(
        &mut self,
        args: HybridSearchVisitArgs<'_>,
    ) -> Result<Self::Output, Self::Error>;

    /// Handle [`SqlPlan::HybridSearchTriple`].
    fn hybrid_search_triple(
        &mut self,
        args: HybridSearchTripleVisitArgs<'_>,
    ) -> Result<Self::Output, Self::Error>;

    /// Handle [`SqlPlan::SpatialScan`].
    fn spatial_scan(&mut self, args: SpatialScanVisitArgs<'_>)
    -> Result<Self::Output, Self::Error>;

    /// Handle [`SqlPlan::Union`].
    fn union(&mut self, inputs: &[SqlPlan], distinct: bool) -> Result<Self::Output, Self::Error>;

    /// Handle [`SqlPlan::Intersect`].
    fn intersect(
        &mut self,
        left: &SqlPlan,
        right: &SqlPlan,
        all: bool,
    ) -> Result<Self::Output, Self::Error>;

    /// Handle [`SqlPlan::Except`].
    fn except(
        &mut self,
        left: &SqlPlan,
        right: &SqlPlan,
        all: bool,
    ) -> Result<Self::Output, Self::Error>;

    /// Handle [`SqlPlan::RecursiveScan`].
    fn recursive_scan(
        &mut self,
        args: RecursiveScanVisitArgs<'_>,
    ) -> Result<Self::Output, Self::Error>;

    /// Handle [`SqlPlan::RecursiveValue`].
    fn recursive_value(
        &mut self,
        args: RecursiveValueVisitArgs<'_>,
    ) -> Result<Self::Output, Self::Error>;

    /// Handle [`SqlPlan::Cte`].
    fn cte(
        &mut self,
        definitions: &[(String, SqlPlan)],
        outer: &SqlPlan,
    ) -> Result<Self::Output, Self::Error>;

    /// Handle [`SqlPlan::Subquery`].
    fn subquery(&mut self, args: SubqueryVisitArgs<'_>) -> Result<Self::Output, Self::Error>;

    /// Handle [`SqlPlan::CreateArray`].
    fn create_array(&mut self, args: CreateArrayVisitArgs<'_>)
    -> Result<Self::Output, Self::Error>;

    /// Handle [`SqlPlan::DropArray`].
    fn drop_array(&mut self, name: &str, if_exists: bool) -> Result<Self::Output, Self::Error>;

    /// Handle [`SqlPlan::AlterArray`].
    fn alter_array(
        &mut self,
        name: &str,
        audit_retain_ms: Option<Option<i64>>,
        minimum_audit_retain_ms: Option<u64>,
    ) -> Result<Self::Output, Self::Error>;

    /// Handle [`SqlPlan::InsertArray`].
    fn insert_array(
        &mut self,
        name: &str,
        rows: &[ArrayInsertRow],
    ) -> Result<Self::Output, Self::Error>;

    /// Handle [`SqlPlan::DeleteArray`].
    fn delete_array(
        &mut self,
        name: &str,
        coords: &[Vec<ArrayCoordLiteral>],
    ) -> Result<Self::Output, Self::Error>;

    /// Handle [`SqlPlan::ArraySlice`].
    fn array_slice(
        &mut self,
        name: &str,
        slice: &ArraySliceAst,
        attr_projection: &[String],
        limit: u32,
        temporal: &TemporalScope,
    ) -> Result<Self::Output, Self::Error>;

    /// Handle [`SqlPlan::ArrayProject`].
    fn array_project(
        &mut self,
        name: &str,
        attr_projection: &[String],
    ) -> Result<Self::Output, Self::Error>;

    /// Handle [`SqlPlan::ArrayAgg`].
    fn array_agg(
        &mut self,
        name: &str,
        attr: &str,
        reducer: &ArrayReducerAst,
        group_by_dim: Option<&str>,
        temporal: &TemporalScope,
    ) -> Result<Self::Output, Self::Error>;

    /// Handle [`SqlPlan::ArrayElementwise`].
    fn array_elementwise(
        &mut self,
        left: &str,
        right: &str,
        op: ArrayBinaryOpAst,
        attr: &str,
    ) -> Result<Self::Output, Self::Error>;

    /// Handle [`SqlPlan::ArrayFlush`].
    fn array_flush(&mut self, name: &str) -> Result<Self::Output, Self::Error>;

    /// Handle [`SqlPlan::ArrayCompact`].
    fn array_compact(&mut self, name: &str) -> Result<Self::Output, Self::Error>;

    /// Handle [`SqlPlan::Merge`].
    fn merge(&mut self, args: MergeVisitArgs<'_>) -> Result<Self::Output, Self::Error>;

    /// Handle [`SqlPlan::LateralTopK`].
    fn lateral_top_k(
        &mut self,
        args: LateralTopKVisitArgs<'_>,
    ) -> Result<Self::Output, Self::Error>;

    /// Handle [`SqlPlan::LateralLoop`].
    fn lateral_loop(&mut self, args: LateralLoopVisitArgs<'_>)
    -> Result<Self::Output, Self::Error>;

    /// Handle [`SqlPlan::VectorPrimaryInsert`].
    fn vector_primary_insert(
        &mut self,
        args: VectorPrimaryInsertVisitArgs<'_>,
    ) -> Result<Self::Output, Self::Error>;

    /// Handle [`SqlPlan::VectorPrimaryDelete`].
    fn vector_primary_delete(
        &mut self,
        args: VectorPrimaryDeleteVisitArgs<'_>,
    ) -> Result<Self::Output, Self::Error>;

    /// Handle [`SqlPlan::VectorPrimaryUpdate`].
    fn vector_primary_update(
        &mut self,
        args: VectorPrimaryUpdateVisitArgs<'_>,
    ) -> Result<Self::Output, Self::Error>;

    /// Handle [`SqlPlan::CreateIndex`].
    fn create_index(
        &mut self,
        index_name: Option<&str>,
        collection: &str,
        field: &str,
        unique: bool,
        if_not_exists: bool,
        case_insensitive: bool,
    ) -> Result<Self::Output, Self::Error>;

    /// Handle [`SqlPlan::DropIndex`].
    fn drop_index(
        &mut self,
        index_name: &str,
        collection: Option<&str>,
        if_exists: bool,
    ) -> Result<Self::Output, Self::Error>;
}
