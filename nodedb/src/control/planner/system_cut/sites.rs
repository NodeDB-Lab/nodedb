// SPDX-License-Identifier: BUSL-1.1

//! Every system-time scope a plan reads at, the plan's own and its child
//! plans'.
//!
//! One macro writes the exhaustive match once. It expands to a shared walk
//! for reads and to a mutable walk for the in-place rewrite.

use nodedb_physical::physical_plan::{
    ArrayOp, ClusterArrayOp, ColumnarOp, DocumentOp, KvOp, MetaOp, PhysicalPlan, QueryOp,
    RowSource, SpatialOp, TextOp, TimeseriesOp, VectorOp,
};
use nodedb_types::SystemTimeScope;

use crate::control::planner::plan_children::{children, children_mut};

/// Every system-time scope `plan` and its child plans read at, in walk
/// order.
pub fn plan_scopes(plan: &PhysicalPlan) -> Vec<SystemTimeScope> {
    let mut scopes = Vec::new();
    for_each_cut_site_ref(plan, &mut |scope| scopes.push(*scope));
    scopes
}

/// Expands the cut-site walk for one reference kind: `$($m)?` is empty for
/// a shared walk and `mut` for a mutable one. Every match is exhaustive.
macro_rules! cut_site_walk {
    ($walk:ident, $own:ident, $document:ident, $kv:ident, $query:ident, $children:ident $(, $m:tt)?) => {
        /// Call `visit` on every system-time scope `plan` and its child plans
        /// read at, in walk order.
        pub fn $walk(
            plan: &$($m)? PhysicalPlan,
            visit: &mut dyn FnMut(&$($m)? SystemTimeScope),
        ) {
            $own(plan, visit);
            for child in $children(plan) {
                $walk(child, visit);
            }
        }

        /// The cuts `plan` itself reads at. Exhaustive over [`PhysicalPlan`].
        fn $own(
            plan: &$($m)? PhysicalPlan,
            visit: &mut dyn FnMut(&$($m)? SystemTimeScope),
        ) {
            match plan {
                PhysicalPlan::Document(op) => $document(op, visit),
                PhysicalPlan::Columnar(op) => match op {
                    ColumnarOp::Scan { system_time, .. }
                    | ColumnarOp::Update { system_time, .. }
                    | ColumnarOp::Delete { system_time, .. } => visit(system_time),
                    ColumnarOp::Insert { .. }
                    | ColumnarOp::ResolvedUpdate { .. }
                    | ColumnarOp::ResolvedDelete { .. }
                    | ColumnarOp::ResolveDml { .. }
                    | ColumnarOp::MaterializeScan { .. }
                    | ColumnarOp::Truncate { .. } => {}
                },
                PhysicalPlan::Timeseries(op) => match op {
                    TimeseriesOp::Scan { system_time, .. }
                    | TimeseriesOp::ReadAggregate { system_time, .. } => visit(system_time),
                    TimeseriesOp::Ingest { .. }
                    | TimeseriesOp::ResolveIngest(_)
                    | TimeseriesOp::Truncate { .. }
                    | TimeseriesOp::RefreshAggregate { .. }
                    | TimeseriesOp::Partitions { .. } => {}
                },
                PhysicalPlan::Array(op) => match op {
                    ArrayOp::Slice { system_time, .. }
                    | ArrayOp::Aggregate { system_time, .. }
                    | ArrayOp::Elementwise { system_time, .. } => visit(system_time),
                    ArrayOp::OpenArray { .. }
                    | ArrayOp::Put { .. }
                    | ArrayOp::Delete { .. }
                    | ArrayOp::Flush { .. }
                    | ArrayOp::Compact { .. }
                    | ArrayOp::SurrogateBitmapScan { .. }
                    | ArrayOp::DropArray { .. }
                    | ArrayOp::RekeyArray { .. }
                    | ArrayOp::PurgeArrayDrop { .. } => {}
                },
                PhysicalPlan::ClusterArray(op) => match op {
                    ClusterArrayOp::Slice { system_time, .. }
                    | ClusterArrayOp::Agg { system_time, .. }
                    | ClusterArrayOp::Elementwise { system_time, .. } => visit(system_time),
                    ClusterArrayOp::Put { .. }
                    | ClusterArrayOp::Delete { .. }
                    | ClusterArrayOp::Flush { .. }
                    | ClusterArrayOp::Compact { .. } => {}
                },
                PhysicalPlan::Kv(op) => $kv(op, visit),
                PhysicalPlan::Query(op) => $query(op, visit),
                PhysicalPlan::Vector(op) => match op {
                    VectorOp::Search { system_time, .. }
                    | VectorOp::MultiSearch { system_time, .. }
                    | VectorOp::SparseSearch { system_time, .. }
                    | VectorOp::MultiVectorScoreSearch { system_time, .. } => visit(system_time),
                    VectorOp::Insert { .. }
                    | VectorOp::BatchInsert { .. }
                    | VectorOp::Delete { .. }
                    | VectorOp::DeleteBySurrogate { .. }
                    | VectorOp::SetParams { .. }
                    | VectorOp::DropIndex { .. }
                    | VectorOp::QueryStats { .. }
                    | VectorOp::Seal { .. }
                    | VectorOp::CompactIndex { .. }
                    | VectorOp::Rebuild { .. }
                    | VectorOp::SparseInsert { .. }
                    | VectorOp::SparseDelete { .. }
                    | VectorOp::MultiVectorInsert { .. }
                    | VectorOp::MultiVectorDelete { .. }
                    | VectorOp::DirectUpsert { .. }
                    | VectorOp::DirectInsert { .. }
                    | VectorOp::DirectInsertIfAbsent { .. }
                    | VectorOp::DirectBatchWrite { .. }
                    | VectorOp::DirectDelete { .. }
                    | VectorOp::DirectTruncate { .. }
                    | VectorOp::DirectUpdate { .. }
                    | VectorOp::ResolveDirectWrite(_)
                    | VectorOp::ResolvedDirectWrite { .. } => {}
                },
                PhysicalPlan::Text(op) => match op {
                    TextOp::Search { system_time, .. }
                    | TextOp::BM25ScoreScan { system_time, .. }
                    | TextOp::PhraseSearch { system_time, .. }
                    | TextOp::HybridSearch { system_time, .. }
                    | TextOp::HybridSearchTriple { system_time, .. } => visit(system_time),
                    TextOp::FtsIndexDoc { .. }
                    | TextOp::FtsDeleteDoc { .. }
                    | TextOp::SetTextConfig { .. } => {}
                },
                PhysicalPlan::Spatial(op) => match op {
                    SpatialOp::Scan { system_time, .. } => visit(system_time),
                    SpatialOp::Insert { .. } | SpatialOp::Delete { .. } => {}
                },
                // A last-value read answers at its cut from the cache's
                // version history.
                PhysicalPlan::Meta(
                    MetaOp::QueryLastValues { system_time, .. }
                    | MetaOp::QueryLastValue { system_time, .. },
                ) => visit(system_time),
                // A graph read carries a `GraphReadScope`, pinned on its own
                // (`pin::pin_graph_snapshot`, `graph_dispatch::graph_read_scope`).
                PhysicalPlan::Graph(_)
                | PhysicalPlan::Crdt(_)
                | PhysicalPlan::Meta(_)
                | PhysicalPlan::ClusterEvent(_) => {}
            }
        }

        /// The cuts a document op itself reads at. A query source's plan is
        /// a child plan, so the walk reaches it through the child walk.
        fn $document(
            op: &$($m)? DocumentOp,
            visit: &mut dyn FnMut(&$($m)? SystemTimeScope),
        ) {
            match op {
                DocumentOp::PointGet { system_time, .. }
                | DocumentOp::Scan { system_time, .. }
                | DocumentOp::IndexedFetch { system_time, .. }
                | DocumentOp::MaterializeScan { system_time, .. }
                | DocumentOp::BulkUpdate { system_time, .. }
                | DocumentOp::BulkDelete { system_time, .. } => visit(system_time),
                DocumentOp::InsertSelect { source, .. } => match source {
                    RowSource::DocumentPages(page) => visit(&$($m)? page.system_time),
                    RowSource::Query(_) => {}
                },
                // The join reads the source and matches the target rows.
                DocumentOp::Merge {
                    source,
                    target_system_time,
                    ..
                }
                | DocumentOp::UpdateFromJoin {
                    source,
                    target_system_time,
                    ..
                } => {
                    if let RowSource::DocumentPages(page) = source {
                        visit(&$($m)? page.system_time);
                    }
                    visit(target_system_time);
                }
                DocumentOp::ResolveWrite(inner) => $document(inner, visit),
                DocumentOp::PointPut { .. }
                | DocumentOp::PointInsert { .. }
                | DocumentOp::PointDelete { .. }
                | DocumentOp::PointUpdate { .. }
                | DocumentOp::BatchInsert { .. }
                | DocumentOp::RangeScan { .. }
                | DocumentOp::Register { .. }
                | DocumentOp::IndexLookup { .. }
                | DocumentOp::DropIndex { .. }
                | DocumentOp::BackfillIndex { .. }
                | DocumentOp::Truncate { .. }
                | DocumentOp::EstimateCount { .. }
                | DocumentOp::Upsert { .. }
                | DocumentOp::BatchUpsert { .. }
                | DocumentOp::ApplyBalanceDelta { .. }
                | DocumentOp::ResolvedWrite { .. } => {}
            }
        }

        /// The cuts a KV op itself reads at.
        fn $kv(
            op: &$($m)? KvOp,
            visit: &mut dyn FnMut(&$($m)? SystemTimeScope),
        ) {
            match op {
                KvOp::Get { system_time, .. }
                | KvOp::Scan { system_time, .. }
                | KvOp::BatchGet { system_time, .. }
                | KvOp::FieldGet { system_time, .. }
                | KvOp::SortedIndexRank { system_time, .. }
                | KvOp::SortedIndexTopK { system_time, .. }
                | KvOp::SortedIndexRange { system_time, .. }
                | KvOp::SortedIndexCount { system_time, .. }
                | KvOp::SortedIndexScore { system_time, .. }
                | KvOp::MaterializeScan { system_time, .. }
                | KvOp::SortedIndexTxnRead { system_time, .. }
                | KvOp::PredicateUpdate { system_time, .. }
                | KvOp::PredicateDelete { system_time, .. } => visit(system_time),
                KvOp::ResolveWrite(inner) => $kv(inner, visit),
                KvOp::Put { .. }
                | KvOp::Insert { .. }
                | KvOp::InsertIfAbsent { .. }
                | KvOp::InsertOnConflictUpdate { .. }
                | KvOp::Delete { .. }
                | KvOp::Expire { .. }
                | KvOp::Persist { .. }
                | KvOp::GetTtl { .. }
                | KvOp::BatchPut { .. }
                | KvOp::RegisterIndex { .. }
                | KvOp::DropIndex { .. }
                | KvOp::FieldSet { .. }
                | KvOp::Truncate { .. }
                | KvOp::Incr { .. }
                | KvOp::IncrFloat { .. }
                | KvOp::Cas { .. }
                | KvOp::GetSet { .. }
                | KvOp::Transfer { .. }
                | KvOp::TransferItem { .. }
                | KvOp::RegisterSortedIndex { .. }
                | KvOp::DropSortedIndex { .. }
                | KvOp::ResolvedWrite { .. } => {}
            }
        }

        /// The cuts a query op itself reads at: the per-shard scan of an
        /// aggregate, the by-name sides of a join, the matched rows of a
        /// facet count, a recursive scan's collection, a named relation the
        /// coordinator materializes, and a LATERAL join's inner scans. Each sub-plan is a child plan, so the walk reaches it
        /// through the child walk.
        fn $query(
            op: &$($m)? QueryOp,
            visit: &mut dyn FnMut(&$($m)? SystemTimeScope),
        ) {
            match op {
                QueryOp::Aggregate { system_time, .. }
                | QueryOp::PartialAggregate { system_time, .. }
                | QueryOp::PartialAggregateState { system_time, .. }
                | QueryOp::NestedLoopJoin { system_time, .. }
                | QueryOp::SortMergeJoin { system_time, .. }
                | QueryOp::FacetCounts { system_time, .. }
                | QueryOp::RecursiveScan { system_time, .. }
                | QueryOp::ProviderScan { system_time, .. } => visit(system_time),
                QueryOp::LateralTopK {
                    inner_system_time, ..
                }
                | QueryOp::LateralLoop {
                    inner_system_time, ..
                } => visit(inner_system_time),
                QueryOp::Exchange(_)
                | QueryOp::PostProcess { .. }
                | QueryOp::SetOp { .. }
                | QueryOp::HashJoin { .. }
                | QueryOp::ShuffleJoinConsume { .. }
                | QueryOp::ShuffleAggregateConsume { .. }
                | QueryOp::RecursiveValue { .. } => {}
            }
        }
    };
}

cut_site_walk!(
    for_each_cut_site_ref,
    own_sites_ref,
    document_sites_ref,
    kv_sites_ref,
    query_sites_ref,
    children
);
cut_site_walk!(
    for_each_cut_site,
    own_sites_mut,
    document_sites_mut,
    kv_sites_mut,
    query_sites_mut,
    children_mut,
    mut
);

#[cfg(test)]
mod tests {
    use super::*;

    fn point_get(system_time: SystemTimeScope) -> DocumentOp {
        DocumentOp::PointGet {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "docs",
            ),
            document_id: "a".into(),
            surrogate: None,
            pk_bytes: Vec::new(),
            rls_filters: Vec::new(),
            system_time,
            valid_at_ms: None,
        }
    }

    fn update_from_join(target_system_time: SystemTimeScope) -> PhysicalPlan {
        PhysicalPlan::Document(DocumentOp::UpdateFromJoin {
            target_collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "t",
            ),
            source: RowSource::whole_collection(nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "s",
            )),
            source_alias: "s".into(),
            target_join_col: "id".into(),
            source_join_col: "id".into(),
            updates: Vec::new(),
            target_filters: Vec::new(),
            returning: None,
            source_rows: None,
            rls_filters: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            resolved_sum_targets: Vec::new(),
            declared_primary_key: None,
            target_system_time,
        })
    }

    /// A join write's target match is a cut site, as its paged source is,
    /// so a transaction block pins both to its snapshot.
    #[test]
    fn a_join_target_match_is_a_cut_site() {
        let mut plan = update_from_join(SystemTimeScope::Current);
        assert_eq!(
            plan_scopes(&plan),
            vec![SystemTimeScope::Current, SystemTimeScope::Current]
        );
        let snapshot = SystemTimeScope::TxnSnapshot {
            ordinal: 42,
            cut_marker: 1,
        };
        for_each_cut_site(&mut plan, &mut |scope| *scope = snapshot);
        let PhysicalPlan::Document(DocumentOp::UpdateFromJoin {
            target_system_time, ..
        }) = plan
        else {
            panic!("expected UpdateFromJoin");
        };
        assert_eq!(target_system_time, snapshot);
    }

    /// The shared and the mutable walk reach the same sites in one order.
    #[test]
    fn both_walks_reach_the_same_sites() {
        let mut plan = PhysicalPlan::Document(DocumentOp::ResolveWrite(Box::new(point_get(
            SystemTimeScope::AsOf(7),
        ))));
        assert_eq!(plan_scopes(&plan), vec![SystemTimeScope::AsOf(7)]);
        for_each_cut_site(&mut plan, &mut |scope| *scope = SystemTimeScope::Current);
        assert_eq!(plan_scopes(&plan), vec![SystemTimeScope::Current]);
    }
}
