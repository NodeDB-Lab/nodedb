// SPDX-License-Identifier: BUSL-1.1

//! Every system-time scope a plan reads at, the plan's own and its child
//! plans'.
//!
//! One macro writes the exhaustive match once. It expands to a shared walk
//! for reads and to a mutable walk for the in-place rewrite.

use nodedb_physical::physical_plan::{
    ArrayOp, ClusterArrayOp, ColumnarOp, DocumentOp, PhysicalPlan, RowSource, TimeseriesOp,
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
    ($walk:ident, $own:ident, $document:ident, $children:ident $(, $m:tt)?) => {
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
                    ColumnarOp::Scan { system_time, .. } => visit(system_time),
                    ColumnarOp::Insert { .. }
                    | ColumnarOp::Update { .. }
                    | ColumnarOp::Delete { .. }
                    | ColumnarOp::ResolvedUpdate { .. }
                    | ColumnarOp::ResolvedDelete { .. }
                    | ColumnarOp::ResolveDml { .. }
                    | ColumnarOp::MaterializeScan { .. }
                    | ColumnarOp::Truncate { .. } => {}
                },
                PhysicalPlan::Timeseries(op) => match op {
                    TimeseriesOp::Scan { system_time, .. } => visit(system_time),
                    TimeseriesOp::Ingest { .. }
                    | TimeseriesOp::ResolveIngest(_)
                    | TimeseriesOp::Truncate { .. }
                    | TimeseriesOp::RefreshAggregate { .. }
                    | TimeseriesOp::Partitions { .. }
                    | TimeseriesOp::ReadAggregate { .. } => {}
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
                PhysicalPlan::Query(_)
                | PhysicalPlan::Vector(_)
                | PhysicalPlan::Graph(_)
                | PhysicalPlan::Kv(_)
                | PhysicalPlan::Text(_)
                | PhysicalPlan::Spatial(_)
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
                | DocumentOp::MaterializeScan { system_time, .. } => visit(system_time),
                DocumentOp::InsertSelect { source, .. }
                | DocumentOp::Merge { source, .. }
                | DocumentOp::UpdateFromJoin { source, .. } => match source {
                    RowSource::DocumentPages(page) => visit(&$($m)? page.system_time),
                    RowSource::Query(_) => {}
                },
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
                | DocumentOp::BulkUpdate { .. }
                | DocumentOp::BulkDelete { .. }
                | DocumentOp::ApplyBalanceDelta { .. }
                | DocumentOp::ResolvedWrite { .. } => {}
            }
        }
    };
}

cut_site_walk!(
    for_each_cut_site_ref,
    own_sites_ref,
    document_sites_ref,
    children
);
cut_site_walk!(
    for_each_cut_site,
    own_sites_mut,
    document_sites_mut,
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
