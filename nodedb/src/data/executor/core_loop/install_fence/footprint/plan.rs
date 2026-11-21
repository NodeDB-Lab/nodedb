// SPDX-License-Identifier: BUSL-1.1

//! The footprint of a task, from its plan alone.
//!
//! A read of named state gets `Keys`, and a write of named state gets
//! `Write`. An op this module cannot bound to keys is a task of its whole
//! database. A misfiled write breaks the fence, so every match is
//! exhaustive. A write names the vShard its events carry, the task's own:
//! its events advance that vShard's partition.

use nodedb_physical::physical_plan::{CrdtOp, PhysicalPlan, QueryOp};

use crate::data::executor::task::ExecutionTask;

use super::array::{array_footprint, cluster_array_footprint};
use super::class::{Footprint, Scope};
use super::document::document_footprint;
use super::engines::{
    columnar_footprint, spatial_footprint, text_footprint, timeseries_footprint, vector_footprint,
};
use super::graph::graph_footprint;
use super::keys::read_of;
use super::kv::kv_footprint;
use super::meta::{cluster_event_footprint, meta_footprint};

/// The footprint of `task`.
pub(in crate::data::executor) fn footprint(task: &ExecutionTask) -> Footprint {
    plan_footprint(task.plan(), &Scope::of(task))
}

/// The footprint of `plan`, run in `scope`.
pub(in crate::data::executor) fn plan_footprint(plan: &PhysicalPlan, scope: &Scope) -> Footprint {
    match plan {
        PhysicalPlan::Document(op) => document_footprint(op, scope),
        PhysicalPlan::Kv(op) => kv_footprint(op, scope),
        PhysicalPlan::Vector(op) => vector_footprint(op, scope),
        PhysicalPlan::Text(op) => text_footprint(op, scope),
        PhysicalPlan::Columnar(op) => columnar_footprint(op, scope),
        PhysicalPlan::Timeseries(op) => timeseries_footprint(op, scope),
        PhysicalPlan::Spatial(op) => spatial_footprint(op, scope),
        PhysicalPlan::Query(op) => query_footprint(op, scope),
        PhysicalPlan::Graph(op) => graph_footprint(op, scope),
        PhysicalPlan::Crdt(op) => crdt_footprint(op, scope),
        PhysicalPlan::Array(op) => array_footprint(op, scope),
        PhysicalPlan::ClusterArray(op) => cluster_array_footprint(op, scope),
        PhysicalPlan::Meta(op) => meta_footprint(op, scope),
        PhysicalPlan::ClusterEvent(op) => cluster_event_footprint(op, scope),
    }
}

fn query_footprint(op: &QueryOp, scope: &Scope) -> Footprint {
    match op {
        QueryOp::Aggregate {
            collection, input, ..
        } => match input {
            Some(input) => read_of(collection, scope).join(plan_footprint(input, scope)),
            None => read_of(collection, scope),
        },
        QueryOp::PartialAggregate { collection, .. } | QueryOp::FacetCounts { collection, .. } => {
            read_of(collection, scope)
        }
        // Post-processing runs over its input alone.
        QueryOp::PostProcess { input, .. } => plan_footprint(input, scope),
        QueryOp::Exchange(_)
        | QueryOp::ProviderScan { .. }
        | QueryOp::SetOp { .. }
        | QueryOp::PartialAggregateState { .. }
        | QueryOp::HashJoin { .. }
        | QueryOp::ShuffleJoinConsume { .. }
        | QueryOp::ShuffleAggregateConsume { .. }
        | QueryOp::NestedLoopJoin { .. }
        | QueryOp::SortMergeJoin { .. }
        | QueryOp::RecursiveScan { .. }
        | QueryOp::RecursiveValue { .. }
        | QueryOp::LateralTopK { .. }
        | QueryOp::LateralLoop { .. } => scope.whole_database(),
    }
}

/// A CRDT op serves from or writes a Loro document whose sync state no key
/// names, so it is a task of its whole database.
fn crdt_footprint(op: &CrdtOp, scope: &Scope) -> Footprint {
    match op {
        CrdtOp::Read { .. }
        | CrdtOp::Apply { .. }
        | CrdtOp::ApplyAuthenticated { .. }
        | CrdtOp::ImportSnapshot { .. }
        | CrdtOp::SetConstraints { .. }
        | CrdtOp::DropConstraints { .. }
        | CrdtOp::ReadConstraints { .. }
        | CrdtOp::SetPolicy { .. }
        | CrdtOp::GetPolicy { .. }
        | CrdtOp::ReadAtVersion { .. }
        | CrdtOp::GetVersionVector { .. }
        | CrdtOp::ExportDelta { .. }
        | CrdtOp::RestoreToVersion { .. }
        | CrdtOp::CompactAtVersion { .. }
        | CrdtOp::ListInsert { .. }
        | CrdtOp::ListDelete { .. }
        | CrdtOp::ListMove { .. }
        | CrdtOp::DocUpsert { .. }
        | CrdtOp::DocDelete { .. }
        | CrdtOp::PreviewApply { .. } => scope.whole_database(),
    }
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::DocumentOp;

    use super::super::keys::tests::{coll, key, scope, whole};
    use super::*;

    fn point_get(name: &str) -> PhysicalPlan {
        PhysicalPlan::Document(DocumentOp::PointGet {
            collection: coll(name),
            document_id: "d".into(),
            surrogate: None,
            pk_bytes: Vec::new(),
            rls_filters: Vec::new(),
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
        })
    }

    #[test]
    fn post_processing_takes_the_footprint_of_its_input() {
        let post = PhysicalPlan::Query(QueryOp::PostProcess {
            input: Box::new(point_get("a")),
            filters: Vec::new(),
            projection: Vec::new(),
            computed_columns: Vec::new(),
            window_functions: Vec::new(),
            sort_keys: Vec::new(),
            limit: None,
            offset: 0,
            distinct: false,
        });
        assert_eq!(
            plan_footprint(&post, &scope()),
            Footprint::Keys(vec![key("a")])
        );
    }

    #[test]
    fn a_crdt_op_is_its_whole_database() {
        let policy = PhysicalPlan::Crdt(CrdtOp::GetPolicy {
            collection: coll("cr"),
        });
        assert_eq!(plan_footprint(&policy, &scope()), whole());
    }

    #[test]
    fn a_read_keyed_from_its_name_meets_a_fence_armed_from_install_collections() {
        use super::super::super::{FenceKey, InstallFence};
        use crate::types::DatabaseId;
        use nodedb_types::QualifiedCollection;

        let other = DatabaseId::new(7);
        let stored = QualifiedCollection::new(other, "users");
        // The install's collections carry the stored names it writes.
        let installed = vec![stored.as_str().to_string()];
        let keys = FenceKey::of_stored_collections(&installed).expect("stored names");
        let fence = InstallFence::new(keys.into_iter().collect());

        // A request of the default database still keys the read by the
        // database its collection names.
        let read = read_of(&stored, &scope());
        assert_eq!(
            read,
            Footprint::Keys(vec![FenceKey::Collection {
                db: other,
                name: stored.as_str().into(),
            }])
        );
        assert!(!fence.admits(&read));
        assert!(fence.admits(&read_of(&coll("users"), &scope())));
    }
}
