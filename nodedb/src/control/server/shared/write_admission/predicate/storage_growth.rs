// SPDX-License-Identifier: BUSL-1.1

//! The storage-growth predicate: the write-class plans a storage quota
//! refuses once a database or tenant is full.
//!
//! A write-class plan grows storage unless `frees_or_keeps_space` lists it.
//! A new write-class variant refuses over the limit until it is listed.

use crate::bridge::envelope::PhysicalPlan;
use nodedb_physical::physical_plan::{
    ArrayOp, ClusterArrayOp, ColumnarOp, CrdtOp, DocumentOp, DocumentResolvedMutation, GraphOp,
    KvOp, KvResolvedMutation, MetaOp, SpatialOp, TextOp, TimeseriesOp, VectorOp,
    VectorResolvedMutation,
};

use super::plan_is_write::plan_is_write;

/// Whether `plan` is a write that can add stored bytes.
///
/// A write that both removes and adds, such as UPDATE or UPSERT, adds.
pub fn plan_adds_storage(plan: &PhysicalPlan) -> bool {
    if !plan_is_write(plan) {
        return false;
    }
    match plan {
        PhysicalPlan::Meta(
            MetaOp::TransactionBatch { plans, .. } | MetaOp::ResolveTxn { plans, .. },
        ) => plans.iter().any(plan_adds_storage),
        _ => !frees_or_keeps_space(plan),
    }
}

/// A write-class plan that removes data, adds no row, or is internal to the
/// node. A resolved write is listed only when every mutation removes data
/// or leaves the body untouched.
fn frees_or_keeps_space(plan: &PhysicalPlan) -> bool {
    removes_data(plan)
        || adds_no_row(plan)
        || internal_maintenance(plan)
        || resolved_without_adds(plan)
}

/// DELETE, TRUNCATE, and the index and edge removals they derive.
fn removes_data(plan: &PhysicalPlan) -> bool {
    matches!(
        plan,
        PhysicalPlan::Text(TextOp::FtsDeleteDoc { .. })
            | PhysicalPlan::Spatial(SpatialOp::Delete { .. })
            | PhysicalPlan::Timeseries(TimeseriesOp::Truncate { .. })
            | PhysicalPlan::Crdt(CrdtOp::ListDelete { .. } | CrdtOp::DocDelete { .. })
            | PhysicalPlan::Vector(
                VectorOp::Delete { .. }
                    | VectorOp::DeleteBySurrogate { .. }
                    | VectorOp::SparseDelete { .. }
                    | VectorOp::MultiVectorDelete { .. }
                    | VectorOp::DirectDelete { .. }
                    | VectorOp::DirectTruncate { .. }
            )
            | PhysicalPlan::Document(
                DocumentOp::PointDelete { .. }
                    | DocumentOp::BulkDelete { .. }
                    | DocumentOp::Truncate { .. }
            )
            | PhysicalPlan::Graph(
                GraphOp::EdgeDelete { .. }
                    | GraphOp::EdgeDeleteBatch { .. }
                    | GraphOp::RemoveNodeLabels { .. }
                    | GraphOp::TruncateEdges { .. }
            )
            | PhysicalPlan::Columnar(
                ColumnarOp::Delete { .. }
                    | ColumnarOp::ResolvedDelete { .. }
                    | ColumnarOp::Truncate { .. }
            )
            | PhysicalPlan::Kv(
                KvOp::Delete { .. } | KvOp::Truncate { .. } | KvOp::PredicateDelete { .. }
            )
            | PhysicalPlan::Array(ArrayOp::Delete { .. })
            | PhysicalPlan::ClusterArray(ClusterArrayOp::Delete { .. })
            | PhysicalPlan::Meta(MetaOp::PurgeTenant { .. })
    )
}

/// A write-class plan that stores no new row: a schema install, an index
/// drop, a TTL change, a guard read, or a derived-state fold.
fn adds_no_row(plan: &PhysicalPlan) -> bool {
    matches!(
        plan,
        PhysicalPlan::Crdt(CrdtOp::SetConstraints { .. } | CrdtOp::DropConstraints { .. })
            | PhysicalPlan::Kv(
                KvOp::RegisterIndex { .. }
                    | KvOp::DropIndex { .. }
                    | KvOp::RegisterSortedIndex { .. }
                    | KvOp::DropSortedIndex { .. }
                    | KvOp::Expire { .. }
                    | KvOp::Persist { .. }
            )
            | PhysicalPlan::Graph(
                GraphOp::NodeEdgeGuard { .. } | GraphOp::NodePresenceGuard { .. }
            )
            | PhysicalPlan::Columnar(ColumnarOp::ResolveDml { .. })
            | PhysicalPlan::Document(DocumentOp::ApplyBalanceDelta { .. })
            | PhysicalPlan::Timeseries(TimeseriesOp::RefreshAggregate { .. })
    )
}

/// A write the node issues on its own or replays from a log already ordered
/// and admitted. The originating client write passed the quota already.
fn internal_maintenance(plan: &PhysicalPlan) -> bool {
    matches!(
        plan,
        PhysicalPlan::Meta(
            MetaOp::WalAppend { .. }
                | MetaOp::CalvinResolve { .. }
                | MetaOp::ApplyTransactionRedo { .. }
                | MetaOp::RestoreRedo(_)
                | MetaOp::CalvinExecuteStatic { .. }
                | MetaOp::CalvinExecutePassive { .. }
                | MetaOp::CalvinExecuteActive { .. }
                | MetaOp::CalvinDrop { .. }
        )
    )
}

/// A resolved write whose every mutation removes data or leaves the body
/// untouched.
fn resolved_without_adds(plan: &PhysicalPlan) -> bool {
    match plan {
        PhysicalPlan::Kv(KvOp::ResolvedWrite { mutations, .. }) => mutations.iter().all(|m| {
            matches!(
                m,
                KvResolvedMutation::Delete { .. }
                    | KvResolvedMutation::Expire { .. }
                    | KvResolvedMutation::Persist { .. }
                    | KvResolvedMutation::Reap { .. }
            )
        }),
        PhysicalPlan::Document(DocumentOp::ResolvedWrite { mutations, .. }) => mutations
            .iter()
            .all(|m| matches!(m, DocumentResolvedMutation::Delete { .. })),
        PhysicalPlan::Vector(VectorOp::ResolvedDirectWrite { mutations, .. }) => mutations
            .iter()
            .all(|m| matches!(m, VectorResolvedMutation::Delete { .. })),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::{DatabaseId, QualifiedCollection, Surrogate};

    fn collection() -> QualifiedCollection {
        QualifiedCollection::new(DatabaseId::DEFAULT, "c")
    }

    fn put() -> PhysicalPlan {
        PhysicalPlan::Kv(KvOp::Put {
            collection: collection(),
            key: b"k".to_vec(),
            value: b"v".to_vec(),
            ttl_ms: 0,
            surrogate: Surrogate::new(1),
            returning: None,
            rls_filters: Vec::new(),
            provenance: None,
        })
    }

    fn delete() -> PhysicalPlan {
        PhysicalPlan::Kv(KvOp::Delete {
            collection: collection(),
            keys: vec![b"k".to_vec()],
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            returning: None,
            rls_filters: Vec::new(),
            provenance: None,
        })
    }

    fn truncate() -> PhysicalPlan {
        PhysicalPlan::Kv(KvOp::Truncate {
            collection: collection(),
            restart_identity: false,
        })
    }

    fn resolved(mutations: Vec<KvResolvedMutation>) -> PhysicalPlan {
        PhysicalPlan::Kv(KvOp::ResolvedWrite {
            mutations,
            response_payload: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
        })
    }

    fn resolved_delete() -> KvResolvedMutation {
        KvResolvedMutation::Delete {
            collection: collection(),
            key: b"k".to_vec(),
            precondition: None,
        }
    }

    fn resolved_put() -> KvResolvedMutation {
        KvResolvedMutation::Put {
            collection: collection(),
            key: b"k".to_vec(),
            value: b"v".to_vec(),
            ttl_ms: 0,
            expire_at_ms: 0,
            surrogate: Surrogate::new(1),
            precondition: None,
        }
    }

    #[test]
    fn an_insert_adds_storage() {
        assert!(plan_adds_storage(&put()));
    }

    #[test]
    fn a_delete_and_a_truncate_add_no_storage() {
        assert!(!plan_adds_storage(&delete()));
        assert!(!plan_adds_storage(&truncate()));
        assert!(!plan_adds_storage(&PhysicalPlan::Meta(
            MetaOp::PurgeTenant { tenant_id: 1 }
        )));
    }

    #[test]
    fn a_read_adds_no_storage() {
        let plan = PhysicalPlan::Meta(MetaOp::Cancel {
            target_request_id: crate::types::RequestId::new(1),
        });
        assert!(!plan_adds_storage(&plan));
    }

    #[test]
    fn a_resolved_write_adds_storage_when_any_mutation_adds() {
        assert!(!plan_adds_storage(&resolved(vec![resolved_delete()])));
        assert!(plan_adds_storage(&resolved(vec![
            resolved_delete(),
            resolved_put()
        ])));
    }

    #[test]
    fn a_transaction_adds_storage_when_any_plan_adds() {
        let batch = |plans| {
            PhysicalPlan::Meta(MetaOp::ResolveTxn {
                txn_id: nodedb_types::id::TxnId::new(1),
                plans,
            })
        };
        assert!(!plan_adds_storage(&batch(vec![delete(), truncate()])));
        assert!(plan_adds_storage(&batch(vec![delete(), put()])));
    }

    #[test]
    fn an_internal_redo_install_adds_no_storage() {
        let plan = PhysicalPlan::Meta(MetaOp::CalvinDrop {
            epoch: 1,
            position: 0,
        });
        assert!(!plan_adds_storage(&plan));
    }
}
