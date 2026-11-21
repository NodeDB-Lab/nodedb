// SPDX-License-Identifier: BUSL-1.1

//! Footprints of document ops.
//!
//! A row write keys the collections it writes and the sum targets it folds
//! into, as the config-pin rule names them. A join write's source rows
//! arrive resolved on the plan, so the write reads no source collection.

use nodedb_physical::physical_plan::{DocumentOp, document_pin_needs};

use super::class::{Footprint, Scope};
use super::keys::{KeySet, read_of, write_of};

pub(super) fn document_footprint(op: &DocumentOp, scope: &Scope) -> Footprint {
    match op {
        DocumentOp::PointGet { collection, .. }
        | DocumentOp::Scan { collection, .. }
        | DocumentOp::RangeScan { collection, .. }
        | DocumentOp::IndexLookup { collection, .. }
        | DocumentOp::IndexedFetch { collection, .. }
        | DocumentOp::EstimateCount { collection, .. }
        | DocumentOp::MaterializeScan { collection, .. } => read_of(collection, scope),
        DocumentOp::PointPut { .. }
        | DocumentOp::PointInsert { .. }
        | DocumentOp::PointDelete { .. }
        | DocumentOp::PointUpdate { .. }
        | DocumentOp::BatchInsert { .. }
        | DocumentOp::Upsert { .. }
        | DocumentOp::BatchUpsert { .. }
        | DocumentOp::BulkUpdate { .. }
        | DocumentOp::BulkDelete { .. }
        | DocumentOp::Truncate { .. }
        | DocumentOp::UpdateFromJoin { .. }
        | DocumentOp::Merge { .. }
        | DocumentOp::ApplyBalanceDelta { .. }
        | DocumentOp::ResolvedWrite { .. } => row_write(op, scope),
        // Collection config and index ops write the collection alone.
        DocumentOp::Register { collection, .. }
        | DocumentOp::DropIndex { collection, .. }
        | DocumentOp::BackfillIndex { collection, .. } => write_of(collection, scope),
        DocumentOp::InsertSelect {
            target_collection, ..
        } => write_of(target_collection, scope),
        // The resolve pass reads what the wrapped write writes.
        DocumentOp::ResolveWrite(inner) => document_footprint(inner, scope),
    }
}

/// The write of every collection and sum target `op` writes.
fn row_write(op: &DocumentOp, scope: &Scope) -> Footprint {
    let needs = document_pin_needs(op);
    let mut keys = KeySet::new(scope);
    for (_, collection) in &needs.writes {
        keys = keys.stored(collection);
    }
    for targets in &needs.sum_targets {
        for target in &targets.resolved {
            keys = keys.sum_target(&target.target_collection);
        }
    }
    keys.write()
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::{
        DocumentResolvedMutation, PhysicalPlan, ResolvedSumTarget, RowSource,
    };
    use nodedb_types::{RlsWriteCheck, Surrogate, SystemTimeScope};

    use super::super::keys::tests::{coll, key, scope, write};
    use super::super::plan::plan_footprint;
    use super::*;

    fn class(op: DocumentOp) -> Footprint {
        plan_footprint(&PhysicalPlan::Document(op), &scope())
    }

    fn put(name: &str, targets: Vec<ResolvedSumTarget>) -> DocumentOp {
        DocumentOp::PointPut {
            collection: coll(name),
            document_id: "d".into(),
            value: Vec::new(),
            surrogate: Surrogate::new(1),
            pk_bytes: Vec::new(),
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: targets,
            rls_write_check: RlsWriteCheck::NoPolicyApplies,
        }
    }

    #[test]
    fn a_point_put_keys_its_collection_and_its_sum_targets() {
        assert_eq!(class(put("a", Vec::new())), write(vec![key("a")]));
        let target = ResolvedSumTarget::new("acct", "a1", Surrogate::new(7));
        assert_eq!(
            class(put("a", vec![target])),
            write(vec![key("a"), key("acct")])
        );
    }

    #[test]
    fn a_bulk_delete_keys_its_collection() {
        let delete = DocumentOp::BulkDelete {
            collection: coll("b"),
            filters: Vec::new(),
            returning: None,
            ollp_predicted_surrogates: None,
            ollp_predicted_edges: None,
            rls_filters: Vec::new(),
            rls_write_check: RlsWriteCheck::NoPolicyApplies,
            resolved_sum_targets: Vec::new(),
            declared_primary_key: None,
            system_time: SystemTimeScope::Current,
        };
        assert_eq!(class(delete), write(vec![key("b")]));
    }

    #[test]
    fn a_resolved_write_keys_every_mutated_collection() {
        let mutation = |name: &str| DocumentResolvedMutation::Delete {
            collection: coll(name),
            document_id: "d".into(),
            surrogate: Surrogate::new(1),
            pk_bytes: Vec::new(),
            precondition: None,
            resolved_sum_targets: Vec::new(),
        };
        let op = DocumentOp::ResolvedWrite {
            mutations: vec![mutation("a"), mutation("b")],
            response_payload: Vec::new(),
            rls_write_check: RlsWriteCheck::NoPolicyApplies,
        };
        assert_eq!(class(op), write(vec![key("a"), key("b")]));
    }

    #[test]
    fn a_truncate_keys_its_collection() {
        let truncate = DocumentOp::Truncate {
            collection: coll("t"),
            restart_identity: false,
            resolved_sum_targets: Vec::new(),
            declared_primary_key: None,
        };
        assert_eq!(class(truncate), write(vec![key("t")]));
    }

    #[test]
    fn an_insert_select_keys_its_target_alone() {
        let op = DocumentOp::InsertSelect {
            target_collection: coll("t"),
            source: RowSource::whole_collection(coll("s")),
            returning: None,
            rls_filters: Vec::new(),
        };
        assert_eq!(class(op), write(vec![key("t")]));
    }

    #[test]
    fn a_resolve_pass_takes_the_footprint_of_its_write() {
        let op = DocumentOp::ResolveWrite(Box::new(put("a", Vec::new())));
        assert_eq!(class(op), write(vec![key("a")]));
    }

    #[test]
    fn a_read_keys_its_collection() {
        let get = DocumentOp::PointGet {
            collection: coll("a"),
            document_id: "d".into(),
            surrogate: None,
            pk_bytes: Vec::new(),
            rls_filters: Vec::new(),
            system_time: SystemTimeScope::Current,
            valid_at_ms: None,
        };
        assert_eq!(class(get), Footprint::Keys(vec![key("a")]));
    }
}
