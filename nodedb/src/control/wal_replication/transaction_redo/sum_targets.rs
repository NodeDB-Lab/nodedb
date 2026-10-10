// SPDX-License-Identifier: BUSL-1.1

//! The materialized-sum resolution a transaction's buffered plans carry,
//! regrouped by source collection for the redo apply.

#![deny(clippy::wildcard_enum_match_arm)]

use std::collections::BTreeMap;

use nodedb_physical::physical_plan::{DocumentOp, PhysicalPlan, RedoSumTargets, ResolvedSumTarget};

/// Every sum target `plans` fold on the source's own core, one entry per
/// source collection, in collection order. A target resolved by several plans
/// is listed once.
///
/// The install folds every row of a collection against this one merged
/// table. So a target a plan defers onto its own `ApplyBalanceDelta` task
/// never enters it. Otherwise a sibling row of the same collection, a DELETE
/// next to an INSERT on the same account, folds that target on the source's
/// core as well as the shipped task moving it.
pub fn redo_sum_targets(plans: &[PhysicalPlan]) -> Vec<RedoSumTargets> {
    let mut by_collection: BTreeMap<String, RedoSumTargets> = BTreeMap::new();
    for plan in plans {
        let PhysicalPlan::Document(op) = plan else {
            continue;
        };
        let Some((collection, resolved, deferred)) = document_sum_targets(op) else {
            continue;
        };
        let mut folded = resolved
            .iter()
            .filter(|target| !shipped_on_own_task(target, deferred))
            .peekable();
        if folded.peek().is_none() {
            continue;
        }
        let entry = by_collection
            .entry(collection.to_string())
            .or_insert_with(|| RedoSumTargets {
                collection: collection.to_string(),
                resolved: Vec::new(),
            });
        for target in folded {
            if !entry.resolved.contains(target) {
                entry.resolved.push(target.clone());
            }
        }
    }
    by_collection.into_values().collect()
}

/// Whether `target` names a target collection its plan defers onto its own
/// `ApplyBalanceDelta` task.
fn shipped_on_own_task(target: &ResolvedSumTarget, deferred: &[String]) -> bool {
    target
        .target_collection
        .as_deref()
        .is_some_and(|collection| deferred.iter().any(|d| d == collection))
}

type SumTargetSlots<'a> = (&'a str, &'a [ResolvedSumTarget], &'a [String]);

/// The source collection and sum-target slots of one document write.
fn document_sum_targets(op: &DocumentOp) -> Option<SumTargetSlots<'_>> {
    match op {
        DocumentOp::PointInsert {
            collection,
            resolved_sum_targets,
            deferred_sum_targets,
            ..
        }
        | DocumentOp::BatchInsert {
            collection,
            resolved_sum_targets,
            deferred_sum_targets,
            ..
        } => Some((
            collection.as_str(),
            resolved_sum_targets,
            deferred_sum_targets,
        )),
        DocumentOp::PointPut {
            collection,
            resolved_sum_targets,
            ..
        }
        | DocumentOp::PointDelete {
            collection,
            resolved_sum_targets,
            ..
        }
        | DocumentOp::PointUpdate {
            collection,
            resolved_sum_targets,
            ..
        }
        | DocumentOp::Upsert {
            collection,
            resolved_sum_targets,
            ..
        }
        | DocumentOp::BulkUpdate {
            collection,
            resolved_sum_targets,
            ..
        }
        | DocumentOp::BulkDelete {
            collection,
            resolved_sum_targets,
            ..
        }
        | DocumentOp::Truncate {
            collection,
            resolved_sum_targets,
            ..
        } => Some((collection.as_str(), resolved_sum_targets, &[])),
        DocumentOp::UpdateFromJoin {
            target_collection,
            resolved_sum_targets,
            ..
        }
        | DocumentOp::Merge {
            target_collection,
            resolved_sum_targets,
            ..
        } => Some((target_collection.as_str(), resolved_sum_targets, &[])),
        // Reads, index DDL, and the ops that carry no sum resolution of their
        // own: a balance delta IS the target write, and a resolved write
        // carries its resolution per mutation, never staged in a transaction.
        DocumentOp::PointGet { .. }
        | DocumentOp::Scan { .. }
        | DocumentOp::RangeScan { .. }
        | DocumentOp::Register { .. }
        | DocumentOp::IndexLookup { .. }
        | DocumentOp::IndexedFetch { .. }
        | DocumentOp::DropIndex { .. }
        | DocumentOp::BackfillIndex { .. }
        | DocumentOp::EstimateCount { .. }
        | DocumentOp::InsertSelect { .. }
        | DocumentOp::MaterializeScan { .. }
        | DocumentOp::ApplyBalanceDelta { .. }
        | DocumentOp::ResolveWrite(_)
        | DocumentOp::ResolvedWrite { .. } => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::{DatabaseId, QualifiedCollection, Surrogate};

    fn delete_with(target: ResolvedSumTarget) -> PhysicalPlan {
        PhysicalPlan::Document(DocumentOp::PointDelete {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "entries"),
            document_id: "e1".into(),
            surrogate: Some(Surrogate::new(1)),
            pk_bytes: b"e1".to_vec(),
            returning: None,
            rls_filters: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            resolved_sum_targets: vec![target],
        })
    }

    #[test]
    fn a_target_resolved_by_two_writes_is_listed_once_per_source() {
        let target = ResolvedSumTarget::new("accounts", "a1", Surrogate::new(9));
        let plans = vec![delete_with(target.clone()), delete_with(target.clone())];
        let targets = redo_sum_targets(&plans);
        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].collection, "entries");
        assert_eq!(targets[0].resolved, vec![target]);
    }

    fn insert_deferring(target: ResolvedSumTarget, deferred: &str) -> PhysicalPlan {
        PhysicalPlan::Document(DocumentOp::PointInsert {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "entries"),
            document_id: "e2".into(),
            value: Vec::new(),
            if_absent: false,
            surrogate: Surrogate::new(2),
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: vec![target],
            deferred_sum_targets: vec![deferred.to_string()],
        })
    }

    /// An insert that ships its balance on its own task lends no resolution
    /// to the merged table. A delete of the same account in the same
    /// transaction ships its own balance too, and the merged table must not
    /// tell the install to fold it on the source's core as well.
    #[test]
    fn a_target_shipped_on_its_own_task_is_not_folded_by_a_sibling_row() {
        let shipped = ResolvedSumTarget::new("accounts", "a1", Surrogate::new(9));
        let plans = vec![
            insert_deferring(shipped, "accounts"),
            PhysicalPlan::Document(DocumentOp::PointDelete {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "entries"),
                document_id: "e1".into(),
                surrogate: Some(Surrogate::new(1)),
                pk_bytes: b"e1".to_vec(),
                returning: None,
                rls_filters: Vec::new(),
                rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
                resolved_sum_targets: Vec::new(),
            }),
        ];
        assert!(redo_sum_targets(&plans).is_empty());
    }

    /// A deferral names one target collection. The same insert still folds a
    /// target it does not defer.
    #[test]
    fn a_deferral_keeps_the_targets_it_does_not_name() {
        let kept = ResolvedSumTarget::new("audit_totals", "a1", Surrogate::new(7));
        let plan = insert_deferring(kept.clone(), "accounts");
        let targets = redo_sum_targets(&[plan]);
        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].resolved, vec![kept]);
    }

    #[test]
    fn a_write_with_no_binding_contributes_nothing() {
        let plan = PhysicalPlan::Document(DocumentOp::PointDelete {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "notes"),
            document_id: "n1".into(),
            surrogate: Some(Surrogate::new(1)),
            pk_bytes: b"n1".to_vec(),
            returning: None,
            rls_filters: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            resolved_sum_targets: Vec::new(),
        });
        assert!(redo_sum_targets(&[plan]).is_empty());
    }
}
