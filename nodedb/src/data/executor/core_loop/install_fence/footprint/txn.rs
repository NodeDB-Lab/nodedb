// SPDX-License-Identifier: BUSL-1.1

//! Footprints of a transaction's staging, resolve and overlay drop.
//!
//! A staged write and a resolve read the base state of every collection
//! their plans write, and the transaction's overlay. Each also reads what
//! its plans read: a staged label set reads the tenant's graph. The
//! overlay drop releases the overlay, drops the empty columnar engines the
//! transaction created, and releases its timeseries resolve holds.
//!
//! None of these emits an event, so each orders by its keys alone. The
//! overlay key orders an overlay drop behind a waiting staged write of its
//! own transaction.

use nodedb_physical::physical_plan::{PhysicalPlan, written_collections};

use crate::types::TxnId;

use super::class::{Footprint, Scope};
use super::keys::KeySet;
use super::plan::plan_footprint;

/// The footprint of staging `plan` into the overlay of the request's
/// transaction.
pub(super) fn stage_write_footprint(plan: &PhysicalPlan, scope: &Scope) -> Footprint {
    staged(std::slice::from_ref(plan), scope.txn, scope)
}

/// The footprint of resolving transaction `txn`, whose buffered writes
/// are `plans`.
pub(super) fn resolve_footprint(txn: TxnId, plans: &[PhysicalPlan], scope: &Scope) -> Footprint {
    staged(plans, Some(txn), scope)
}

/// The footprint of dropping the overlay of transaction `txn`.
pub(super) fn drop_overlay_footprint(txn: TxnId, scope: &Scope) -> Footprint {
    scope
        .registry
        .overlay_collections(txn)
        .iter()
        .fold(KeySet::new(scope).txn_overlay(txn), |keys, (db, name)| {
            keys.named_in(*db, name)
        })
        .read()
}

/// A read of the collections `plans` write, of the overlay of `txn`, and
/// of everything each plan reads.
fn staged(plans: &[PhysicalPlan], txn: Option<TxnId>, scope: &Scope) -> Footprint {
    let base = written_collections(plans)
        .iter()
        .fold(KeySet::new(scope), |keys, collection| {
            keys.stored(collection)
        });
    let keys = match txn {
        Some(txn) => base.txn_overlay(txn),
        None => base,
    };
    plans.iter().fold(keys.read(), |footprint, plan| {
        footprint.join(as_read(plan_footprint(plan, scope)))
    })
}

/// `footprint` with its writes read: a staged plan emits no event.
fn as_read(footprint: Footprint) -> Footprint {
    match footprint {
        Footprint::Write { keys, .. } => Footprint::Keys(keys),
        Footprint::Keys(_)
        | Footprint::WholeDatabase { .. }
        | Footprint::Core
        | Footprint::Background => footprint,
    }
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::{DocumentOp, GraphOp, MetaOp};

    use super::super::keys::tests::{coll, graph, key, scope};
    use super::super::registry::tests::{FakeRegistry, NO_REGISTRY};
    use super::*;
    use crate::data::executor::core_loop::install_fence::FenceKey;

    fn put(name: &str) -> PhysicalPlan {
        PhysicalPlan::Document(DocumentOp::PointPut {
            collection: coll(name),
            document_id: "d".into(),
            value: Vec::new(),
            surrogate: nodedb_types::Surrogate::new(1),
            pk_bytes: Vec::new(),
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
        })
    }

    fn overlay(txn: u64) -> FenceKey {
        FenceKey::TxnOverlay(TxnId::new(txn))
    }

    fn class(op: MetaOp, txn: Option<TxnId>) -> Footprint {
        let scope = Scope { txn, ..scope() };
        plan_footprint(&PhysicalPlan::Meta(op), &scope)
    }

    #[test]
    fn a_staged_write_keys_its_collection_and_its_overlay() {
        let stage = MetaOp::StageWrite {
            plan: Box::new(put("a")),
        };
        assert_eq!(
            class(stage, Some(TxnId::new(2))),
            Footprint::Keys(vec![key("a"), overlay(2), key("a")])
        );
    }

    #[test]
    fn a_staged_label_set_keys_the_graph() {
        let stage = MetaOp::StageWrite {
            plan: Box::new(PhysicalPlan::Graph(GraphOp::SetNodeLabels {
                node_id: "n".into(),
                labels: vec!["Person".into()],
            })),
        };
        assert_eq!(
            class(stage, Some(TxnId::new(2))),
            Footprint::Keys(vec![overlay(2), graph()])
        );
    }

    #[test]
    fn a_resolve_keys_its_transactions_overlay() {
        let resolve = MetaOp::ResolveTxnPart {
            txn_id: TxnId::new(3),
            plans: vec![put("a")],
            part: 0,
            max_part_bytes: 1,
        };
        assert_eq!(
            class(resolve, None),
            Footprint::Keys(vec![key("a"), overlay(3), key("a")])
        );
    }

    #[test]
    fn a_savepoint_keys_its_overlay_alone() {
        let mark = MetaOp::MarkSavepoint {
            txn_id: TxnId::new(4),
            savepoint: 1,
        };
        assert_eq!(class(mark, None), Footprint::Keys(vec![overlay(4)]));
    }

    /// A registry whose transaction 5 created the columnar engine `m`.
    const CREATED: FakeRegistry = FakeRegistry {
        overlay: Some((5, "m")),
        ..NO_REGISTRY
    };

    #[test]
    fn an_overlay_drop_keys_its_overlay_and_the_engines_it_created() {
        let scope = Scope {
            registry: &CREATED,
            ..scope()
        };
        let drop = |txn: u64| {
            plan_footprint(
                &PhysicalPlan::Meta(MetaOp::DropTxnOverlay {
                    txn_id: TxnId::new(txn),
                }),
                &scope,
            )
        };
        assert_eq!(drop(5), Footprint::Keys(vec![overlay(5), key("m")]));
        assert_eq!(drop(6), Footprint::Keys(vec![overlay(6)]));
    }
}
