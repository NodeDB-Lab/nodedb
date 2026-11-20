// SPDX-License-Identifier: BUSL-1.1

//! Resolve the declared columns a KV write re-types under, from the catalog.
//!
//! A KV op is built with its declared columns pending. They resolve here
//! before the write is journaled or dispatched, so the live write, its WAL
//! record and its replay re-type under one set of columns.

use nodedb_physical::physical_plan::{KvOp, MetaOp, PhysicalPlan};

use crate::control::state::SharedState;
use crate::types::TenantId;

/// Resolve, in place, every pending declared-column set `plan` carries, from
/// the committed catalog through the config pin cache. Each collection names
/// its own database. A set already resolved keeps its columns. Returns
/// whether any set changed.
pub fn resolve_kv_declared_columns(
    state: &SharedState,
    tenant_id: TenantId,
    plan: &mut PhysicalPlan,
) -> crate::Result<bool> {
    let mut resolve = |op: &mut KvOp| -> crate::Result<bool> {
        let Some((collection, slot)) = op.declared_columns_mut() else {
            return Ok(false);
        };
        if !slot.is_pending() {
            return Ok(false);
        }
        *slot = state.config_pins.kv_declared_columns(
            state.credentials.catalog(),
            tenant_id,
            collection,
        )?;
        Ok(true)
    };
    visit_kv_ops(plan, &mut resolve)
}

/// Run `visit` on every KV op `plan` holds, nested batches included.
/// Returns whether any visit reported a change.
fn visit_kv_ops(
    plan: &mut PhysicalPlan,
    visit: &mut dyn FnMut(&mut KvOp) -> crate::Result<bool>,
) -> crate::Result<bool> {
    match plan {
        PhysicalPlan::Kv(op) => visit(op),
        PhysicalPlan::Meta(
            MetaOp::TransactionBatch { plans, .. }
            | MetaOp::CalvinExecuteStatic { plans, .. }
            | MetaOp::CalvinExecuteActive { plans, .. },
        ) => {
            let mut changed = false;
            for nested in plans.iter_mut() {
                changed |= visit_kv_ops(nested, visit)?;
            }
            Ok(changed)
        }
        // A staged write's overlay value re-types under these columns, and
        // COMMIT logs that value.
        PhysicalPlan::Meta(MetaOp::StageWrite { plan }) => visit_kv_ops(plan, visit),
        // A resolve part's plans only name the collections the transaction
        // wrote. The core reads no columns from them.
        // No other plan holds a KV write the core re-types.
        PhysicalPlan::Meta(_)
        | PhysicalPlan::Document(_)
        | PhysicalPlan::Vector(_)
        | PhysicalPlan::Graph(_)
        | PhysicalPlan::Text(_)
        | PhysicalPlan::Columnar(_)
        | PhysicalPlan::Timeseries(_)
        | PhysicalPlan::Spatial(_)
        | PhysicalPlan::Crdt(_)
        | PhysicalPlan::Query(_)
        | PhysicalPlan::Array(_)
        | PhysicalPlan::ClusterArray(_)
        | PhysicalPlan::ClusterEvent(_) => Ok(false),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use nodedb_physical::physical_plan::{DeclaredColumn, KvDeclaredColumns};
    use nodedb_types::{DatabaseId, QualifiedCollection, Surrogate};

    use super::*;
    use crate::bridge::dispatch::Dispatcher;
    use crate::wal::WalManager;

    fn state() -> (Arc<SharedState>, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = Arc::new(WalManager::open_for_testing(&dir.path().join("wal")).expect("wal"));
        let (dispatcher, _sides) = Dispatcher::new(1, 64);
        let state = SharedState::new(dispatcher, wal).expect("state");
        (state, dir)
    }

    fn put(declared_columns: KvDeclaredColumns) -> PhysicalPlan {
        PhysicalPlan::Kv(KvOp::Put {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "kv"),
            declared_columns,
            key: b"k".to_vec(),
            value: b"v".to_vec(),
            ttl_ms: 0,
            surrogate: Surrogate::new(1),
            returning: None,
            rls_filters: Vec::new(),
            provenance: None,
        })
    }

    fn columns_of(plan: &PhysicalPlan) -> KvDeclaredColumns {
        let PhysicalPlan::Kv(op) = plan else {
            panic!("a KV plan");
        };
        op.declared_columns().expect("a re-typing write").1.clone()
    }

    /// An applied replicated entry arrives resolved. This node's catalog,
    /// which declares nothing here, never replaces the proposer's columns.
    #[test]
    fn resolved_columns_are_never_resolved_again() {
        let (state, _dir) = state();
        let carried = KvDeclaredColumns::resolved(vec![
            DeclaredColumn::from_declared("value", "INT").expect("int"),
        ]);
        let mut plan = put(carried.clone());
        let changed =
            resolve_kv_declared_columns(&state, TenantId::new(1), &mut plan).expect("resolve");
        assert!(!changed, "a resolved set stays as it is");
        assert_eq!(columns_of(&plan), carried);
    }

    /// A staged write reaches the core resolved. A resolve part's plans are
    /// left as they are: the core reads no columns from them.
    #[test]
    fn a_staged_write_resolves_and_a_resolve_part_is_left_alone() {
        let (state, _dir) = state();
        let mut staged = PhysicalPlan::Meta(MetaOp::StageWrite {
            plan: Box::new(put(KvDeclaredColumns::pending())),
        });
        assert!(
            resolve_kv_declared_columns(&state, TenantId::new(1), &mut staged).expect("resolve"),
            "the staged write resolves"
        );
        let PhysicalPlan::Meta(MetaOp::StageWrite { plan }) = &staged else {
            panic!("a staged write");
        };
        assert_eq!(columns_of(plan), KvDeclaredColumns::none());

        let mut resolve = PhysicalPlan::Meta(MetaOp::ResolveTxnPart {
            txn_id: nodedb_types::id::TxnId::new(3),
            plans: vec![put(KvDeclaredColumns::pending())],
            part: 0,
            max_part_bytes: 1024,
        });
        assert!(
            !resolve_kv_declared_columns(&state, TenantId::new(1), &mut resolve).expect("resolve"),
            "a resolve part reads no catalog"
        );
    }

    #[test]
    fn pending_columns_resolve_from_the_catalog() {
        let (state, _dir) = state();
        let mut plan = put(KvDeclaredColumns::pending());
        assert!(resolve_kv_declared_columns(&state, TenantId::new(1), &mut plan).expect("resolve"));
        assert_eq!(
            columns_of(&plan),
            KvDeclaredColumns::none(),
            "a collection the catalog does not hold declares no columns"
        );
    }
}
