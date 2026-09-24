// SPDX-License-Identifier: BUSL-1.1

//! The plan a sorted-index read runs, and the transaction it runs in.
//!
//! Outside a transaction block a read asks the index's registered tree.
//! Inside one it becomes `KvOp::SortedIndexTxnRead`. The Data Plane answers
//! that from a transaction-local tree over the collection's base rows and the
//! transaction's staged writes. An index this transaction created has no tree
//! before COMMIT, so its definition travels with the read. The definition
//! comes from the index build that the transaction deferred to COMMIT.

use nodedb_physical::physical_plan::{KvOp, PhysicalPlan, SortedIndexRead, SortedIndexSpec};
use nodedb_types::QualifiedCollection;

use crate::control::server::shared::session::ddl_buffer;
use crate::control::server::shared::session::ddl_effect::DeferredDdlEffect;
use crate::control::server::shared::session::{DmlTxnCtx, TransactionState};
use crate::types::{DatabaseId, TenantId, TxnId};

/// One sorted-index read: the plan and the transaction whose overlay it reads.
pub(super) struct SortedRead {
    pub plan: PhysicalPlan,
    pub txn_id: Option<TxnId>,
}

/// Where a read runs and which index it names.
pub(super) struct ReadScope<'a> {
    pub txn_ctx: &'a DmlTxnCtx<'a>,
    pub tenant_id: TenantId,
    pub database_id: DatabaseId,
    /// The collection the index covers, resolved by the read gate.
    pub collection: &'a str,
    pub index_name: &'a str,
}

/// The plan for `read`.
pub(super) fn plan_read(scope: &ReadScope<'_>, read: SortedIndexRead) -> SortedRead {
    let txn_ctx = scope.txn_ctx;
    if txn_ctx.sessions.transaction_state(txn_ctx.session_id) != TransactionState::InBlock {
        return SortedRead {
            plan: PhysicalPlan::Kv(autocommit_op(scope.index_name, read)),
            txn_id: None,
        };
    }
    SortedRead {
        plan: PhysicalPlan::Kv(KvOp::SortedIndexTxnRead {
            collection: QualifiedCollection::new(scope.database_id, scope.collection),
            index_name: scope.index_name.to_string(),
            pending: pending_definition(scope.tenant_id, scope.database_id, scope.index_name),
            read,
        }),
        txn_id: txn_ctx.sessions.tx_id(txn_ctx.session_id),
    }
}

/// The read outside a transaction block, against the registered tree.
fn autocommit_op(index_name: &str, read: SortedIndexRead) -> KvOp {
    let index_name = index_name.to_string();
    match read {
        SortedIndexRead::Rank { primary_key } => KvOp::SortedIndexRank {
            index_name,
            primary_key,
        },
        SortedIndexRead::TopK { k } => KvOp::SortedIndexTopK { index_name, k },
        SortedIndexRead::Range {
            score_min,
            score_max,
        } => KvOp::SortedIndexRange {
            index_name,
            score_min,
            score_max,
        },
        SortedIndexRead::Count => KvOp::SortedIndexCount { index_name },
        SortedIndexRead::Score { primary_key } => KvOp::SortedIndexScore {
            index_name,
            primary_key,
        },
    }
}

/// The definition of `index_name` when this transaction created it and has
/// not committed it. `None` for a committed index.
///
/// Replays the deferred effects in statement order: a later drop of the same
/// name cancels an earlier create.
fn pending_definition(
    tenant_id: TenantId,
    database_id: DatabaseId,
    index_name: &str,
) -> Option<SortedIndexSpec> {
    ddl_buffer::with_buffered(|items| {
        let mut pending = None;
        for effect in items.iter().flat_map(|item| item.effects.iter()) {
            pending = step(pending, effect, tenant_id, database_id, index_name);
        }
        pending
    })
    .flatten()
}

/// Apply one deferred effect to the pending definition resolved so far.
fn step(
    current: Option<SortedIndexSpec>,
    effect: &DeferredDdlEffect,
    tenant_id: TenantId,
    database_id: DatabaseId,
    index_name: &str,
) -> Option<SortedIndexSpec> {
    match effect {
        DeferredDdlEffect::SortedIndexRegister {
            tenant_id: effect_tenant,
            database_id: effect_db,
            plan:
                PhysicalPlan::Kv(KvOp::RegisterSortedIndex {
                    index_name: effect_index,
                    sort_columns,
                    key_column,
                    window_type,
                    window_timestamp_column,
                    window_start_ms,
                    window_end_ms,
                    ..
                }),
            ..
        } if *effect_tenant == tenant_id
            && *effect_db == database_id
            && effect_index == index_name =>
        {
            Some(SortedIndexSpec {
                sort_columns: sort_columns.clone(),
                key_column: key_column.clone(),
                window_type: window_type.clone(),
                window_timestamp_column: window_timestamp_column.clone(),
                window_start_ms: *window_start_ms,
                window_end_ms: *window_end_ms,
            })
        }
        DeferredDdlEffect::SortedIndexDrop {
            tenant_id: effect_tenant,
            database_id: effect_db,
            index_name: effect_index,
            ..
        } if *effect_tenant == tenant_id
            && *effect_db == database_id
            && effect_index == index_name =>
        {
            None
        }
        _ => current,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn register(index_name: &str) -> DeferredDdlEffect {
        DeferredDdlEffect::SortedIndexRegister {
            tenant_id: TenantId::new(1),
            database_id: DatabaseId::DEFAULT,
            collection: "board".to_string(),
            plan: PhysicalPlan::Kv(KvOp::RegisterSortedIndex {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "board"),
                index_name: index_name.to_string(),
                sort_columns: vec![("score".to_string(), "DESC".to_string())],
                key_column: "id".to_string(),
                window_type: "none".to_string(),
                window_timestamp_column: String::new(),
                window_start_ms: 0,
                window_end_ms: 0,
            }),
        }
    }

    fn drop_effect(index_name: &str) -> DeferredDdlEffect {
        DeferredDdlEffect::SortedIndexDrop {
            tenant_id: TenantId::new(1),
            database_id: DatabaseId::DEFAULT,
            collection: "board".to_string(),
            index_name: index_name.to_string(),
        }
    }

    fn replay(effects: &[DeferredDdlEffect], index_name: &str) -> Option<SortedIndexSpec> {
        effects.iter().fold(None, |current, effect| {
            step(
                current,
                effect,
                TenantId::new(1),
                DatabaseId::DEFAULT,
                index_name,
            )
        })
    }

    #[test]
    fn a_buffered_create_carries_its_definition() {
        let spec = replay(&[register("lb")], "lb").expect("the create is pending");
        assert_eq!(spec.key_column, "id");
        assert_eq!(
            spec.sort_columns,
            vec![("score".to_string(), "DESC".to_string())]
        );
    }

    #[test]
    fn a_later_drop_cancels_the_create_and_other_names_are_ignored() {
        assert!(replay(&[register("lb"), drop_effect("lb")], "lb").is_none());
        assert!(replay(&[register("other")], "lb").is_none());
        assert!(replay(&[drop_effect("lb"), register("lb")], "lb").is_some());
    }
}
