// SPDX-License-Identifier: BUSL-1.1

//! The chain-head lock of a Calvin transaction's writes to a `HASH_CHAIN`
//! collection.
//!
//! Every row a chained collection stores links after the collection's chain
//! head, so every write to it moves the head. A Calvin transaction that
//! writes a chained collection locks the collection whole `Exclusive`.
//!
//! - Two Calvin writers of one chain serialize in sequence order.
//! - A gated insert takes the collection `Intent`, which the `Exclusive`
//!   lock conflicts with, so no insert lands between the slice's grant and
//!   its install.
//!
//! So the head the slice's stage reads is the head its install finds. The
//! stage derives each row's link from it, and the `RETURNING` reply answers
//! that link.

use nodedb_cluster::calvin::types::{EngineKeySet, SortedVec, TxClass};
use nodedb_physical::physical_plan::{PhysicalPlan, written_collections};

use crate::control::state::SharedState;

/// Add the chain-head lock of every `HASH_CHAIN` collection `plans` write to
/// `tx_class`'s write set, read from this node's catalog. `plans` are
/// `tx_class`'s decoded plans.
///
/// A class already split into parts was stamped by its coordinator, so it is
/// left as it is. A forwarded class is stamped again with the same result:
/// a key set already in the write set is not added twice.
pub(crate) fn stamp_chain_heads(
    state: &SharedState,
    tx_class: &mut TxClass,
    plans: &[PhysicalPlan],
) -> crate::Result<()> {
    if tx_class.is_multi_part() {
        return Ok(());
    }
    let sets = catalog_chain_head_sets(
        state,
        tx_class.database_id,
        tx_class.tenant_id.as_u64(),
        plans,
    )?;
    let mut added = false;
    for set in sets {
        if !tx_class.write_set.0.contains(&set) {
            tx_class.write_set.0.push(set);
            added = true;
        }
    }
    // The participants derive from the write set, so a widened write set
    // derives them again.
    if added {
        tx_class
            .restore_derived()
            .map_err(|e| crate::Error::Internal {
                detail: format!("calvin chain heads: participants underivable: {e}"),
            })?;
    }
    Ok(())
}

/// The chain-head key sets of `plans`, with each collection's `HASH_CHAIN`
/// flag read from this node's catalog.
fn catalog_chain_head_sets(
    state: &SharedState,
    database_id: nodedb_types::DatabaseId,
    tenant_id: u64,
    plans: &[PhysicalPlan],
) -> crate::Result<Vec<EngineKeySet>> {
    let catalog = state.credentials.catalog();
    chain_head_sets(plans, |collection| {
        let key = nodedb_types::CollectionKey::from_relation_name(database_id, collection)?;
        let stored = catalog.get_collection(key.database_id(), tenant_id, key.name())?;
        Ok(stored.is_some_and(|stored| stored.hash_chain))
    })
}

/// One whole-collection key set per collection `plans` write that
/// `hash_chained` names chained, in collection order.
fn chain_head_sets(
    plans: &[PhysicalPlan],
    mut hash_chained: impl FnMut(&str) -> crate::Result<bool>,
) -> crate::Result<Vec<EngineKeySet>> {
    let mut sets = Vec::new();
    for collection in written_collections(plans) {
        if hash_chained(&collection)? {
            sets.push(EngineKeySet::Collection {
                collection,
                vshards: SortedVec::new(Vec::new()),
            });
        }
    }
    Ok(sets)
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::DocumentOp;
    use nodedb_types::{DatabaseId, QualifiedCollection, RlsWriteCheck, Surrogate};

    use super::*;

    fn coll(name: &str) -> QualifiedCollection {
        QualifiedCollection::new(DatabaseId::DEFAULT, name)
    }

    fn insert(collection: &str) -> PhysicalPlan {
        PhysicalPlan::Document(DocumentOp::PointInsert {
            collection: coll(collection),
            document_id: "r1".into(),
            value: Vec::new(),
            if_absent: false,
            surrogate: Surrogate::new(1),
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
            deferred_sum_targets: Vec::new(),
            rls_write_check: RlsWriteCheck::NoPolicyApplies,
        })
    }

    /// Only a chained collection the plans write locks whole, once.
    #[test]
    fn a_chained_collection_locks_whole_once() {
        let ledger = coll("ledger").to_string();
        let plans = [insert("ledger"), insert("ledger"), insert("orders")];
        let sets = chain_head_sets(&plans, |collection| Ok(collection == ledger)).expect("sets");
        assert_eq!(
            sets,
            vec![EngineKeySet::Collection {
                collection: ledger.clone(),
                vshards: SortedVec::new(Vec::new()),
            }]
        );
    }

    #[test]
    fn plans_with_no_chained_collection_lock_nothing() {
        let plans = [insert("orders")];
        assert!(
            chain_head_sets(&plans, |_| Ok(false))
                .expect("sets")
                .is_empty()
        );
    }

    /// A catalog that cannot be read fails the stamp: an unread flag never
    /// drops the lock.
    #[test]
    fn a_catalog_error_fails_the_stamp() {
        let plans = [insert("ledger")];
        let failed = chain_head_sets(&plans, |_| {
            Err(crate::Error::Internal {
                detail: "catalog unreadable".into(),
            })
        });
        assert!(failed.is_err());
    }
}
