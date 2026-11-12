// SPDX-License-Identifier: BUSL-1.1

//! The materialized-sum target rows a write folds on its own vShard.
//!
//! The install of a document write folds each row's delta into its target
//! row: it reads the row, adds the delta, and writes it back. A Calvin slice
//! judges that fold at its stage (`check_commit_boundary`), so the target
//! rows must hold between the stage and the install. Each target row a
//! write folds on the source's vShard is a write key, held `Exclusive`:
//!
//! - a Calvin transaction that writes the row, or folds into it, holds the
//!   row too, and waits until this transaction installed;
//! - a gated write takes the same keys through `plan_admission_keys`.
//!
//! A target on another vShard travels on its own `ApplyBalanceDelta` task,
//! which locks its row as a document write. A period-lock reference row
//! shares the resolution slot. The write reads it and never writes it, so it
//! is no write key here (`period_lock_reads`).

use std::collections::{BTreeMap, BTreeSet};

use nodedb_cluster::calvin::types::{EngineKeySet, SortedVec, TxClass};
use nodedb_physical::physical_plan::PhysicalPlan;

use crate::control::state::SharedState;
use crate::control::wal_replication::transaction_redo::sum_targets::document_sum_targets;

/// Add the target row of every fold `plans` apply on their own vShard to
/// `tx_class`'s write set. `plans` are `tx_class`'s decoded plans.
///
/// A class already split into parts was stamped by its coordinator, so it is
/// left as it is. A forwarded class is stamped again with the same result.
pub(crate) fn stamp_sum_target_rows(
    state: &SharedState,
    tx_class: &mut TxClass,
    plans: &[PhysicalPlan],
) -> crate::Result<()> {
    if tx_class.is_multi_part() {
        return Ok(());
    }
    let sets = catalog_sum_target_sets(
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
                detail: format!("calvin sum targets: participants underivable: {e}"),
            })?;
    }
    Ok(())
}

/// The target-row key sets of `plans`, with each source collection's period
/// lock read from this node's catalog. A Calvin transaction and a gated
/// write both lock through it.
pub(crate) fn catalog_sum_target_sets(
    state: &SharedState,
    database_id: nodedb_types::DatabaseId,
    tenant_id: u64,
    plans: &[PhysicalPlan],
) -> crate::Result<Vec<EngineKeySet>> {
    sum_target_row_sets(database_id, plans, |collection| {
        period_lock_ref_table(state, database_id, tenant_id, collection)
    })
}

/// The reference table of `collection`'s period lock, read from this node's
/// catalog. `None` for a collection with none.
pub(super) fn period_lock_ref_table(
    state: &SharedState,
    database_id: nodedb_types::DatabaseId,
    tenant_id: u64,
    collection: &str,
) -> crate::Result<Option<String>> {
    let key = nodedb_types::CollectionKey::from_qualified_str(database_id, collection)?;
    Ok(state
        .credentials
        .catalog()
        .get_collection(key.database_id(), tenant_id, key.name())?
        .and_then(|stored| stored.period_lock)
        .map(|config| config.ref_table))
}

/// One document key set per target collection, naming every target row the
/// writes of `plans` fold on their own vShard. `ref_table_of` names the
/// period-lock reference table of a source collection, whose entries are
/// reads.
fn sum_target_row_sets(
    database_id: nodedb_types::DatabaseId,
    plans: &[PhysicalPlan],
    mut ref_table_of: impl FnMut(&str) -> crate::Result<Option<String>>,
) -> crate::Result<Vec<EngineKeySet>> {
    let mut rows: BTreeMap<String, BTreeSet<u32>> = BTreeMap::new();
    for plan in plans {
        let PhysicalPlan::Document(op) = plan else {
            continue;
        };
        let Some((collection, resolved, deferred)) = document_sum_targets(op) else {
            continue;
        };
        if resolved.is_empty() {
            continue;
        }
        let source = nodedb_types::CollectionKey::from_qualified_str(database_id, collection)?;
        let ref_table = ref_table_of(collection)?;
        for target in resolved {
            let shipped = deferred.contains(&target.target_collection);
            let reference = ref_table.as_deref() == Some(target.target_collection.as_str());
            let folded_here =
                crate::query::sum_target_is_co_resident(source, &target.target_collection);
            if shipped || reference || !folded_here {
                continue;
            }
            rows.entry(
                nodedb_types::QualifiedCollection::new(database_id, &target.target_collection)
                    .to_string(),
            )
            .or_default()
            .insert(target.surrogate.as_u32());
        }
    }
    Ok(rows
        .into_iter()
        .map(|(collection, surrogates)| EngineKeySet::Document {
            collection,
            surrogates: SortedVec::new(surrogates.into_iter().collect()),
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::{DocumentOp, ResolvedSumTarget};
    use nodedb_types::{CollectionKey, DatabaseId, QualifiedCollection, RlsWriteCheck, Surrogate};

    use super::*;

    fn insert(collection: &str, targets: Vec<ResolvedSumTarget>) -> PhysicalPlan {
        PhysicalPlan::Document(DocumentOp::PointInsert {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, collection),
            document_id: "r1".into(),
            value: Vec::new(),
            if_absent: false,
            surrogate: Surrogate::new(1),
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: targets,
            deferred_sum_targets: Vec::new(),
            rls_write_check: RlsWriteCheck::NoPolicyApplies,
        })
    }

    /// A target collection on the vShard of `source`.
    fn co_resident_target(source: &str) -> String {
        let key = CollectionKey::from_bare(DatabaseId::DEFAULT, source);
        (0..)
            .map(|n| format!("totals_{n}"))
            .find(|name| crate::query::sum_target_is_co_resident(key, name))
            .expect("a co-resident target")
    }

    /// A fold on the source's vShard locks its target row. A period-lock
    /// reference row in the same slot is no write key.
    #[test]
    fn a_co_resident_fold_locks_its_target_row() {
        let target = co_resident_target("journal");
        let plans = [insert(
            "journal",
            vec![
                ResolvedSumTarget::new(&target, "a1", Surrogate::new(9)),
                ResolvedSumTarget::new("periods", "2024-Q1", Surrogate::new(7)),
            ],
        )];
        let sets = sum_target_row_sets(DatabaseId::DEFAULT, &plans, |_| {
            Ok(Some("periods".to_string()))
        })
        .expect("sets");
        assert_eq!(
            sets,
            vec![EngineKeySet::Document {
                collection: target,
                surrogates: SortedVec::new(vec![9]),
            }]
        );
    }

    #[test]
    fn a_write_with_no_fold_locks_nothing() {
        let plans = [insert("journal", Vec::new())];
        let sets = sum_target_row_sets(DatabaseId::DEFAULT, &plans, |_| Ok(None)).expect("sets");
        assert!(sets.is_empty());
    }
}
