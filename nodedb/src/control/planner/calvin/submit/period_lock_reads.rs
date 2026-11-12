// SPDX-License-Identifier: BUSL-1.1

//! The period-lock reference rows a Calvin transaction's writes read.
//!
//! A write to a collection with a period lock is judged against the status
//! of its reference row: at the slice's stage, and again at its install. A
//! transaction that writes the reference row between the two would flip the
//! verdict after the commit, and a refused install halts the vShard. So
//! each reference row a gated write resolved is a read key, held `Shared`:
//!
//! - every writer of the reference row holds the row `Exclusive`, Calvin
//!   transaction or gated write alike, and waits until this transaction
//!   installed;
//! - every lower writer of it installed before this transaction's stage.
//!
//! So the stage and the install read one status.

use std::collections::{BTreeMap, BTreeSet};

use nodedb_cluster::calvin::types::{EngineKeySet, SortedVec, TxClass};
use nodedb_physical::physical_plan::PhysicalPlan;

use crate::control::planner::period_lock::period_lock_gated_collection;
use crate::control::state::SharedState;
use crate::control::wal_replication::transaction_redo::sum_targets::document_sum_targets;

use super::sum_target_rows::period_lock_ref_table;

/// Add the reference row of every period-locked write in `plans` to
/// `tx_class`'s read set, with each collection's period lock read from this
/// node's catalog. `plans` are `tx_class`'s decoded plans.
///
/// A class already split into parts was stamped by its coordinator, so it is
/// left as it is. A forwarded class is stamped again with the same result:
/// a key set already in the read set is not added twice.
pub(crate) fn stamp_period_lock_reads(
    state: &SharedState,
    tx_class: &mut TxClass,
    plans: &[PhysicalPlan],
) -> crate::Result<()> {
    if tx_class.is_multi_part() {
        return Ok(());
    }
    let database_id = tx_class.database_id;
    let tenant_id = tx_class.tenant_id.as_u64();
    let sets = period_lock_read_sets(database_id, plans, |collection| {
        period_lock_ref_table(state, database_id, tenant_id, collection)
    })?;
    let mut added = false;
    for set in sets {
        if !tx_class.read_set.0.contains(&set) {
            tx_class.read_set.0.push(set);
            added = true;
        }
    }
    // The participants derive from the read set too, so a widened read set
    // derives them again.
    if added {
        tx_class
            .restore_derived()
            .map_err(|e| crate::Error::Internal {
                detail: format!("calvin period-lock reads: participants underivable: {e}"),
            })?;
    }
    Ok(())
}

/// One document key set per reference collection, naming every reference
/// row the period-locked writes of `plans` resolved. `ref_table_of` names
/// the reference table of a collection's period lock, `None` for a
/// collection with none.
fn period_lock_read_sets(
    database_id: nodedb_types::DatabaseId,
    plans: &[PhysicalPlan],
    mut ref_table_of: impl FnMut(&str) -> crate::Result<Option<String>>,
) -> crate::Result<Vec<EngineKeySet>> {
    let mut rows: BTreeMap<String, BTreeSet<u32>> = BTreeMap::new();
    for plan in plans {
        let PhysicalPlan::Document(op) = plan else {
            continue;
        };
        let Some(collection) = period_lock_gated_collection(op) else {
            continue;
        };
        let Some(ref_table) = ref_table_of(collection)? else {
            continue;
        };
        let Some((_, resolved, _)) = document_sum_targets(op) else {
            continue;
        };
        let qualified = nodedb_types::QualifiedCollection::new(database_id, &ref_table).to_string();
        for target in resolved.iter().filter(|t| t.target_collection == ref_table) {
            rows.entry(qualified.clone())
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
    use nodedb_types::{DatabaseId, QualifiedCollection, RlsWriteCheck, Surrogate};

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

    fn ref_table(collection: &str) -> crate::Result<Option<String>> {
        Ok((collection == "journal").then(|| "periods".to_string()))
    }

    /// A period-locked write reads its reference row. A sum target of
    /// another collection on the same write is no read key.
    #[test]
    fn a_period_locked_write_reads_its_reference_row() {
        let plans = [
            insert(
                "journal",
                vec![
                    ResolvedSumTarget::new("periods", "2024-Q1", Surrogate::new(7)),
                    ResolvedSumTarget::new("accounts", "a1", Surrogate::new(9)),
                ],
            ),
            insert(
                "journal",
                vec![ResolvedSumTarget::new(
                    "periods",
                    "2024-Q2",
                    Surrogate::new(8),
                )],
            ),
        ];
        let sets = period_lock_read_sets(DatabaseId::DEFAULT, &plans, ref_table).expect("sets");
        assert_eq!(
            sets,
            vec![EngineKeySet::Document {
                collection: "periods".to_string(),
                surrogates: SortedVec::new(vec![7, 8]),
            }]
        );
    }

    #[test]
    fn a_write_with_no_period_lock_reads_nothing() {
        let plans = [insert(
            "orders",
            vec![ResolvedSumTarget::new(
                "periods",
                "2024-Q1",
                Surrogate::new(7),
            )],
        )];
        let sets = period_lock_read_sets(DatabaseId::DEFAULT, &plans, ref_table).expect("sets");
        assert!(sets.is_empty());
    }

    /// A catalog that cannot be read fails the stamp: an unread lock never
    /// drops its read key.
    #[test]
    fn a_catalog_error_fails_the_stamp() {
        let plans = [insert("journal", Vec::new())];
        let failed = period_lock_read_sets(DatabaseId::DEFAULT, &plans, |_| {
            Err(crate::Error::Internal {
                detail: "catalog unreadable".into(),
            })
        });
        assert!(failed.is_err());
    }
}
