// SPDX-License-Identifier: BUSL-1.1

//! The request guard for writes that move one materialized-sum balance.

use std::collections::BTreeSet;

use nodedb_physical::physical_task::PhysicalTask;

use super::is_dependent_predicate;
use crate::Error;

/// The refusal text of [`refuse_several_settling_writes`]. The wire tests
/// match it, so a rewording cannot pass unseen.
pub const SEVERAL_WRITES_REFUSAL: &str = "several writes of one request move the same \
materialized-sum balance and one is a predicate write; run them as separate statements or in \
a transaction block";

/// Refuse a request in which a predicate write and another write move the
/// same materialized-sum balance. Each write settles its delta from the
/// committed rows, so a later write cannot see the rows an earlier one
/// changes and the balance would drift from the sum of the source.
///
/// A balance is one column of a target row. Two sum columns of one row, fed
/// by one write, are two balances and pass.
pub fn refuse_several_settling_writes(tasks: &[PhysicalTask]) -> crate::Result<()> {
    use nodedb_physical::physical_plan::{DocumentOp, PhysicalPlan};
    if !tasks.iter().any(|task| is_dependent_predicate(&task.plan)) {
        return Ok(());
    }
    let mut targets = BTreeSet::new();
    for task in tasks {
        if let PhysicalPlan::Document(DocumentOp::ApplyBalanceDelta {
            collection,
            document_id,
            column,
            ..
        }) = &task.plan
            && !targets.insert((collection.to_string(), document_id.clone(), column.clone()))
        {
            return Err(Error::BadRequest {
                detail: SEVERAL_WRITES_REFUSAL.to_owned(),
            });
        }
    }
    Ok(())
}

/// Whether a settled request ships a materialized-sum balance delta to another
/// vShard, or reads source images its deltas were folded from. Such a request
/// commits through the deterministic scheduler, which applies the deltas with
/// the writes that moved them and checks the images against the rows then.
pub fn ships_balance_delta(
    tasks: &[PhysicalTask],
    reads: &[crate::control::server::shared::session::read_set::ReadSetEntry],
) -> bool {
    use nodedb_physical::physical_plan::{DocumentOp, PhysicalPlan};
    !reads.is_empty()
        || tasks.iter().any(|task| {
            matches!(
                task.plan,
                PhysicalPlan::Document(DocumentOp::ApplyBalanceDelta { .. })
            )
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_physical::physical_plan::{DocumentOp, PhysicalPlan};

    use crate::types::{DatabaseId, TenantId, VShardId};

    fn task(plan: PhysicalPlan) -> PhysicalTask {
        PhysicalTask {
            tenant_id: TenantId::new(1),
            vshard_id: VShardId::new(0),
            database_id: DatabaseId::DEFAULT,
            plan,
            post_set_op: nodedb_physical::physical_task::PostSetOp::None,
            txn_id: None,
        }
    }

    fn bulk_delete(collection: &str) -> PhysicalTask {
        task(PhysicalPlan::Document(DocumentOp::BulkDelete {
            collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, collection),
            filters: Vec::new(),
            returning: None,
            ollp_predicted_surrogates: None,
            ollp_predicted_edges: None,
            rls_filters: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
            resolved_sum_targets: Vec::new(),
            declared_primary_key: None,
        }))
    }

    fn balance_delta(document_id: &str, column: &str) -> PhysicalTask {
        task(PhysicalPlan::Document(DocumentOp::ApplyBalanceDelta {
            collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, "accounts"),
            document_id: document_id.to_owned(),
            surrogate: nodedb_types::Surrogate::new(1),
            column: column.to_owned(),
            delta: "5".to_owned(),
            join_column: "account_id".to_owned(),
            join_value: "acc".to_owned(),
            declared_primary_key: None,
        }))
    }

    #[test]
    fn two_deltas_of_one_balance_beside_a_predicate_write_are_refused() {
        let tasks = vec![
            bulk_delete("postings"),
            balance_delta("0a", "balance"),
            balance_delta("0a", "balance"),
        ];
        assert!(refuse_several_settling_writes(&tasks).is_err());
    }

    #[test]
    fn two_sum_columns_of_one_row_are_not_refused() {
        let tasks = vec![
            bulk_delete("postings"),
            balance_delta("0a", "balance"),
            balance_delta("0a", "fees"),
        ];
        assert!(refuse_several_settling_writes(&tasks).is_ok());
    }

    #[test]
    fn plain_predicate_writes_are_not_refused() {
        let tasks = vec![bulk_delete("orders"), bulk_delete("users")];
        assert!(refuse_several_settling_writes(&tasks).is_ok());
    }
}
