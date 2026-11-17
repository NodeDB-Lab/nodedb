// SPDX-License-Identifier: BUSL-1.1

//! Whether a write carries a `RETURNING` contract.
//!
//! The planner puts the full response list in a plan's spec when the write
//! evaluates `RETURNING` before its source commits. A plan with that list
//! carries a contract: a Calvin submit stamps it, and the participant
//! evaluates the list before it votes.

use nodedb_physical::physical_plan::PhysicalPlan;
use nodedb_physical::physical_task::PhysicalTask;

/// Whether `plan` carries a `RETURNING` contract: its spec holds the full
/// list the precommit evaluation runs.
pub fn plan_carries_returning_contract(plan: &PhysicalPlan) -> bool {
    plan.returning_target()
        .is_some_and(|(_, spec)| spec.precommit.is_some())
}

/// Whether any task of `tasks` carries a `RETURNING` contract.
pub fn tasks_carry_returning_contract(tasks: &[PhysicalTask]) -> bool {
    tasks
        .iter()
        .any(|task| plan_carries_returning_contract(&task.plan))
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::DocumentOp;
    use nodedb_physical::physical_task::PostSetOp;
    use nodedb_types::{
        DatabaseId, QualifiedCollection, ReturningColumns, ReturningItem, ReturningSpec,
    };

    use super::*;
    use crate::types::{TenantId, VShardId};

    fn insert(returning: Option<ReturningSpec>) -> PhysicalPlan {
        PhysicalPlan::Document(DocumentOp::PointInsert {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "items"),
            document_id: "a".into(),
            value: Vec::new(),
            if_absent: false,
            surrogate: nodedb_types::Surrogate::new(1),
            returning,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
            deferred_sum_targets: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
        })
    }

    fn spec(precommit: bool) -> ReturningSpec {
        let named = ReturningColumns::Named(vec![ReturningItem::Column {
            name: "id".into(),
            alias: None,
        }]);
        ReturningSpec {
            columns: named.clone(),
            precommit: precommit.then_some(named),
        }
    }

    fn task(plan: PhysicalPlan) -> PhysicalTask {
        PhysicalTask {
            tenant_id: TenantId::new(1),
            vshard_id: VShardId::new(0),
            database_id: DatabaseId::DEFAULT,
            plan,
            post_set_op: PostSetOp::None,
            txn_id: None,
        }
    }

    #[test]
    fn only_a_full_list_carries_a_contract() {
        assert!(plan_carries_returning_contract(&insert(Some(spec(true)))));
        assert!(!plan_carries_returning_contract(&insert(Some(spec(false)))));
        assert!(!plan_carries_returning_contract(&insert(None)));
    }

    #[test]
    fn a_task_set_carries_a_contract_when_one_task_does() {
        assert!(!tasks_carry_returning_contract(&[]));
        assert!(!tasks_carry_returning_contract(&[task(insert(None))]));
        assert!(tasks_carry_returning_contract(&[
            task(insert(None)),
            task(insert(Some(spec(true)))),
        ]));
    }
}
