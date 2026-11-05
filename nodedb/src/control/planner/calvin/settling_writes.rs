// SPDX-License-Identifier: BUSL-1.1

//! What a settled request needs from its route.

use nodedb_physical::physical_plan::{DocumentOp, PhysicalPlan};
use nodedb_physical::physical_task::PhysicalTask;

use crate::control::server::shared::session::read_set::ReadSetEntry;

/// Whether a settled request ships a materialized-sum balance delta, or holds
/// the source reads its deltas were folded from. Such a request commits through
/// the deterministic scheduler. A gateway forward applies tasks one by one and
/// checks no read.
pub fn ships_balance_delta(tasks: &[PhysicalTask], reads: &[ReadSetEntry]) -> bool {
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
    use nodedb_physical::physical_task::PostSetOp;

    use super::*;
    use crate::control::server::shared::session::read_set::{EngineTag, ReadKey};
    use crate::types::{DatabaseId, TenantId, VShardId};

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

    fn truncate() -> PhysicalTask {
        task(PhysicalPlan::Document(DocumentOp::Truncate {
            collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, "postings"),
            restart_identity: false,
            resolved_sum_targets: Vec::new(),
            declared_primary_key: None,
        }))
    }

    fn balance_delta() -> PhysicalTask {
        task(PhysicalPlan::Document(DocumentOp::ApplyBalanceDelta {
            collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, "accounts"),
            document_id: "0a".to_owned(),
            surrogate: nodedb_types::Surrogate::new(1),
            column: "balance".to_owned(),
            delta: "5".to_owned(),
            join_column: "account_id".to_owned(),
            join_value: "acc".to_owned(),
            declared_primary_key: None,
        }))
    }

    fn settlement_read() -> ReadSetEntry {
        ReadSetEntry {
            engine: EngineTag::Document,
            database_id: DatabaseId::DEFAULT,
            tenant_id: TenantId::new(1),
            collection: "postings".to_owned(),
            key: ReadKey::Predicate,
            observed_at: Some(42),
            home: None,
        }
    }

    #[test]
    fn a_shipped_delta_needs_the_scheduler() {
        assert!(ships_balance_delta(&[truncate(), balance_delta()], &[]));
    }

    #[test]
    fn a_zero_net_settlement_ships_no_task_and_still_needs_the_scheduler() {
        assert!(ships_balance_delta(&[truncate()], &[settlement_read()]));
    }

    #[test]
    fn an_unsettled_request_can_forward() {
        assert!(!ships_balance_delta(&[truncate()], &[]));
    }
}
