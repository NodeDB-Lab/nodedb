// SPDX-License-Identifier: BUSL-1.1

//! Whether a task set on one vShard commits through the Calvin scheduler.

use nodedb_physical::physical_task::PhysicalTask;

use crate::control::planner::calvin::TxnDispatchPosition;
use crate::control::server::shared::returning::tasks_carry_returning_contract;
use crate::control::server::shared::session::read_set::ReadSetEntry;

/// Whether a task set on one vShard commits through the scheduler at
/// `position`. Only an autocommit statement does, and only when it needs
/// what the scheduler gives:
///
/// - A zero-net settlement ships no task, so its set can hold one vShard.
///   Its `reads` still guard the balance.
/// - A task that carries a `RETURNING` contract has its list evaluated
///   before the vote, so an error in the list stores nothing.
///
/// Either commits through the scheduler in every cross-shard mode.
pub(crate) fn single_vshard_commits_through_calvin(
    tasks: &[PhysicalTask],
    reads: &[ReadSetEntry],
    position: TxnDispatchPosition,
) -> bool {
    position == TxnDispatchPosition::Autocommit
        && (!reads.is_empty() || tasks_carry_returning_contract(tasks))
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::{DocumentOp, PhysicalPlan};
    use nodedb_physical::physical_task::PostSetOp;
    use nodedb_types::{
        CollectionKey, QualifiedCollection, ReturningColumns, ReturningItem, ReturningSpec,
        RlsWriteCheck, Surrogate,
    };

    use super::*;
    use crate::control::server::shared::session::read_set::{EngineTag, ReadKey, ReadPoint};
    use crate::types::{DatabaseId, TenantId};

    fn insert(returning: Option<ReturningSpec>) -> PhysicalTask {
        PhysicalTask {
            tenant_id: TenantId::new(1),
            vshard_id: CollectionKey::from_bare(DatabaseId::DEFAULT, "single").vshard(),
            database_id: DatabaseId::DEFAULT,
            plan: PhysicalPlan::Document(DocumentOp::PointInsert {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "single"),
                document_id: "d1".into(),
                surrogate: Surrogate::new(1),
                value: Vec::new(),
                if_absent: false,
                returning,
                rls_filters: Vec::new(),
                resolved_sum_targets: Vec::new(),
                deferred_sum_targets: Vec::new(),
                rls_write_check: RlsWriteCheck::NoPolicyApplies,
            }),
            post_set_op: PostSetOp::None,
            txn_id: None,
        }
    }

    fn contract() -> ReturningSpec {
        let list = ReturningColumns::Named(vec![ReturningItem::Column {
            name: "id".into(),
            alias: None,
        }]);
        ReturningSpec {
            columns: list.clone(),
            precommit: Some(list),
        }
    }

    fn read() -> ReadSetEntry {
        ReadSetEntry {
            engine: EngineTag::Document,
            database_id: DatabaseId::DEFAULT,
            tenant_id: TenantId::new(1),
            collection: "single".to_owned(),
            key: ReadKey::Point {
                repr: ReadPoint::Surrogate(1),
            },
            observed_at: None,
            home: None,
        }
    }

    #[test]
    fn one_vshard_commits_through_calvin_only_at_autocommit_with_a_need() {
        let plain = [insert(None)];
        let carrying = [insert(Some(contract()))];
        let reads = [read()];
        let autocommit = TxnDispatchPosition::Autocommit;
        assert!(!single_vshard_commits_through_calvin(
            &plain,
            &[],
            autocommit
        ));
        assert!(single_vshard_commits_through_calvin(
            &plain, &reads, autocommit
        ));
        assert!(single_vshard_commits_through_calvin(
            &carrying,
            &[],
            autocommit
        ));
        for position in [
            TxnDispatchPosition::MidBlockStatement,
            TxnDispatchPosition::CommitFlush,
        ] {
            assert!(!single_vshard_commits_through_calvin(
                &carrying, &reads, position
            ));
        }
    }
}
