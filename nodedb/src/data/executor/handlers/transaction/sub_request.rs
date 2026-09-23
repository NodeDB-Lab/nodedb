// SPDX-License-Identifier: BUSL-1.1

//! The request envelope a transaction sub-plan runs under.

use crate::bridge::envelope::{Admission, PhysicalPlan, Priority, Request};
use crate::types::{DatabaseId, ReadConsistency, RequestId, TenantId, TraceId, VShardId};

/// Request fields a sub-plan inherits from the batch that carries it.
pub(super) struct SubRequestScope {
    /// Data Plane handlers key storage and collection metadata by database.
    pub database_id: DatabaseId,
    /// Handlers decide shard-owner side effects by vShard.
    pub vshard_id: VShardId,
    pub deadline: std::time::Instant,
    pub admission: Admission,
}

impl SubRequestScope {
    /// Copy `parent`'s database, vShard, `deadline`, and `admission`.
    ///
    /// A sub-request's
    /// [`execution_deadline`](Request::execution_deadline) then equals the
    /// parent's. A sub-plan is part of the statement that spawned it, so it
    /// runs on that statement's remaining budget. An already-ordered parent,
    /// such as a Calvin apply, has no execution deadline, and neither does
    /// its sub-plan.
    pub(super) fn of(parent: &Request) -> Self {
        Self {
            database_id: parent.database_id,
            vshard_id: parent.vshard_id,
            // no-determinism: sub-plan deadline is ephemeral, not written to WAL
            deadline: parent.deadline,
            admission: parent.admission,
        }
    }

    /// Build the request that runs `plan` for tenant `tid` in this scope.
    pub(super) fn request(&self, tid: u64, plan: PhysicalPlan) -> Request {
        Request {
            request_id: RequestId::new(0),
            tenant_id: TenantId::new(tid),
            database_id: self.database_id,
            vshard_id: self.vshard_id,
            plan,
            // no-determinism: sub-plan deadline is ephemeral, not written to WAL
            deadline: self.deadline,
            priority: Priority::Normal,
            trace_id: TraceId::ZERO,
            consistency: ReadConsistency::Strong,
            idempotency_key: None,
            event_source: crate::event::EventSource::User,
            user_roles: Vec::new(),
            user_id: None,
            statement_digest: None,
            txn_id: None,
            wal_lsn: None,
            resolved_now_ms: None,
            admission: self.admission,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::envelope::ExemptReason;
    use nodedb_physical::physical_plan::MetaOp;

    fn cancel_plan() -> PhysicalPlan {
        PhysicalPlan::Meta(MetaOp::Cancel {
            target_request_id: RequestId::new(0),
        })
    }

    #[test]
    fn sub_request_runs_in_parent_database_and_vshard() {
        let parent = SubRequestScope {
            database_id: DatabaseId::new(7),
            vshard_id: VShardId::new(3),
            deadline: std::time::Instant::now() + std::time::Duration::from_secs(5),
            admission: Admission::Admitted,
        }
        .request(9, cancel_plan());

        let sub = SubRequestScope::of(&parent).request(9, PhysicalPlan::Meta(MetaOp::Compact));

        assert_eq!(sub.database_id, DatabaseId::new(7));
        assert_eq!(sub.vshard_id, VShardId::new(3));
        assert_eq!(sub.tenant_id, TenantId::new(9));
        assert!(matches!(sub.plan, PhysicalPlan::Meta(MetaOp::Compact)));
    }

    #[test]
    fn sub_request_shares_parent_execution_deadline() {
        for admission in [
            Admission::Admitted,
            Admission::Exempt(ExemptReason::Read),
            Admission::Exempt(ExemptReason::AlreadyOrdered),
        ] {
            let parent = SubRequestScope {
                database_id: DatabaseId::DEFAULT,
                vshard_id: VShardId::new(0),
                deadline: std::time::Instant::now() + std::time::Duration::from_secs(5),
                admission,
            }
            .request(1, cancel_plan());

            let sub = SubRequestScope::of(&parent).request(1, cancel_plan());

            assert_eq!(sub.admission, parent.admission);
            assert_eq!(sub.execution_deadline(), parent.execution_deadline());
        }
    }
}
