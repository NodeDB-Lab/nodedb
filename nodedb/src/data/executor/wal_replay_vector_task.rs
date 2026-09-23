// SPDX-License-Identifier: BUSL-1.1

//! The synthetic task the vector replay arms hand the live handlers.

use crate::bridge::envelope::{PhysicalPlan, Priority, Request};
use crate::data::executor::task::{ExecutionTask, TaskState};
use crate::types::{DatabaseId, ReadConsistency};

use super::core_loop::CoreLoop;

impl CoreLoop {
    /// Build a synthetic `ExecutionTask` for WAL replay.
    ///
    /// Mirrors `CoreLoop::replay_task` (`replay_task.rs`). The task carries
    /// no meaningful request semantics — it is only needed so that the handler
    /// methods can return a typed `Response`.
    pub(in crate::data::executor) fn replay_vector_task(
        tenant_id: crate::types::TenantId,
        database_id: DatabaseId,
        vshard_id: crate::types::VShardId,
        plan: PhysicalPlan,
    ) -> ExecutionTask {
        ExecutionTask {
            request: Request {
                request_id: crate::types::RequestId::new(0),
                tenant_id,
                database_id,
                vshard_id,
                plan,
                deadline: std::time::Instant::now()
                    + crate::data::executor::deadline::REPLAY_DEADLINE,
                priority: Priority::Normal,
                trace_id: crate::types::TraceId::ZERO,
                consistency: ReadConsistency::Strong,
                idempotency_key: None,
                event_source: crate::event::EventSource::User,
                user_roles: Vec::new(),
                user_id: None,
                statement_digest: None,
                txn_id: None,
                wal_lsn: None,
                resolved_now_ms: None,
                admission: crate::bridge::envelope::Admission::Exempt(
                    crate::bridge::envelope::ExemptReason::AlreadyOrdered,
                ),
            },
            state: TaskState::Running,
            wal_lsn: None,
            resolved_now_ms: None,
        }
    }
}
