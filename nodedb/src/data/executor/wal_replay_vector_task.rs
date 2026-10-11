// SPDX-License-Identifier: BUSL-1.1

//! The synthetic task the vector replay arms hand the live handlers.

use crate::bridge::envelope::{PhysicalPlan, Priority, Request};
use crate::data::executor::task::{ExecutionTask, TaskState};
use crate::types::{DatabaseId, ReadConsistency};

use super::core_loop::CoreLoop;

impl CoreLoop {
    /// The LSN a replayed record hands its task. A `record_lsn` of 0 means
    /// the record has no durable LSN and yields `None`.
    pub(in crate::data::executor) fn replay_record_lsn(
        record_lsn: u64,
    ) -> Option<crate::types::Lsn> {
        (record_lsn != 0).then(|| crate::types::Lsn::new(record_lsn))
    }

    /// Build a synthetic `ExecutionTask` for WAL replay.
    ///
    /// Mirrors `CoreLoop::replay_task` (`replay_task.rs`). The task carries
    /// no meaningful request semantics. It lets the handler methods return a
    /// typed `Response`. The task carries the record's LSN so the handler
    /// records the write's version.
    pub(in crate::data::executor) fn replay_vector_task(
        tenant_id: crate::types::TenantId,
        database_id: DatabaseId,
        vshard_id: crate::types::VShardId,
        plan: PhysicalPlan,
        wal_lsn: Option<crate::types::Lsn>,
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
                wal_lsn,
                resolved_now_ms: None,
                commit_hlc: None,
                entry_version: None,
                admission: crate::bridge::envelope::Admission::Exempt(
                    crate::bridge::envelope::ExemptReason::AlreadyOrdered,
                ),
            },
            state: TaskState::Running,
            wal_lsn,
            resolved_now_ms: None,
        }
    }
}
