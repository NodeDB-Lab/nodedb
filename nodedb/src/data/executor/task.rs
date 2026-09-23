// SPDX-License-Identifier: BUSL-1.1

use crate::bridge::envelope::{PhysicalPlan, Request};
use crate::types::{Lsn, RequestId};

/// An execution task on a Data Plane core.
///
/// Wraps a `Request` envelope with execution state tracking.
/// This type is `!Send` — it lives and dies on a single core.
pub struct ExecutionTask {
    /// The original request envelope.
    pub request: Request,

    /// Current execution state.
    pub state: TaskState,

    /// WAL LSN the Control Plane allocated for this write at wal-dispatch time,
    /// carried alongside the request so the apply chokepoint can record the
    /// per-key / per-collection write version (see
    /// [`CoreLoop::note_write_lsn`](crate::data::executor::core_loop::CoreLoop)).
    /// `None` for reads, control ops, and writes whose LSN is not (yet) threaded
    /// — the version index is skipped rather than advanced with a wrong value.
    /// Copied from [`Request::wal_lsn`](crate::bridge::envelope::Request) in
    /// [`ExecutionTask::new`] — the request envelope is the cross-plane channel
    /// that carries the allocated LSN from Control Plane to this core.
    pub wal_lsn: Option<Lsn>,

    /// Wall-clock instant (ms since epoch) the Control Plane resolved at
    /// WAL-append time for a TTL-bearing KV write, carried alongside the
    /// request so live apply installs the SAME instant the durable WAL record
    /// carries rather than re-reading the clock. Re-reading it at apply time
    /// would let the live value disagree with the durable one by the dispatch
    /// latency — harmless day to day, but a crash between the two would have
    /// replay recompute `now_ms` at restart time instead of installing the
    /// original instant, pushing the TTL's expiry forward by the
    /// crash-to-restart delay. `None` for non-TTL writes, reads, and writes
    /// whose resolved instant is not (yet) threaded.
    /// Copied from [`Request::resolved_now_ms`](crate::bridge::envelope::Request)
    /// in [`ExecutionTask::new`], same as `wal_lsn`.
    pub resolved_now_ms: Option<u64>,
}

/// Lifecycle states for a Data Plane task.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskState {
    /// Queued, waiting for core capacity.
    Pending,
    /// Actively executing (io_uring submitted, SIMD running, etc.)
    Running,
    /// Completed successfully — response ready to send back.
    Completed,
    /// Cancelled by Control Plane.
    Cancelled,
    /// Failed with error.
    Failed,
}

impl ExecutionTask {
    pub fn new(request: Request) -> Self {
        // The request envelope is the only Control->Data channel: copy the
        // allocated write LSN and resolved TTL instant off it so the apply
        // chokepoints on this core can record the per-key / per-collection
        // write version and install the same expiry instant the WAL record
        // carries.
        let wal_lsn = request.wal_lsn;
        let resolved_now_ms = request.resolved_now_ms;
        Self {
            request,
            state: TaskState::Pending,
            wal_lsn,
            resolved_now_ms,
        }
    }

    /// Construct a task carrying the WAL LSN allocated for its write.
    /// `resolved_now_ms` is `None` — callers needing a resolved TTL instant on
    /// a directly-constructed task should go through [`ExecutionTask::new`].
    pub fn with_wal_lsn(request: Request, wal_lsn: Option<Lsn>) -> Self {
        Self {
            request,
            state: TaskState::Pending,
            wal_lsn,
            resolved_now_ms: None,
        }
    }

    /// WAL LSN allocated for this write, if any.
    pub fn wal_lsn(&self) -> Option<Lsn> {
        self.wal_lsn
    }

    /// Wall-clock instant the Control Plane resolved for a TTL-bearing KV
    /// write, if any. See the field doc on [`ExecutionTask::resolved_now_ms`].
    pub fn resolved_now_ms(&self) -> Option<u64> {
        self.resolved_now_ms
    }

    pub fn request_id(&self) -> RequestId {
        self.request.request_id
    }

    pub fn plan(&self) -> &PhysicalPlan {
        &self.request.plan
    }

    /// Whether the task's execution deadline has passed. Already-ordered work
    /// has no execution deadline and never expires.
    pub fn is_expired(&self) -> bool {
        self.request
            .execution_deadline()
            .is_some_and(|deadline| std::time::Instant::now() > deadline)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::envelope::{PhysicalPlan, Priority};
    use crate::types::{DatabaseId, ReadConsistency, TenantId, TraceId, VShardId};
    use nodedb_physical::physical_plan::MetaOp;
    use std::time::{Duration, Instant};

    fn request_with_wal_lsn(wal_lsn: Option<Lsn>) -> Request {
        Request {
            request_id: RequestId::new(1),
            tenant_id: TenantId::new(1),
            database_id: DatabaseId::DEFAULT,
            vshard_id: VShardId::new(0),
            plan: PhysicalPlan::Meta(MetaOp::Cancel {
                target_request_id: RequestId::new(7),
            }),
            deadline: Instant::now() + Duration::from_secs(5),
            priority: Priority::Normal,
            trace_id: TraceId::ZERO,
            consistency: ReadConsistency::Strong,
            idempotency_key: None,
            event_source: crate::event::EventSource::User,
            user_roles: Vec::new(),
            user_id: None,
            statement_digest: None,
            txn_id: None,
            wal_lsn,
            resolved_now_ms: None,
            admission: crate::bridge::envelope::Admission::Exempt(
                crate::bridge::envelope::ExemptReason::Read,
            ),
        }
    }

    #[test]
    fn new_copies_request_wal_lsn_onto_task() {
        // The request envelope is the only Control->Data channel for the
        // allocated write LSN: `ExecutionTask::new` must copy it so the apply
        // chokepoints can record the committed write version.
        let lsn = Lsn::new(4242);
        let task = ExecutionTask::new(request_with_wal_lsn(Some(lsn)));
        assert_eq!(task.wal_lsn(), Some(lsn));
    }

    fn past_deadline_task(admission: crate::bridge::envelope::Admission) -> ExecutionTask {
        ExecutionTask::new(Request {
            deadline: Instant::now() - Duration::from_secs(1),
            admission,
            ..request_with_wal_lsn(None)
        })
    }

    #[test]
    fn already_ordered_task_past_its_deadline_is_not_expired() {
        let task = past_deadline_task(crate::bridge::envelope::Admission::Exempt(
            crate::bridge::envelope::ExemptReason::AlreadyOrdered,
        ));
        assert!(!task.is_expired());
    }

    #[test]
    fn admitted_or_read_task_past_its_deadline_is_expired() {
        let admitted = past_deadline_task(crate::bridge::envelope::Admission::Admitted);
        assert!(admitted.is_expired());
        let read = past_deadline_task(crate::bridge::envelope::Admission::Exempt(
            crate::bridge::envelope::ExemptReason::Read,
        ));
        assert!(read.is_expired());
    }

    #[test]
    fn new_leaves_wal_lsn_none_for_reads() {
        let task = ExecutionTask::new(request_with_wal_lsn(None));
        assert_eq!(task.wal_lsn(), None);
    }

    #[test]
    fn new_copies_request_resolved_now_ms_onto_task() {
        let mut request = request_with_wal_lsn(Some(Lsn::new(1)));
        request.resolved_now_ms = Some(1_000);
        let task = ExecutionTask::new(request);
        assert_eq!(task.resolved_now_ms(), Some(1_000));
    }

    #[test]
    fn with_wal_lsn_leaves_resolved_now_ms_none() {
        let task = ExecutionTask::with_wal_lsn(request_with_wal_lsn(None), Some(Lsn::new(5)));
        assert_eq!(task.resolved_now_ms(), None);
    }
}
