// SPDX-License-Identifier: BUSL-1.1

//! Control -> Data request envelope, plus the admission decision it carries.

use std::sync::Arc;
use std::time::Instant;

use nodedb_physical::physical_plan::PhysicalPlan;

use super::status::Priority;
use crate::event::types::EventSource;
use crate::types::{
    DatabaseId, Lsn, ReadConsistency, RequestId, TenantId, TraceId, TxnId, VShardId,
};

/// Request envelope: Control Plane -> Data Plane.
///
/// Every field is mandatory.
#[derive(Debug, Clone)]
pub struct Request {
    /// Globally unique request identifier (monotonic per connection).
    pub request_id: RequestId,

    /// Tenant scope — all data access is tenant-scoped by construction.
    pub tenant_id: TenantId,

    /// Database scope — identifies which catalog namespace this request targets.
    /// `DatabaseId::DEFAULT` (0) is the built-in `default` database.
    pub database_id: DatabaseId,

    /// Target virtual shard.
    pub vshard_id: VShardId,

    /// Opaque plan digest identifying the physical operation to execute.
    pub plan: PhysicalPlan,

    /// Absolute deadline. The Data Plane reads it only through
    /// [`Request::execution_deadline`], and stops at the next safe point after
    /// that deadline passes. Already-ordered work has no execution deadline.
    pub deadline: Instant,

    /// Request priority for scheduling on the Data Plane.
    pub priority: Priority,

    /// Distributed trace identifier for cross-plane observability.
    pub trace_id: TraceId,

    /// Read consistency level for this request.
    pub consistency: ReadConsistency,

    /// Optional idempotency key for non-idempotent writes.
    /// If present, the Data Plane deduplicates by skipping execution
    /// when the same key has already been processed (returns the
    /// cached response status).
    pub idempotency_key: Option<u64>,

    /// Origin of this DML request. Propagated to the Data Plane so that
    /// emitted WriteEvents carry the correct source tag. Trigger-generated
    /// writes use `EventSource::Trigger` to prevent cascade re-triggering.
    pub event_source: EventSource,

    /// Roles held by the authenticated user. Propagated to the Data Plane
    /// for role-guarded state transition enforcement (`BY ROLE 'manager'`).
    /// Empty for system-generated writes (triggers, CRDT sync, etc.).
    pub user_roles: Vec<String>,

    /// Authenticated user ID. Propagated to WriteEvents for DML audit attribution.
    /// `None` for system-generated writes (triggers, CRDT sync, Raft follower).
    pub user_id: Option<Arc<str>>,

    /// SQL plan digest identifying the statement that produced this request.
    /// Reuses the plan digest already computed by nodedb-sql. `None` for
    /// non-user writes.
    pub statement_digest: Option<Arc<str>>,

    /// Set when this write originates inside a session transaction block;
    /// keys the per-transaction staging overlay. `None` for autocommit /
    /// non-transactional / system requests.
    pub txn_id: Option<TxnId>,

    /// WAL LSN the Control Plane allocated for this write at wal-dispatch time.
    /// The committed write-LSN is part of the cross-plane write contract: the
    /// Data Plane copies it onto the [`ExecutionTask`] so the apply chokepoint
    /// records the per-key / per-collection write version (see
    /// `data::executor::core_loop::write_index`). `None` for reads, control
    /// ops, and writes whose LSN is not (yet) threaded — the version index is
    /// skipped rather than advanced with a wrong value.
    ///
    /// [`ExecutionTask`]: crate::data::executor::task::ExecutionTask
    pub wal_lsn: Option<Lsn>,

    /// Wall-clock instant (ms since epoch) the Control Plane resolved at
    /// WAL-append time for a TTL-bearing KV write. The durable WAL record and
    /// the live Data-Plane apply MUST use this same instant for `expire_at_ms`
    /// — resolving it independently at apply time would let live state
    /// disagree with the durable record by the dispatch latency, and on a
    /// crash-then-replay, replay would recompute `now_ms` at restart time
    /// instead of installing the original instant, pushing the TTL forward by
    /// the crash-to-restart delay. `None` for reads, non-TTL writes, and
    /// writes whose resolved instant is not (yet) threaded — the live apply
    /// falls back to `epoch_system_ms` (Calvin) or the wall clock, same as
    /// before this field existed.
    pub resolved_now_ms: Option<u64>,

    /// Write-admission decision for this request.
    ///
    /// Every write-class [`PhysicalPlan`] MUST pass the neutral write-admission
    /// gate (`crate::control::server::shared::write_admission`) before it is
    /// enqueued to a Data-Plane core; the gate stamps [`Admission::Admitted`].
    /// Requests that do not re-enter the gate carry [`Admission::Exempt`] with
    /// an [`ExemptReason`] — [`ExemptReason::Read`] for reads / savepoint /
    /// overlay meta ops, [`ExemptReason::AlreadyOrdered`] for writes already
    /// serialized elsewhere (Calvin-scheduled applies, Raft-follower / replay /
    /// clone / checkpoint).
    ///
    /// The field is REQUIRED (no `Default`, no `#[serde(default)]`) so every
    /// `Request` construction site makes an explicit choice — that is the
    /// write-ingress completeness enforcement. The SPSC enqueue chokepoint
    /// (`crate::bridge::dispatch`) asserts no write-class plan reaches a core
    /// with the decision unmade.
    pub admission: Admission,
}

impl Request {
    /// The deadline the Data Plane enforces while it runs this request.
    ///
    /// Returns `None` for [`ExemptReason::AlreadyOrdered`] work: Calvin
    /// applies, replicated applies, replay, clone, and checkpoint. Their order
    /// is already fixed and other replicas apply the same work. Such work has
    /// no refusal outcome, so every replica must run it to completion. A
    /// replica that drops it on a deadline diverges from the others.
    ///
    /// Returns `Some(self.deadline)` for every other request. Every Data-Plane
    /// deadline check reads this method, never the raw field.
    pub fn execution_deadline(&self) -> Option<Instant> {
        match self.admission {
            Admission::Exempt(ExemptReason::AlreadyOrdered) => None,
            Admission::Admitted | Admission::Exempt(ExemptReason::Read) => Some(self.deadline),
        }
    }
}

/// Write-admission marker carried by every [`Request`].
///
/// A write-class plan becomes [`Admission::Admitted`] only by passing the
/// neutral write-admission gate. Everything that does not re-enter the gate's
/// OCC fence carries [`Admission::Exempt`] with an explicit [`ExemptReason`].
/// There is intentionally no "unresolved" variant: the required field makes an
/// unmade decision unrepresentable, so a missed write path is a compile error
/// at the construction site rather than a silent serializability hole at
/// runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Admission {
    /// Passed the write-admission gate.
    Admitted,
    /// Does not re-enter the write-admission gate — either a non-write, or a
    /// write whose ordering was already decided elsewhere. The [`ExemptReason`]
    /// records which, so the SPSC chokepoint can tell a legitimately exempt
    /// write apart from a base-state write that bypassed the gate.
    Exempt(ExemptReason),
}

/// Why a [`Request`] is exempt from the write-admission gate.
///
/// The distinction is load-bearing at the SPSC chokepoint: a write-class plan
/// marked [`ExemptReason::Read`] is a bug (a write that bypassed the gate),
/// whereas [`ExemptReason::AlreadyOrdered`] is a legitimately exempt write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExemptReason {
    /// The plan is not a base-state write — a read / query, or an overlay /
    /// savepoint meta-op. It never needs the write fence.
    Read,
    /// A write-class plan whose ordering was ALREADY decided elsewhere and does
    /// NOT re-enter the OCC fence: Calvin-scheduler applies (the scheduler
    /// already holds the locks), replicated / Raft-follower applies, recovery /
    /// replay, clone / copy-up materialization, and checkpoint. These are
    /// legitimately exempt writes.
    AlreadyOrdered,
}

impl Admission {
    /// Whether this marker is [`Admission::Exempt`] with reason
    /// [`ExemptReason::Read`] — i.e. claims the plan is not a base-state write.
    ///
    /// The SPSC chokepoint uses this to catch a write-class plan wrongly marked
    /// exempt-as-read: such a plan bypassed the write-admission gate.
    pub fn is_exempt_as_read(&self) -> bool {
        matches!(self, Admission::Exempt(ExemptReason::Read))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{DatabaseId, RequestId, TenantId, TraceId, VShardId};
    use nodedb_physical::physical_plan::{DocumentOp, MetaOp};
    use std::time::Duration;

    fn sample_request() -> Request {
        Request {
            request_id: RequestId::new(1),
            tenant_id: TenantId::new(1),
            database_id: DatabaseId::DEFAULT,
            vshard_id: VShardId::new(0),
            plan: PhysicalPlan::Document(DocumentOp::PointGet {
                collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, "users"),
                document_id: "doc-1".into(),
                surrogate: nodedb_types::Surrogate::ZERO,
                pk_bytes: Vec::new(),
                rls_filters: Vec::new(),
                system_time: nodedb_types::SystemTimeScope::Current,
                valid_at_ms: None,
            }),
            deadline: Instant::now() + Duration::from_secs(5),
            priority: Priority::Normal,
            trace_id: TraceId::generate(),
            consistency: ReadConsistency::Strong,
            idempotency_key: None,
            event_source: EventSource::User,
            user_roles: Vec::new(),
            user_id: None,
            statement_digest: None,
            txn_id: None,
            wal_lsn: None,
            resolved_now_ms: None,
            admission: Admission::Exempt(ExemptReason::Read),
        }
    }

    #[test]
    fn request_fields_accessible() {
        let req = sample_request();
        assert_eq!(req.request_id, RequestId::new(1));
        assert_eq!(req.tenant_id, TenantId::new(1));
        assert_ne!(req.trace_id, TraceId::ZERO);
    }

    #[test]
    fn already_ordered_request_has_no_execution_deadline() {
        let req = Request {
            admission: Admission::Exempt(ExemptReason::AlreadyOrdered),
            ..sample_request()
        };
        assert_eq!(req.execution_deadline(), None);
    }

    #[test]
    fn admitted_and_read_requests_keep_their_deadline() {
        for admission in [Admission::Admitted, Admission::Exempt(ExemptReason::Read)] {
            let req = Request {
                admission,
                ..sample_request()
            };
            assert_eq!(req.execution_deadline(), Some(req.deadline));
        }
    }

    #[test]
    fn cancel_plan() {
        let req = Request {
            request_id: RequestId::new(99),
            tenant_id: TenantId::new(1),
            database_id: DatabaseId::DEFAULT,
            vshard_id: VShardId::new(0),
            plan: PhysicalPlan::Meta(MetaOp::Cancel {
                target_request_id: RequestId::new(42),
            }),
            deadline: Instant::now() + Duration::from_secs(1),
            priority: Priority::Critical,
            trace_id: TraceId::ZERO,
            consistency: ReadConsistency::Eventual,
            idempotency_key: None,
            event_source: EventSource::User,
            user_roles: Vec::new(),
            user_id: None,
            statement_digest: None,
            txn_id: None,
            wal_lsn: None,
            resolved_now_ms: None,
            admission: Admission::Exempt(ExemptReason::Read),
        };
        match req.plan {
            PhysicalPlan::Meta(MetaOp::Cancel { target_request_id }) => {
                assert_eq!(target_request_id, RequestId::new(42));
            }
            _ => panic!("expected Cancel plan"),
        }
    }
}
