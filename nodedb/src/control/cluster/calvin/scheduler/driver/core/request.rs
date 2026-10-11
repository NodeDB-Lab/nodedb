// SPDX-License-Identifier: BUSL-1.1

//! Shared `Request` construction for already-sequenced Calvin sub-operations.

use std::time::{Duration, Instant};

use super::scheduler::Scheduler;
use crate::bridge::envelope::{Admission, ExemptReason, Priority, Request};
use crate::control::cluster::calvin::scheduler::lock_manager::TxnId;
use crate::types::{DatabaseId, ReadConsistency, RequestId, TenantId, VShardId};
use nodedb_physical::physical_plan::PhysicalPlan;

/// The event source a Calvin sub-operation runs with when its transaction
/// names none: a client transaction's.
pub(in crate::control::cluster::calvin::scheduler::driver::core) const CALVIN_EVENT_SOURCE:
    crate::event::EventSource = crate::event::EventSource::User;

/// The event source a sequenced transaction's writes carry. Its redo entry
/// carries it, so every replica's install and WAL replay emit the same
/// events, and a server-run body's writes fire no triggers.
pub(in crate::control::cluster::calvin::scheduler::driver::core) fn txn_event_source(
    tx_class: &nodedb_cluster::calvin::types::TxClass,
) -> crate::event::EventSource {
    crate::event::EventSource::from_wal_code(tx_class.event_source).unwrap_or(CALVIN_EVENT_SOURCE)
}

/// The event source this vShard's slice of a sequenced transaction commits
/// under. A slice only trigger bodies wrote, in a transaction a client also
/// wrote, takes the source a body's row takes in one overlay. Every other
/// slice takes the transaction's source. The stage and the redo entry both
/// carry it.
pub(in crate::control::cluster::calvin::scheduler::driver::core) fn slice_event_source(
    tx_class: &nodedb_cluster::calvin::types::TxClass,
    scope: &super::super::types::SliceScope,
) -> crate::event::EventSource {
    let source = txn_event_source(tx_class);
    if scope.body_only {
        source.committed_row_override(crate::event::EventSource::Trigger)
    } else {
        source
    }
}

impl Scheduler {
    /// Builds a `Request` for an already-sequenced Calvin sub-operation.
    ///
    /// Calvin has already globally ordered the transaction, so the request is
    /// admitted `Exempt(AlreadyOrdered)` — it bypasses admission control. Only
    /// `request_id`, `tenant_id`, `database_id` and `plan` vary between call
    /// sites; everything else (vshard, deadline, and the fixed metadata
    /// constants) is derived from `&self` or constant. No scheduler request
    /// writes a WAL record, so none carries a WAL LSN.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn build_exempt_request(
        &self,
        request_id: RequestId,
        tenant_id: TenantId,
        database_id: DatabaseId,
        plan: PhysicalPlan,
    ) -> Request {
        Request {
            request_id,
            tenant_id,
            database_id,
            vshard_id: VShardId::new(self.vshard_id),
            plan,
            deadline: self.request_deadline(),
            priority: Priority::Normal,
            trace_id: nodedb_types::TraceId([0u8; 16]),
            consistency: ReadConsistency::Strong,
            idempotency_key: None,
            event_source: CALVIN_EVENT_SOURCE,
            user_roles: Vec::new(),
            user_id: None,
            statement_digest: None,
            txn_id: None,
            wal_lsn: None,
            resolved_now_ms: None,
            commit_hlc: None,
            entry_version: None,
            admission: Admission::Exempt(ExemptReason::AlreadyOrdered),
        }
    }

    /// The deadline for a Calvin sub-operation sent now: one epoch duration
    /// times the configured deadline multiplier.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn request_deadline(
        &self,
    ) -> Instant {
        // no-determinism: scheduler deadline controls waiting, not ordered state.
        Instant::now()
            + Duration::from_millis(
                self.config.epoch_duration_ms * u64::from(self.config.txn_deadline_multiplier),
            )
    }

    /// Spawn a bridge task that awaits a single executor response and forwards
    /// it to the scheduler's fan-in completion channel.
    ///
    /// The bridge task is cancel-safe: it holds only a cloned sender and the
    /// per-request receiver. Dropping the scheduler's `completion_rx` causes
    /// the bridge's `send` to fail silently, which is fine on shutdown.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn spawn_response_bridge(
        &self,
        txn_id: TxnId,
        request_id: RequestId,
        mut response_rx: crate::control::ResponseReceiver,
    ) {
        let tx = self.completion_tx.clone();
        tokio::spawn(async move {
            let result = response_rx.recv().await;
            // Ignore send error: scheduler has shut down.
            let _ = tx.send((txn_id, request_id, result)).await;
        });
    }

    /// Allocate a fresh request ID for a dispatch.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn next_request_id(
        &self,
    ) -> RequestId {
        self.shared.next_request_id()
    }
}
