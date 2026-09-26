// SPDX-License-Identifier: BUSL-1.1

//! Build the wire `Request` and hand it to the Data-Plane dispatcher.

use std::time::Instant;

use tokio::sync::OwnedMutexGuard;

use crate::bridge::envelope::{Admission, PhysicalPlan, Priority, Request};
use crate::control::array_catalog::ddl::AuthorizedDdlTransition;
use crate::control::server::shared::write_admission::WriteAdmissionGuard;
use crate::control::state::SharedState;
use crate::types::{
    DatabaseId, Lsn, ReadConsistency, RequestId, TenantId, TraceId, TxnId, VShardId,
};

use super::wal_append::rollback_on_err;

/// The write-admission guards a dispatched write must hold across the
/// response await — deferred (rather than dropped at enqueue) only when the
/// write's redo is minted post-apply.
pub(super) type DeferredGuards = Option<(Option<WriteAdmissionGuard>, Option<OwnedMutexGuard<()>>)>;

/// Everything [`dispatch_to_data_plane`] needs to build the wire `Request`.
pub(super) struct DispatchTarget {
    pub tenant_id: TenantId,
    pub database_id: DatabaseId,
    pub vshard_id: VShardId,
    pub plan: PhysicalPlan,
    pub deadline: Instant,
    pub trace_id: TraceId,
    pub event_source: crate::event::EventSource,
    pub user_id: Option<std::sync::Arc<str>>,
    pub txn_id: Option<TxnId>,
    pub wal_lsn: Option<Lsn>,
    pub resolved_now_ms: Option<u64>,
    pub admission: Admission,
}

/// What the dispatch phase produced: the id the response is tracked under,
/// the receiver it arrives on, the per-vShard latency timer's start instant,
/// and the write-admission guards deferred across the response await (if
/// any).
pub(super) struct DispatchOutcome {
    pub request_id: RequestId,
    pub rx: crate::control::ResponseReceiver,
    pub dispatch_started: Instant,
    pub deferred_guards: DeferredGuards,
}

/// Build the `Request` envelope, register its response channel, and hand it
/// to the Data-Plane dispatcher — then release the write-admission guards
/// (or defer them, for a write whose redo mints post-apply).
///
/// The per-vShard QPS + latency timer starts here, at the wall-clock moment
/// the request enters dispatch; the caller observes it on every exit path
/// (success, budget over-run, timeout) so the histogram captures the true
/// end-to-end shape of the work routed to this vshard.
///
/// `waits_for_capacity` makes a capacity refusal wait for freed capacity and
/// retry, up to the request's deadline. Every other refusal returns at once.
pub(super) async fn dispatch_to_data_plane(
    shared: &SharedState,
    ddl_transition: &AuthorizedDdlTransition,
    target: DispatchTarget,
    admission_guard: Option<WriteAdmissionGuard>,
    order_guard: Option<OwnedMutexGuard<()>>,
    post_apply_pending: bool,
    waits_for_capacity: bool,
) -> crate::Result<DispatchOutcome> {
    let dispatch_started = Instant::now();

    let request_id = shared.next_request_id();
    let request = Request {
        request_id,
        tenant_id: target.tenant_id,
        database_id: target.database_id,
        vshard_id: target.vshard_id,
        plan: target.plan,
        deadline: target.deadline,
        priority: Priority::Normal,
        trace_id: target.trace_id,
        consistency: ReadConsistency::Strong,
        idempotency_key: None,
        event_source: target.event_source,
        user_roles: Vec::new(),
        user_id: target.user_id,
        statement_digest: None,
        txn_id: target.txn_id,
        wal_lsn: target.wal_lsn,
        resolved_now_ms: target.resolved_now_ms,
        admission: target.admission,
    };

    let rx = shared.tracker.register(request_id);

    let dispatched = if waits_for_capacity {
        dispatch_when_capacity_frees(shared, request).await
    } else {
        match shared.dispatcher.lock() {
            Ok(mut d) => d.dispatch(request),
            Err(poisoned) => poisoned.into_inner().dispatch(request),
        }
    };
    if dispatched.is_err() {
        // No response will ever arrive for a refused request.
        shared.tracker.cancel(&request_id);
    }
    rollback_on_err(shared, ddl_transition, dispatched)?;

    // Release the write-admission guards immediately after the enqueue, before
    // the Data-Plane round-trip. The per-database WFQ is strict FIFO, so once LSN
    // order equals enqueue order the apply order follows from the queue alone;
    // holding the guards across the response await would only serialize same-key
    // throughput needlessly.
    //
    // EXCEPTION — a post-apply-redo write mints its durable redo AFTER apply,
    // from the write-set on the response; the guards MUST stay held across the
    // response collect + that append so two concurrent same-surrogate writes
    // cannot reorder their redo appends. Both guard types are `Send`, so
    // holding them across the `.await` is sound. Moved into an `Option` so the
    // release is a single, unconditional `drop` below regardless of which path
    // took it. (`None` guard slots when no lock manager was registered / for
    // the exempt-read / Calvin / already-ordered cases.)
    let deferred_guards = if post_apply_pending {
        Some((admission_guard, order_guard))
    } else {
        drop(admission_guard);
        drop(order_guard);
        None
    };

    Ok(DispatchOutcome {
        request_id,
        rx,
        dispatch_started,
        deferred_guards,
    })
}

/// Dispatch `request`, waiting for freed capacity after each capacity
/// refusal, until the request's deadline. Returns the last refusal once the
/// deadline passes.
async fn dispatch_when_capacity_frees(shared: &SharedState, request: Request) -> crate::Result<()> {
    let deadline = tokio::time::Instant::from_std(request.deadline);
    let capacity_freed = match shared.dispatcher.lock() {
        Ok(d) => d.capacity_freed(),
        Err(poisoned) => poisoned.into_inner().capacity_freed(),
    };
    let mut request = request;
    loop {
        // Registered before the attempt, so a slot freed between the refusal
        // and the wait still wakes it.
        let freed = capacity_freed.notified();
        tokio::pin!(freed);
        freed.as_mut().enable();
        let attempt = match shared.dispatcher.lock() {
            Ok(mut d) => d.try_dispatch(request),
            Err(poisoned) => poisoned.into_inner().try_dispatch(request),
        };
        let refusal = match attempt {
            Ok(()) => return Ok(()),
            Err(refusal) => *refusal,
        };
        if !matches!(refusal.error, crate::Error::DispatchCapacity { .. }) {
            return Err(refusal.error);
        }
        if tokio::time::timeout_at(deadline, freed).await.is_err() {
            return Err(refusal.error);
        }
        request = refusal.request;
    }
}
