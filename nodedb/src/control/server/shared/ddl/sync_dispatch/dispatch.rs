// SPDX-License-Identifier: BUSL-1.1

//! Async Data-Plane dispatch for system-initiated and authorized work.

use std::time::{Duration, Instant};

use crate::bridge::envelope::{PhysicalPlan, Priority, Request, Response};
use crate::control::server::shared::clone_write::CloneCheckedTask;
use crate::control::server::shared::response_payload::payload_or_typed_error;
use crate::control::server::shared::session::statement_deadline;
use crate::control::server::shared::write_admission::plan_is_write;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, ReadConsistency, TenantId, TraceId, VShardId};

use super::system_task::SystemTask;

/// Send system-initiated work to the Data Plane and await its payload.
///
/// This is async — it yields the Tokio thread while waiting, so the response
/// poller can deliver the result without deadlocking.
///
/// A Data-Plane refusal returns `crate::Error::DataPlane` with the response's
/// own [`crate::bridge::envelope::ErrorCode`]. Each protocol renders that code
/// through its SQLSTATE or status table. Only a refusal with no code is
/// `crate::Error::Internal`.
pub(crate) async fn dispatch_system(
    state: &SharedState,
    task: SystemTask<'_>,
    timeout: Duration,
) -> crate::Result<Vec<u8>> {
    let event_source = task.reason.event_source();
    let resp = dispatch_system_response_with_source(state, task, timeout, event_source).await?;

    // A system task never advances the tenant's observed write-HLC. Its
    // `SystemReason` states that no client asked for the work, and RESTORE's
    // staleness gate counts only user data writes.
    payload_or_typed_error(resp)
}

/// Send system-initiated work and await the full [`Response`], preserving the
/// typed [`crate::bridge::envelope::ErrorCode`] on a non-`Ok` status instead of
/// flattening it to a string.
///
/// Infrastructure failures (dispatch, timeout, channel close) surface as typed
/// `Error` variants. Callers that inspect the whole response (the CRDT sync
/// delta path, CONVERT) use this. [`dispatch_system`] wraps it and returns the
/// payload or the typed refusal. This function does **not** advance the
/// tenant write-HLC — the caller does that on its own success path.
pub(crate) async fn dispatch_system_response_with_source(
    state: &SharedState,
    task: SystemTask<'_>,
    timeout: Duration,
    event_source: crate::event::EventSource,
) -> crate::Result<Response> {
    let vshard_id = task.collection.vshard();
    tracing::trace!(
        reason = task.reason.label(),
        collection = task.collection.name(),
        "system-initiated data plane dispatch"
    );
    dispatch_plan(
        state,
        PlanDispatch {
            tenant_id: task.tenant_id,
            database_id: task.collection.database_id(),
            vshard_id,
            plan: task.plan,
            timeout,
            event_source,
        },
    )
    .await
}

/// Send clone-checked, already-authorized work to the Data Plane and await its
/// payload.
///
/// A Data-Plane refusal returns `crate::Error::DataPlane` with its own code,
/// the same shape [`dispatch_system`] returns.
///
/// The capability is consumed here: the plan that reaches storage is the plan
/// authorization approved, so a caller cannot authorize one shape and dispatch
/// another. Client-reachable paths use this rather than the system door.
///
/// Taking a [`CloneCheckedTask`] rather than a bare `AuthorizedTask` is what
/// makes the clone hooks unskippable on this door: only
/// [`intercept_and_authorize`](crate::control::server::shared::clone_write::intercept_and_authorize)
/// mints that type, so an entry point that authorizes without running clone-write
/// and clone-read interception fails to compile instead of silently reading a
/// `Shadowed` clone target-locally.
///
/// `collection` is the bare catalog name in the task's database.
pub(crate) async fn dispatch_authorized(
    state: &SharedState,
    checked: CloneCheckedTask,
    collection: &str,
    timeout: Duration,
    linearizable: bool,
) -> crate::Result<Vec<u8>> {
    // The write lease lives until the dispatch returns its outcome.
    let (authorized, _lease) = checked.into_parts();
    let task = authorized.into_physical_task();
    let vshard_id = nodedb_types::CollectionKey::from_bare(task.database_id, collection).vshard();
    let tenant_id = task.tenant_id;
    // A read runs where the collection's owning group serves it: here when
    // this node replicates the group, on its leader otherwise. A linearizable
    // read is confirmed where it runs (`dispatch_utils::owner_read`).
    let plan = if plan_is_write(&task.plan) {
        task.plan
    } else {
        use crate::control::server::dispatch_utils::{OwnedRead, OwnedReadScope, route_owned_read};
        let scope = OwnedReadScope {
            tenant_id,
            database_id: task.database_id,
            vshard_id,
            trace_id: crate::types::TraceId::ZERO,
            txn_id: None,
            linearizable,
        };
        let plan = match route_owned_read(state, scope, task.plan).await? {
            OwnedRead::Local(plan) => *plan,
            OwnedRead::Served(resp) => return payload_or_typed_error(resp),
        };
        // A RAG fusion's legs live on different nodes in a cluster: it runs
        // in stages from here (`graph_dispatch::rag_fusion`).
        if let Some(served) = crate::control::server::graph_dispatch::serve_rag_plan(
            state,
            tenant_id,
            task.database_id,
            &plan,
            linearizable,
        )
        .await
        {
            return served.and_then(payload_or_typed_error);
        }
        plan
    };
    let resp = dispatch_plan(
        state,
        PlanDispatch {
            tenant_id,
            database_id: task.database_id,
            vshard_id,
            plan,
            timeout,
            event_source: crate::event::EventSource::User,
        },
    )
    .await?;

    payload_or_typed_error(resp)
}

/// What [`dispatch_plan`] sends, and where.
struct PlanDispatch {
    tenant_id: TenantId,
    database_id: DatabaseId,
    vshard_id: VShardId,
    plan: PhysicalPlan,
    timeout: Duration,
    event_source: crate::event::EventSource,
}

/// Shared transport: build the request envelope, dispatch, await the response.
async fn dispatch_plan(state: &SharedState, dispatch: PlanDispatch) -> crate::Result<Response> {
    let PlanDispatch {
        tenant_id,
        database_id,
        vshard_id,
        plan,
        timeout,
        event_source,
    } = dispatch;
    let request_id = state.next_request_id();

    // Whichever comes first: the running statement's deadline, or the caller's
    // own timeout. A DDL statement a session bounded with `statement_timeout`
    // stops with the statement; a background caller with no statement scope
    // keeps exactly the timeout it asked for.
    let deadline = statement_deadline(state.tuning.network.default_deadline_secs)
        .min(Instant::now() + timeout);

    let request = Request {
        request_id,
        tenant_id,
        database_id,
        vshard_id,
        plan,
        deadline,
        priority: Priority::Normal,
        trace_id: TraceId::generate(),
        consistency: ReadConsistency::Strong,
        idempotency_key: None,
        event_source,
        user_roles: Vec::new(),
        user_id: None,
        statement_digest: None,
        txn_id: None,
        wal_lsn: None,
        resolved_now_ms: None,
        commit_hlc: None,
        entry_version: None,
        admission: crate::bridge::envelope::Admission::Exempt(
            crate::bridge::envelope::ExemptReason::AlreadyOrdered,
        ),
    };

    let mut rx = state.tracker.register(request_id);

    let dispatched = match state.dispatcher.lock() {
        Ok(mut d) => d.dispatch(request),
        Err(p) => p.into_inner().dispatch(request),
    };
    if let Err(error) = dispatched {
        // No response will arrive, and no core applied the plan.
        state.tracker.cancel(&request_id);
        return Err(crate::Error::Internal {
            detail: error.to_string(),
        });
    }
    // Await to the same instant the envelope carries — yields the thread so the
    // response poller can run. Reaching that instant is the statement running
    // out of time, so it reports the deadline, and so does a producer that
    // stopped after it: the closure there is the symptom, not the cause.
    let received =
        tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), rx.recv()).await;
    match received {
        Ok(Some(response)) => Ok(response),
        Ok(None) => Err(closed_error(request_id, deadline)),
        Err(_) => Err(crate::Error::DeadlineExceeded { request_id }),
    }
}

/// The error for a response channel that closed before a response: the
/// deadline when it already passed, since the closure is then its symptom.
fn closed_error(request_id: crate::types::RequestId, deadline: Instant) -> crate::Error {
    if Instant::now() >= deadline {
        crate::Error::DeadlineExceeded { request_id }
    } else {
        crate::Error::Internal {
            detail: "response channel closed".into(),
        }
    }
}
