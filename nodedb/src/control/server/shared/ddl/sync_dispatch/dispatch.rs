// SPDX-License-Identifier: BUSL-1.1

//! Async Data-Plane dispatch for system-initiated and authorized work.

use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::bridge::envelope::{PhysicalPlan, Priority, Request, Response, Status};
use crate::control::server::dispatch_utils::{
    Collect, MintedRecords, OwnedResponse, OwnedWait, RecordOwner, await_response_owned,
};
use crate::control::server::shared::clone_write::CloneCheckedTask;
use crate::control::server::shared::session::statement_deadline;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, ReadConsistency, TenantId, TraceId, VShardId};

use super::system_task::SystemTask;

/// Send system-initiated work to the Data Plane and await its payload.
///
/// This is async — it yields the Tokio thread while waiting, so the response
/// poller can deliver the result without deadlocking.
pub(crate) async fn dispatch_system(
    state: &SharedState,
    task: SystemTask<'_>,
    timeout: Duration,
) -> crate::Result<Vec<u8>> {
    let resp =
        dispatch_system_response_with_source(state, task, timeout, crate::event::EventSource::User)
            .await?;

    if resp.status != Status::Ok {
        // DDL/DSL callers receive the flattened message form. Callers that need
        // to classify the Data-Plane rejection by type use
        // `dispatch_system_response_with_source` and inspect `resp.error_code`.
        let detail = resp
            .error_code
            .as_ref()
            .map(|c| format!("{c:?}"))
            .unwrap_or_else(|| String::from_utf8_lossy(&resp.payload).into_owned());
        return Err(crate::Error::Internal { detail });
    }

    // A system task never advances the tenant's observed write-HLC. Its
    // `SystemReason` states that no client asked for the work, and RESTORE's
    // staleness gate counts only user data writes.
    Ok(resp.payload.to_vec())
}

/// Send system-initiated work and await the full [`Response`], preserving the
/// typed [`crate::bridge::envelope::ErrorCode`] on a non-`Ok` status instead of
/// flattening it to a string.
///
/// Infrastructure failures (dispatch, timeout, channel close) still surface as
/// typed `Error` variants. Callers that must classify a Data-Plane rejection by
/// type (e.g. the CRDT sync delta path) use this and inspect `resp.error_code`;
/// [`dispatch_system`] wraps this and flattens the code to a message
/// for DDL/DSL callers. This function does **not** advance the tenant write-HLC
/// — the caller does that on its own success path.
pub(crate) async fn dispatch_system_response_with_source(
    state: &SharedState,
    task: SystemTask<'_>,
    timeout: Duration,
    event_source: crate::event::EventSource,
) -> crate::Result<Response> {
    let vshard_id = VShardId::from_collection_in_database(task.database_id, task.collection);
    tracing::trace!(
        reason = task.reason.label(),
        collection = task.collection,
        "system-initiated data plane dispatch"
    );
    dispatch_plan(
        state,
        PlanDispatch {
            tenant_id: task.tenant_id,
            database_id: task.database_id,
            vshard_id,
            plan: task.plan,
            timeout,
            event_source,
            minted: task.minted,
        },
    )
    .await
}

/// Send clone-checked, already-authorized work to the Data Plane and await its
/// payload.
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
pub(crate) async fn dispatch_authorized(
    state: &SharedState,
    checked: CloneCheckedTask,
    collection: &str,
    timeout: Duration,
) -> crate::Result<Vec<u8>> {
    let task = checked.into_authorized().into_physical_task();
    let vshard_id = VShardId::from_collection_in_database(task.database_id, collection);
    let tenant_id = task.tenant_id;
    let resp = dispatch_plan(
        state,
        PlanDispatch {
            tenant_id,
            database_id: task.database_id,
            vshard_id,
            plan: task.plan,
            timeout,
            event_source: crate::event::EventSource::User,
            minted: None,
        },
    )
    .await?;

    if resp.status != Status::Ok {
        let detail = resp
            .error_code
            .as_ref()
            .map(|c| format!("{c:?}"))
            .unwrap_or_else(|| String::from_utf8_lossy(&resp.payload).into_owned());
        return Err(crate::Error::Internal { detail });
    }

    Ok(resp.payload.to_vec())
}

/// What [`dispatch_plan`] sends, and where.
struct PlanDispatch {
    tenant_id: TenantId,
    database_id: DatabaseId,
    vshard_id: VShardId,
    plan: PhysicalPlan,
    timeout: Duration,
    event_source: crate::event::EventSource,
    /// Records the caller appended for this plan, under their outcome-floor
    /// window. The transport closes the window from the plan's outcome.
    minted: Option<MintedRecords>,
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
        minted,
    } = dispatch;
    let owner = RecordOwner {
        tenant_id,
        database_id,
        vshard_id,
    };
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
        if let Some(minted) = minted {
            minted.cancel(&state.wal, owner, 0).await?;
        }
        return Err(crate::Error::Internal {
            detail: error.to_string(),
        });
    }
    // A core holds the request now. From here the records close from its
    // final response, never from a drop.
    if let Some(minted) = &minted {
        minted.mark_sent();
    }

    // Await to the same instant the envelope carries — yields the thread so the
    // response poller can run. Reaching that instant is the statement running
    // out of time, so it reports the deadline, and so does a producer that
    // stopped after it: the closure there is the symptom, not the cause.
    let Some(minted) = minted else {
        let received =
            tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), rx.recv()).await;
        return match received {
            Ok(Some(response)) => Ok(response),
            Ok(None) => Err(closed_error(request_id, deadline)),
            Err(_) => Err(crate::Error::DeadlineExceeded { request_id }),
        };
    };
    // The records close in a task this future does not own, so a caller
    // dropped mid-wait still closes them. A late refusal still cancels them.
    let outcome = await_response_owned(
        OwnedWait {
            wal: Arc::clone(&state.wal),
            owner,
            final_refusal_key: 0,
            deadline,
            collect: Collect::First,
        },
        rx,
        minted,
    )
    .await?;
    match outcome {
        OwnedResponse::Answered { response, closed } => {
            closed?;
            Ok(response)
        }
        OwnedResponse::ChannelClosed => Err(closed_error(request_id, deadline)),
        OwnedResponse::DeadlineExceeded => Err(crate::Error::DeadlineExceeded { request_id }),
        OwnedResponse::OverBudget { bytes } => Err(crate::Error::ExecutionLimitExceeded {
            detail: format!("system task response exceeded its byte budget ({bytes} bytes)"),
        }),
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
