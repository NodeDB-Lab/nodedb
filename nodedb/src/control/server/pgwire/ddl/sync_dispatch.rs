//! Shared async dispatch helper for DDL and DSL handlers.
//!
//! Sends a [`PhysicalPlan`] to the Data Plane and awaits the response.

use std::time::{Duration, Instant};

use crate::bridge::envelope::{PhysicalPlan, Priority, Request, Status};
use crate::control::state::SharedState;
use crate::types::{ReadConsistency, RequestId, TenantId, VShardId};

/// Send `plan` to the Data Plane and await the response.
///
/// This is async — it yields the Tokio thread while waiting, so the
/// response poller can deliver the result without deadlocking.
pub async fn dispatch_async(
    state: &SharedState,
    tenant_id: TenantId,
    collection: &str,
    plan: PhysicalPlan,
    timeout: Duration,
) -> crate::Result<Vec<u8>> {
    let vshard_id = VShardId::from_collection(collection);
    let request_id = RequestId::new(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64,
    );

    let request = Request {
        request_id,
        tenant_id,
        vshard_id,
        plan,
        deadline: Instant::now() + timeout,
        priority: Priority::Normal,
        trace_id: 0,
        consistency: ReadConsistency::Strong,
        idempotency_key: None,
    };

    let rx = state.tracker.register_oneshot(request_id);

    match state.dispatcher.lock() {
        Ok(mut d) => d.dispatch(request).map_err(|e| crate::Error::Internal {
            detail: e.to_string(),
        })?,
        Err(p) => p
            .into_inner()
            .dispatch(request)
            .map_err(|e| crate::Error::Internal {
                detail: e.to_string(),
            })?,
    };

    // Await with timeout — yields the thread so the response poller can run.
    let resp = tokio::time::timeout(timeout, rx)
        .await
        .map_err(|_| crate::Error::Internal {
            detail: format!("dispatch timeout after {}ms", timeout.as_millis()),
        })?
        .map_err(|_| crate::Error::Internal {
            detail: "response channel closed".into(),
        })?;

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
