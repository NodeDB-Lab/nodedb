//! Shared HTTP route helpers: request ID extraction, plan dispatch.
//!
//! Used by the CRDT endpoint and any future dedicated endpoints.

use std::time::{Duration, Instant};

use axum::http::HeaderMap;

use crate::bridge::envelope::{PhysicalPlan, Priority, Request, Status};
use crate::control::server::http::auth::{ApiError, AppState};
use crate::types::{ReadConsistency, TenantId, VShardId};

/// Extract X-Request-Id from headers, or generate one.
pub(super) fn extract_request_id(headers: &HeaderMap) -> u64 {
    headers
        .get("x-request-id")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or_else(|| {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as u64
        })
}

/// Dispatch a physical plan and await the response.
pub(super) async fn dispatch_plan(
    state: &AppState,
    tenant_id: TenantId,
    collection: &str,
    plan: PhysicalPlan,
) -> Result<Vec<u8>, ApiError> {
    dispatch_plan_with_trace(state, tenant_id, collection, plan, 0).await
}

/// Dispatch a physical plan with trace ID and await the response.
pub(super) async fn dispatch_plan_with_trace(
    state: &AppState,
    tenant_id: TenantId,
    collection: &str,
    plan: PhysicalPlan,
    trace_id: u64,
) -> Result<Vec<u8>, ApiError> {
    let vshard_id = VShardId::from_collection(collection);
    let request_id = state.shared.next_request_id();

    let request = Request {
        request_id,
        tenant_id,
        vshard_id,
        plan,
        deadline: Instant::now()
            + Duration::from_secs(state.shared.tuning.network.default_deadline_secs),
        priority: Priority::Normal,
        trace_id,
        consistency: ReadConsistency::Strong,
        idempotency_key: None,
        event_source: crate::event::EventSource::User,
        user_roles: Vec::new(),
    };

    let rx = state.shared.tracker.register_oneshot(request_id);

    match state.shared.dispatcher.lock() {
        Ok(mut d) => d
            .dispatch(request)
            .map_err(|e| ApiError::Internal(e.to_string()))?,
        Err(p) => p
            .into_inner()
            .dispatch(request)
            .map_err(|e| ApiError::Internal(e.to_string()))?,
    };

    let resp = tokio::time::timeout(
        Duration::from_secs(state.shared.tuning.network.default_deadline_secs),
        rx,
    )
    .await
    .map_err(|_| ApiError::Internal("request timed out".into()))?
    .map_err(|_| ApiError::Internal("response channel closed".into()))?;

    if resp.status != Status::Ok {
        let detail = if let Some(ref code) = resp.error_code {
            format!("{code:?}")
        } else {
            String::from_utf8_lossy(&resp.payload).into_owned()
        };
        return Err(ApiError::Internal(detail));
    }

    Ok(resp.payload.to_vec())
}
