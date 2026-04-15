//! Health check endpoints.

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use serde_json::json;

use super::super::auth::AppState;

/// GET /healthz — k8s-style readiness/liveness probe.
///
/// Returns `200 OK` when the node has reached `GatewayEnable` and is
/// serving traffic. Returns `503 Service Unavailable` during startup or if
/// startup has failed. This endpoint bypasses the startup gate middleware
/// and is always reachable, making it suitable as a k8s readiness probe.
pub async fn healthz(State(state): State<AppState>) -> impl IntoResponse {
    let health = crate::control::startup::health::observe(&state.shared.startup);
    let (status, body) = crate::control::startup::health::to_http_response(&health);
    (status, axum::Json(body))
}

/// GET /health — liveness check.
pub async fn health(State(state): State<AppState>) -> impl IntoResponse {
    // Derive both the node count and version view from the live
    // cluster topology in one read. Single-node mode reports 1
    // via the view's fallback.
    let view = state.shared.cluster_version_view();
    let nodes = if view.node_count > 0 {
        view.node_count
    } else {
        1
    };
    let cluster_version = json!({
        "nodes": nodes,
        "min_version": view.min_version,
        "max_version": view.max_version,
        "mixed_version": view.is_mixed_version(),
        "compat_mode": crate::control::rolling_upgrade::should_compat_mode(&view),
    });
    let body = json!({
        "status": "ok",
        "node_id": state.shared.node_id,
        "cluster_version": cluster_version,
    });
    (StatusCode::OK, axum::Json(body))
}

/// GET /health/ready — readiness check (WAL recovered, cores initialized).
pub async fn ready(State(state): State<AppState>) -> impl IntoResponse {
    let wal_ready = state.shared.wal.next_lsn().as_u64() > 0;
    let status = if wal_ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    let body = json!({
        "status": if wal_ready { "ready" } else { "not_ready" },
        "wal_lsn": state.shared.wal.next_lsn().as_u64(),
        "node_id": state.shared.node_id,
    });
    (status, axum::Json(body))
}
