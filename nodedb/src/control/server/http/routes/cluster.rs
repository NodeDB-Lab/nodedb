//! Cluster observability endpoint.
//!
//! `GET /cluster/status` returns a full JSON snapshot of the
//! cluster's observability surface — lifecycle phase, every known
//! peer, every Raft group hosted on this node — sourced from the
//! `ClusterObserver` published by `control::cluster::start_raft`.
//!
//! In single-node mode (no `[cluster]` config) the endpoint returns
//! `503 Service Unavailable` with a short JSON error body so clients
//! can distinguish "cluster mode disabled" from "cluster mode broken".

use axum::extract::State;
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};

use super::super::auth::AppState;

/// `GET /cluster/status` — full observability snapshot.
pub async fn cluster_status(State(state): State<AppState>) -> Response {
    match state.shared.cluster_observer.get() {
        Some(observer) => {
            let snap = observer.snapshot();
            match sonic_rs::to_string(&snap) {
                Ok(body) => json_response(StatusCode::OK, body),
                Err(e) => {
                    tracing::warn!(error = %e, "cluster snapshot serialization failed");
                    json_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        r#"{"error":"snapshot serialization failed"}"#.to_string(),
                    )
                }
            }
        }
        None => json_response(
            StatusCode::SERVICE_UNAVAILABLE,
            r#"{"error":"cluster mode not enabled","detail":"this node is running in single-node mode; /cluster/status requires a [cluster] config section"}"#
                .to_string(),
        ),
    }
}

/// Build a JSON response with the given status and pre-serialised
/// body. Centralised so every branch uses the same content-type and
/// so `axum::Json` (which calls `serde_json::to_vec` internally) is
/// not on any hot path — runtime JSON is owned by `sonic_rs` per
/// CLAUDE.md.
fn json_response(status: StatusCode, body: String) -> Response {
    (status, [(header::CONTENT_TYPE, "application/json")], body).into_response()
}
