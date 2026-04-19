//! Opaque session handle HTTP endpoint.
//!
//! ```text
//! POST /api/auth/session
//! Authorization: Bearer <jwt-or-api-key>
//!
//! Response: { "session_id": "nds_...", "expires_in": 3600 }
//! ```
//!
//! The returned `session_id` can be used with pgwire connection poolers:
//! ```text
//! SET LOCAL nodedb.auth_session = 'nds_...';
//! SELECT * FROM orders;  -- Uses the cached AuthContext from the session handle
//! ```

use std::net::SocketAddr;

use axum::extract::{ConnectInfo, State};
use axum::http::HeaderMap;
use axum::response::IntoResponse;

use crate::control::security::session_handle::ClientFingerprint;

use super::super::auth::{ApiError, AppState, resolve_auth};

/// `POST /api/auth/session` — Create an opaque session handle.
///
/// Validates the bearer token (JWT or API key), creates a server-side
/// cached `AuthContext`, and returns a UUID handle the client can use
/// with `SET LOCAL nodedb.auth_session = '<handle>'` on pgwire connections.
///
/// The caller's `(tenant_id, peer IP)` is captured as a `ClientFingerprint`
/// and bound to the handle; subsequent resolves from a different origin are
/// rejected per the configured `FingerprintMode`.
pub async fn create_session(
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    State(state): State<AppState>,
) -> Result<impl IntoResponse, ApiError> {
    let (identity, auth_ctx) = resolve_auth(&headers, &state, "http")?;

    let fingerprint = ClientFingerprint::from_peer(identity.tenant_id, &peer);
    let handle = state.shared.session_handles.create(auth_ctx, fingerprint);

    Ok(axum::Json(serde_json::json!({
        "session_id": handle,
        "expires_in": 3600,
    })))
}

/// `DELETE /api/auth/session` — Invalidate a session handle.
///
/// ```text
/// DELETE /api/auth/session
/// X-Session-Id: nds_...
/// ```
pub async fn delete_session(
    headers: HeaderMap,
    State(state): State<AppState>,
) -> Result<impl IntoResponse, ApiError> {
    let handle = headers
        .get("x-session-id")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| ApiError::BadRequest("missing X-Session-Id header".into()))?;

    let found = state.shared.session_handles.invalidate(handle);
    if !found {
        return Err(ApiError::BadRequest("session handle not found".into()));
    }

    Ok(axum::Json(serde_json::json!({ "status": "ok" })))
}
