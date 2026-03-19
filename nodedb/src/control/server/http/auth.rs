//! HTTP API authentication via API key bearer tokens.
//!
//! Extracts `AuthenticatedIdentity` from the `Authorization: Bearer ndb_...` header.
//! Falls back to trust mode if configured.

use std::sync::Arc;

use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};

use crate::config::auth::AuthMode;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::session_auth;
use crate::control::state::SharedState;

/// Application state shared across all HTTP handlers.
#[derive(Clone)]
pub struct AppState {
    pub shared: Arc<SharedState>,
    pub auth_mode: AuthMode,
}

/// Resolve an authenticated identity from HTTP headers.
///
/// Supports:
/// - `Authorization: Bearer ndb_<key_id>_<secret>` — API key auth
/// - Trust mode (no header required) — if configured
pub fn resolve_identity(
    headers: &HeaderMap,
    state: &AppState,
    peer_addr: &str,
) -> Result<AuthenticatedIdentity, ApiError> {
    // Try Authorization header first.
    if let Some(auth_header) = headers.get("authorization") {
        let auth_str = auth_header
            .to_str()
            .map_err(|_| ApiError::Unauthorized("invalid authorization header encoding".into()))?;

        if let Some(token) = auth_str.strip_prefix("Bearer ") {
            let token = token.trim();
            let identity =
                session_auth::verify_api_key_identity(&state.shared, token, peer_addr, "HTTP")
                    .ok_or_else(|| ApiError::Unauthorized("invalid API key".into()))?;

            return Ok(identity);
        }
    }

    // No auth header — check if trust mode.
    if state.auth_mode == AuthMode::Trust {
        return Ok(session_auth::trust_identity(&state.shared, "anonymous"));
    }

    Err(ApiError::Unauthorized(
        "missing Authorization: Bearer <api-key> header".into(),
    ))
}

/// HTTP API error type.
#[derive(Debug)]
pub enum ApiError {
    Unauthorized(String),
    Forbidden(String),
    BadRequest(String),
    Internal(String),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            ApiError::Unauthorized(msg) => (StatusCode::UNAUTHORIZED, msg),
            ApiError::Forbidden(msg) => (StatusCode::FORBIDDEN, msg),
            ApiError::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg),
            ApiError::Internal(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg),
        };
        let body = serde_json::json!({ "error": message });
        (status, axum::Json(body)).into_response()
    }
}
