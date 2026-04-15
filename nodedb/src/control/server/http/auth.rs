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
    /// DataFusion query context for SQL planning (Send + Sync).
    pub query_ctx: Arc<crate::control::planner::context::QueryContext>,
}

/// Try to validate a Bearer token as a JWT via the JWKS registry.
///
/// Returns `Some(identity)` if the token is a valid JWT with 2 dots and
/// the registry verifies the signature. Returns `None` otherwise.
fn try_validate_jwt(state: &AppState, token: &str) -> Option<AuthenticatedIdentity> {
    if token.matches('.').count() == 2
        && let Some(ref registry) = state.shared.jwks_registry
        && let Ok(identity) = tokio::runtime::Handle::current().block_on(registry.validate(token))
    {
        Some(identity)
    } else {
        None
    }
}

/// Resolve an authenticated identity from HTTP headers.
///
/// Authentication order:
/// 1. `Authorization: Bearer eyJ...` — JWT (if JwksRegistry configured)
/// 2. `Authorization: Bearer ndb_...` — API key
/// 3. Trust mode (no header required) — if configured
pub fn resolve_identity(
    headers: &HeaderMap,
    state: &AppState,
    peer_addr: &str,
) -> Result<AuthenticatedIdentity, ApiError> {
    if let Some(auth_header) = headers.get("authorization") {
        let auth_str = auth_header
            .to_str()
            .map_err(|_| ApiError::Unauthorized("invalid authorization header encoding".into()))?;

        if let Some(token) = auth_str.strip_prefix("Bearer ") {
            let token = token.trim();

            // Try JWT first (token has 2 dots = JWT format).
            if let Some(identity) = try_validate_jwt(state, token) {
                return Ok(identity);
            }

            // Try API key.
            if let Some(identity) =
                session_auth::verify_api_key_identity(&state.shared, token, peer_addr, "HTTP")
            {
                return Ok(identity);
            }

            return Err(ApiError::Unauthorized("invalid bearer token".into()));
        }
    }

    if state.auth_mode == AuthMode::Trust {
        return Ok(session_auth::trust_identity(&state.shared, "anonymous"));
    }

    Err(ApiError::Unauthorized(
        "missing Authorization: Bearer <token> header".into(),
    ))
}

/// Resolve both authenticated identity and auth context from HTTP headers.
///
/// Uses `AuthContext::from_jwt()` when JWT claims are available (richer context),
/// falls back to `build_auth_context()` for API key / password auth.
pub fn resolve_auth(
    headers: &HeaderMap,
    state: &AppState,
    peer_addr: &str,
) -> Result<
    (
        AuthenticatedIdentity,
        crate::control::security::auth_context::AuthContext,
    ),
    ApiError,
> {
    use crate::control::security::auth_context::{AuthContext, generate_session_id};

    // Check for JWT Bearer to get rich AuthContext.
    if let Some(auth_header) = headers.get("authorization")
        && let Ok(auth_str) = auth_header.to_str()
        && let Some(token) = auth_str.strip_prefix("Bearer ")
    {
        let token = token.trim();
        if let Some(identity) = try_validate_jwt(state, token) {
            let auth_ctx = if let Some(ref registry) = state.shared.jwks_registry
                && let Ok(claims) = registry.decode_claims(token)
            {
                AuthContext::from_jwt(&claims, generate_session_id())
            } else {
                tracing::trace!("JWT claims decode unavailable, using basic auth context");
                session_auth::build_auth_context(&identity)
            };
            let auth_ctx = apply_on_deny_header(headers, auth_ctx);
            return Ok((identity, auth_ctx));
        }
    }

    // Fallback: resolve identity normally and build basic AuthContext.
    let identity = resolve_identity(headers, state, peer_addr)?;
    let auth_ctx = apply_on_deny_header(headers, session_auth::build_auth_context(&identity));
    Ok((identity, auth_ctx))
}

/// Check `X-On-Deny` header and set the `on_deny_override` on AuthContext.
fn apply_on_deny_header(
    headers: &HeaderMap,
    mut auth_ctx: crate::control::security::auth_context::AuthContext,
) -> crate::control::security::auth_context::AuthContext {
    if let Some(val) = headers.get("x-on-deny")
        && let Ok(s) = val.to_str()
        && let Ok(mode) = crate::control::security::deny::parse_on_deny(&[s])
    {
        auth_ctx.on_deny_override = Some(mode);
    }
    auth_ctx
}

/// HTTP API error type.
#[derive(Debug)]
pub enum ApiError {
    Unauthorized(String),
    Forbidden(String),
    BadRequest(String),
    Internal(String),
    /// 429 Too Many Requests with Retry-After header.
    RateLimited {
        message: String,
        retry_after_secs: u64,
    },
    /// Arbitrary HTTP status from gateway error mapping.
    HttpStatus(u16, String),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        match self {
            ApiError::RateLimited {
                message,
                retry_after_secs,
            } => {
                let body = serde_json::json!({ "error": message });
                let mut resp = (StatusCode::TOO_MANY_REQUESTS, axum::Json(body)).into_response();
                if let Ok(val) = retry_after_secs.to_string().parse() {
                    resp.headers_mut().insert("Retry-After", val);
                }
                resp
            }
            other => {
                let (status, message) = match other {
                    ApiError::Unauthorized(msg) => (StatusCode::UNAUTHORIZED, msg),
                    ApiError::Forbidden(msg) => (StatusCode::FORBIDDEN, msg),
                    ApiError::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg),
                    ApiError::Internal(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg),
                    ApiError::RateLimited { .. } => unreachable!(),
                    ApiError::HttpStatus(code, msg) => (
                        StatusCode::from_u16(code).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
                        msg,
                    ),
                };
                let body = serde_json::json!({ "error": message });
                (status, axum::Json(body)).into_response()
            }
        }
    }
}

impl From<crate::Error> for ApiError {
    fn from(e: crate::Error) -> Self {
        match &e {
            crate::Error::RejectedAuthz { .. } => Self::Forbidden(e.to_string()),
            crate::Error::BadRequest { .. }
            | crate::Error::PlanError { .. }
            | crate::Error::Config { .. } => Self::BadRequest(e.to_string()),
            crate::Error::CollectionNotFound { .. } | crate::Error::DocumentNotFound { .. } => {
                Self::BadRequest(e.to_string())
            }
            _ => Self::Internal(e.to_string()),
        }
    }
}
