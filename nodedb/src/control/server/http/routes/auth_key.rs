// SPDX-License-Identifier: BUSL-1.1

//! JWT exchange endpoint: validate JWT, JIT provision user, return `nda_` key.
//!
//! ```text
//! POST /v1/auth/exchange-key
//! Authorization: Bearer <jwt>
//! Content-Type: application/json
//! { "scopes": ["profile:read"], "rate_limit_qps": 100, "expires_days": 30 }
//!
//! Response: { "api_key": "nda_...", "auth_user_id": "...", "expires_in": 2592000 }
//! ```

use axum::extract::State;
use axum::http::HeaderMap;
use axum::response::IntoResponse;

use super::super::admission::{admit, identity_database};
use super::super::auth::{ApiError, AppState, resolve_auth};
use super::super::peer::PeerAddr;
use super::super::transport::ClientTransport;
use super::super::types::{HttpExchangeKeyRequest, HttpExchangeKeyResponse};

/// `POST /v1/auth/exchange-key` — Exchange a JWT for an `nda_` API key.
pub async fn exchange_key(
    headers: HeaderMap,
    peer: PeerAddr,
    transport: ClientTransport,
    State(state): State<AppState>,
    axum::Json(body): axum::Json<HttpExchangeKeyRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let (identity, _auth_ctx) =
        resolve_auth(&headers, &state, peer.as_str(), transport.security()).await?;

    // Minting a long-lived credential is work done on the caller's behalf, so
    // it runs behind the full gate: a blacklisted IP or a suspended/banned
    // account must not be able to issue itself an API key, and the rate limit
    // bounds bulk key minting from one principal.
    let rate_limit_headers = admit(
        &state,
        &identity,
        identity_database(&identity),
        peer.as_str(),
        "auth_exchange_key",
    )?;

    let auth_user_id = identity.user_id.to_string();
    let rate_limit_qps = body.rate_limit_qps.unwrap_or(0);
    let rate_limit_burst = body.rate_limit_burst.unwrap_or(0);
    let expires_days = body.expires_days.unwrap_or(0);

    // The record is durable before the key exists, so a failed record issues no key.
    state.shared.audit_record_strict(
        crate::control::security::audit::AuditEvent::ActionIntent,
        Some(identity.tenant_id),
        &identity.username,
        &format!("JWT exchange issues auth API key for user '{auth_user_id}'"),
    )?;

    let token = state.shared.auth_api_keys.create_key(
        &auth_user_id,
        identity.tenant_id.as_u64(),
        body.scopes,
        rate_limit_qps,
        rate_limit_burst,
        expires_days,
    )?;

    let expires_in = if expires_days > 0 {
        expires_days * 86_400
    } else {
        0
    };

    state.shared.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!("JWT exchanged for auth API key for user '{auth_user_id}'"),
    );

    Ok((
        rate_limit_headers,
        axum::Json(HttpExchangeKeyResponse {
            api_key: token,
            auth_user_id,
            expires_in,
        }),
    ))
}
