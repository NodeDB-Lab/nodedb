//! Native protocol authentication handshake.
//!
//! The first frame on a native protocol connection MUST be an auth request.
//! Supported auth methods:
//! - `{"op": "auth", "method": "api_key", "token": "ndb_..."}` — API key
//! - `{"op": "auth", "method": "password", "username": "...", "password": "..."}` — cleartext
//! - `{"op": "auth", "method": "trust"}` — trust mode (only if configured)
//!
//! On success, returns `{"status": "ok", "username": "...", "tenant_id": ...}`.
//! On failure, returns `{"status": "error", "error": "..."}` and closes connection.

use crate::config::auth::AuthMode;
use crate::control::security::audit::AuditEvent;
use crate::control::security::identity::{AuthMethod, AuthenticatedIdentity, Role};
use crate::control::state::SharedState;
use crate::types::TenantId;

/// Verify an API key token and build an authenticated identity.
///
/// Shared by native protocol and HTTP API authentication paths.
/// Returns `None` if the token is invalid or the owner user is not found.
pub fn verify_api_key_identity(
    state: &SharedState,
    token: &str,
    peer_addr: &str,
    protocol: &str,
) -> Option<AuthenticatedIdentity> {
    let key_record = state.api_keys.verify_key(token)?;

    let user = state.credentials.get_user(&key_record.username)?;

    let identity = AuthenticatedIdentity {
        user_id: key_record.user_id,
        username: key_record.username.clone(),
        tenant_id: key_record.tenant_id,
        auth_method: AuthMethod::ApiKey,
        roles: user.roles,
        is_superuser: user.is_superuser,
    };

    state.audit_record(
        AuditEvent::AuthSuccess,
        Some(identity.tenant_id),
        peer_addr,
        &format!(
            "{protocol} api_key auth: {} (key {})",
            identity.username, key_record.key_id
        ),
    );

    Some(identity)
}

/// Build a default trust-mode identity for a given username.
///
/// Used by both explicit auth requests and auto-auth on first frame.
pub fn trust_identity(state: &SharedState, username: &str) -> AuthenticatedIdentity {
    if let Some(id) = state.credentials.to_identity(username, AuthMethod::Trust) {
        id
    } else {
        AuthenticatedIdentity {
            user_id: 0,
            username: username.to_string(),
            tenant_id: TenantId::new(1),
            auth_method: AuthMethod::Trust,
            roles: vec![Role::Superuser],
            is_superuser: true,
        }
    }
}

/// Authenticate a native protocol connection from the first JSON frame.
///
/// Returns the authenticated identity on success.
pub fn authenticate(
    state: &SharedState,
    auth_mode: &AuthMode,
    body: &serde_json::Value,
    peer_addr: &str,
) -> crate::Result<AuthenticatedIdentity> {
    let method = body["method"].as_str().unwrap_or("trust");

    match method {
        "trust" => {
            if *auth_mode != AuthMode::Trust {
                state.audit_record(
                    AuditEvent::AuthFailure,
                    None,
                    peer_addr,
                    "trust auth rejected: server requires authentication",
                );
                return Err(crate::Error::RejectedAuthz {
                    tenant_id: TenantId::new(0),
                    resource: "trust mode not enabled".into(),
                });
            }

            let username = body["username"].as_str().unwrap_or("anonymous");
            let identity = trust_identity(state, username);

            state.audit_record(
                AuditEvent::AuthSuccess,
                Some(identity.tenant_id),
                peer_addr,
                &format!("native trust auth: {username}"),
            );

            Ok(identity)
        }

        "password" => {
            let username = body["username"]
                .as_str()
                .ok_or_else(|| crate::Error::BadRequest {
                    detail: "missing 'username' for password auth".into(),
                })?;
            let password = body["password"]
                .as_str()
                .ok_or_else(|| crate::Error::BadRequest {
                    detail: "missing 'password' for password auth".into(),
                })?;

            // Check lockout.
            state.credentials.check_lockout(username)?;

            if !state.credentials.verify_password(username, password) {
                state.credentials.record_login_failure(username);
                state.audit_record(
                    AuditEvent::AuthFailure,
                    None,
                    peer_addr,
                    &format!("native password auth failed: {username}"),
                );
                return Err(crate::Error::RejectedAuthz {
                    tenant_id: TenantId::new(0),
                    resource: format!("authentication failed for user '{username}'"),
                });
            }

            state.credentials.record_login_success(username);

            let identity = state
                .credentials
                .to_identity(username, AuthMethod::CleartextPassword)
                .ok_or_else(|| crate::Error::BadRequest {
                    detail: format!("user '{username}' not found after password verification"),
                })?;

            state.audit_record(
                AuditEvent::AuthSuccess,
                Some(identity.tenant_id),
                peer_addr,
                &format!("native password auth: {username}"),
            );

            Ok(identity)
        }

        "api_key" => {
            let token = body["token"]
                .as_str()
                .ok_or_else(|| crate::Error::BadRequest {
                    detail: "missing 'token' for api_key auth".into(),
                })?;

            verify_api_key_identity(state, token, peer_addr, "native").ok_or_else(|| {
                state.audit_record(
                    AuditEvent::AuthFailure,
                    None,
                    peer_addr,
                    "native api_key auth failed: invalid token or owner not found",
                );
                crate::Error::RejectedAuthz {
                    tenant_id: TenantId::new(0),
                    resource: "invalid API key".into(),
                }
            })
        }

        other => Err(crate::Error::BadRequest {
            detail: format!(
                "unknown auth method: '{other}'. Use 'trust', 'password', or 'api_key'."
            ),
        }),
    }
}
