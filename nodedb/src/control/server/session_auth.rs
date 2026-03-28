//! Authentication helpers shared across protocol handlers.
//!
//! **Native protocol** (JSON frames): the first frame MUST be an auth request.
//! Supported methods:
//! - `{"op": "auth", "method": "api_key", "token": "ndb_..."}` — API key
//! - `{"op": "auth", "method": "password", "username": "...", "password": "..."}` — cleartext
//! - `{"op": "auth", "method": "trust"}` — trust mode (only if configured)
//!
//! **mTLS certificate auth**: resolved during TLS handshake before the first
//! frame. The certificate's Common Name (CN) is mapped to a username via
//! [`resolve_certificate_identity()`]. This is called from the connection
//! listener, not from the JSON `authenticate()` dispatcher.
//!
//! On success, returns `{"status": "ok", "username": "...", "tenant_id": ...}`.
//! On failure, returns `{"status": "error", "error": "..."}` and closes connection.

use crate::config::auth::AuthMode;
use crate::control::security::audit::AuditEvent;
use crate::control::security::auth_context::{AuthContext, generate_session_id};
use crate::control::security::identity::{AuthMethod, AuthenticatedIdentity, Role};
use crate::control::state::SharedState;
use crate::types::TenantId;

/// Resolve an identity from a TLS client certificate CN.
///
/// Maps the certificate Common Name to a username in the credential store.
/// Used when `auth.mode = "certificate"` and client presents a TLS cert.
pub fn resolve_certificate_identity(
    state: &SharedState,
    cn: &str,
    peer_addr: &str,
) -> crate::Result<AuthenticatedIdentity> {
    // Map cert CN to username (direct mapping: CN = username).
    let identity = state
        .credentials
        .to_identity(cn, AuthMethod::Certificate)
        .ok_or_else(|| {
            state.audit_record(
                AuditEvent::AuthFailure,
                None,
                peer_addr,
                &format!("mTLS auth failed: no user for cert CN '{cn}'"),
            );
            crate::Error::RejectedAuthz {
                tenant_id: TenantId::new(0),
                resource: format!("no user mapped to certificate CN '{cn}'"),
            }
        })?;

    state.audit_record(
        AuditEvent::AuthSuccess,
        Some(identity.tenant_id),
        peer_addr,
        &format!("mTLS cert auth: {cn}"),
    );

    Ok(identity)
}

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

/// Build an `AuthContext` from an `AuthenticatedIdentity`.
///
/// This is the centralized factory used by all auth flows (password,
/// API key, certificate, trust). JWT flows can use `AuthContext::from_jwt()`
/// directly when JWT claims are available for richer context.
pub fn build_auth_context(identity: &AuthenticatedIdentity) -> AuthContext {
    AuthContext::from_identity(identity, generate_session_id())
}

/// Check if a user is blacklisted. Returns `Err` if blocked.
///
/// Called after identity is resolved, before authorization.
pub fn check_blacklist(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    peer_addr: &str,
) -> crate::Result<()> {
    // Check user blacklist.
    let user_id = identity.user_id.to_string();
    if let Some(entry) = state.blacklist.check_user(&user_id) {
        state.audit_record(
            AuditEvent::AuthFailure,
            Some(identity.tenant_id),
            peer_addr,
            &format!(
                "blacklisted user '{}' denied: {}",
                identity.username, entry.reason
            ),
        );
        return Err(crate::Error::RejectedAuthz {
            tenant_id: identity.tenant_id,
            resource: format!("user blacklisted: {}", entry.reason),
        });
    }

    // Check IP blacklist.
    if let Some(entry) = state.blacklist.check_ip(peer_addr) {
        state.audit_record(
            AuditEvent::AuthFailure,
            Some(identity.tenant_id),
            peer_addr,
            &format!("blacklisted IP '{peer_addr}' denied: {}", entry.reason),
        );
        return Err(crate::Error::RejectedAuthz {
            tenant_id: identity.tenant_id,
            resource: format!("IP blacklisted: {}", entry.reason),
        });
    }

    // Check auth user status (JIT-provisioned users).
    if let Some(status) = state.auth_users.get_status(&user_id) {
        let ctx_status = status;
        if matches!(
            ctx_status,
            crate::control::security::auth_context::AuthStatus::Suspended
                | crate::control::security::auth_context::AuthStatus::Banned
        ) {
            state.audit_record(
                AuditEvent::AuthFailure,
                Some(identity.tenant_id),
                peer_addr,
                &format!(
                    "auth user '{}' denied: account {}",
                    identity.username, ctx_status
                ),
            );
            return Err(crate::Error::RejectedAuthz {
                tenant_id: identity.tenant_id,
                resource: format!("account {ctx_status}"),
            });
        }
    }

    Ok(())
}

/// Check rate limit for a request.
///
/// Called after identity and blacklist checks, before query execution.
/// Returns `Err(RateLimited)` if the request exceeds the rate limit.
pub fn check_rate_limit(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    auth_ctx: &AuthContext,
    operation: &str,
) -> crate::Result<crate::control::security::ratelimit::limiter::RateLimitResult> {
    let plan_tier = auth_ctx.metadata.get("plan").map(|s| s.as_str());
    let result = state.rate_limiter.check(
        &identity.user_id.to_string(),
        &auth_ctx.org_ids,
        plan_tier,
        operation,
    );

    if !result.allowed {
        return Err(crate::Error::RejectedAuthz {
            tenant_id: identity.tenant_id,
            resource: format!("rate limited: retry after {}s", result.retry_after_secs),
        });
    }

    Ok(result)
}
