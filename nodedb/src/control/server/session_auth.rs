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

/// Enrich AuthContext with scope status data from the scope grant store.
///
/// Populates metadata entries for `scope_status.<name>` and `scope_expires_at.<name>`
/// so RLS predicates can reference `$auth.metadata.scope_status.pro:all`.
pub fn enrich_auth_context_with_scopes(
    ctx: &mut AuthContext,
    scope_grants: &crate::control::security::scope::grant::ScopeGrantStore,
    org_ids: &[String],
) {
    let effective = scope_grants.effective_scopes(&ctx.id, org_ids);
    for scope_name in &effective {
        let status = scope_grants.scope_status(scope_name, "user", &ctx.id);
        ctx.metadata
            .insert(format!("scope_status.{scope_name}"), status.to_string());
        let expires_at = scope_grants.scope_expires_at(scope_name, "user", &ctx.id);
        if expires_at > 0 {
            ctx.metadata.insert(
                format!("scope_expires_at.{scope_name}"),
                expires_at.to_string(),
            );
        }
    }
    // Also set a comma-separated list of effective scopes.
    let scope_list: Vec<String> = effective.into_iter().collect();
    if !scope_list.is_empty() {
        ctx.metadata.insert("scopes".into(), scope_list.join(","));
    }
}

/// Build an `AuthContext` with pgwire session overrides applied.
///
/// Reads `nodedb.on_deny` and `nodedb.auth_session` from session parameters.
/// If `nodedb.auth_session` is set, resolves the opaque handle to a cached
/// `AuthContext` (created via `POST /api/auth/session`), replacing the
/// connection-level identity context entirely.
pub fn build_auth_context_with_session(
    identity: &AuthenticatedIdentity,
    sessions: &crate::control::server::pgwire::session::SessionStore,
    addr: &std::net::SocketAddr,
) -> AuthContext {
    let mut ctx = build_auth_context(identity);

    // Read ON DENY override from SET LOCAL nodedb.on_deny = '...'.
    if let Some(on_deny_val) = sessions.get_parameter(addr, "nodedb.on_deny")
        && let Ok(mode) = crate::control::security::deny::parse_on_deny(&[&on_deny_val])
    {
        ctx.on_deny_override = Some(mode);
    }

    ctx
}

/// Extract a per-query `ON DENY` clause from SQL and apply it to the auth context.
///
/// Parses: `SELECT ... ON DENY ERROR 'CODE' MESSAGE '...'`
/// Strips the `ON DENY` clause from the SQL and sets `auth_ctx.on_deny_override`.
/// Returns the cleaned SQL.
pub fn extract_and_apply_on_deny(
    sql: &str,
    auth_ctx: &mut crate::control::security::auth_context::AuthContext,
) -> String {
    // Use lowercase for case-insensitive search to avoid byte-length mismatches
    // with non-ASCII characters under Unicode case folding.
    let lower = sql.to_lowercase();
    let Some(idx) = lower.rfind("on deny ") else {
        return sql.to_string();
    };

    // Only strip ON DENY from SELECT/WITH queries (not CREATE RLS POLICY).
    let trimmed = lower.trim_start();
    if !trimmed.starts_with("select") && !trimmed.starts_with("with") {
        return sql.to_string();
    }

    let on_deny_part = &sql[idx + "on deny ".len()..];
    let parts: Vec<&str> = on_deny_part.split_whitespace().collect();
    match crate::control::security::deny::parse_on_deny(&parts) {
        Ok(mode) => {
            auth_ctx.on_deny_override = Some(mode);
            sql[..idx].trim_end().to_string()
        }
        Err(_) => sql.to_string(),
    }
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

    // Check org status overrides member status.
    // If any of the user's orgs is suspended/banned, block the user.
    let user_org_ids = state.orgs.orgs_for_user(&user_id);
    for org_id in &user_org_ids {
        if !state.orgs.is_active(org_id) {
            state.audit_record(
                AuditEvent::AuthFailure,
                Some(identity.tenant_id),
                peer_addr,
                &format!(
                    "org '{}' is not active — user '{}' blocked",
                    org_id, identity.username
                ),
            );
            return Err(crate::Error::RejectedAuthz {
                tenant_id: identity.tenant_id,
                resource: format!("organization '{org_id}' is suspended"),
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
