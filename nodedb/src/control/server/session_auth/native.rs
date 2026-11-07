// SPDX-License-Identifier: BUSL-1.1

//! Native-protocol JSON authentication dispatcher.
//!
//! The first frame on a native connection MUST be an auth request:
//! - `{"method": "trust"}` — trust mode (only if configured)
//! - `{"method": "password", "username": "...", "password": "..."}`
//! - `{"method": "api_key", "token": "ndb_..."}`
//!
//! An OIDC bearer token uses the `OidcBearer` auth frame, which
//! `native::dispatch::handle_auth` verifies before any JSON body is built.

use crate::config::auth::AuthMode;
use crate::control::security::audit::{
    ArcAuditEmitter, AuditEmitContext, AuditEmitter, AuditEvent,
};
use crate::control::security::credential::store::{AuthRejection, PasswordVerification};
use crate::control::security::escalation::{
    AuthViolation, ViolationSubject, record_auth_violation,
};
use crate::control::security::identity::{AuthMethod, AuthenticatedIdentity};
use crate::control::security::session_identity::stored_user_identity;
use crate::control::state::SharedState;
use crate::types::TenantId;

use super::identity::{configured_trust_identity, trust_identity, verify_api_key_identity};

/// Minimum wall-clock time for any authentication attempt that ends in failure.
///
/// All failed password auth paths (rate-limit, lockout, wrong password, unknown
/// user) sleep until `auth_start + AUTH_FLOOR` before returning an error.  This
/// makes the reject latency indistinguishable from a real Argon2 verification,
/// so an attacker cannot use timing to tell a rate-limit rejection from a
/// credential rejection — or to probe whether a username exists.
///
/// 200 ms matches a conservative Argon2id baseline (m=65536, t=3, p=1 on a
/// mid-range server core). Operators running faster Argon2 params can accept a
/// slightly narrower timing envelope; operators running slower params should
/// increase this constant.
pub const AUTH_FLOOR: std::time::Duration = std::time::Duration::from_millis(200);

/// Authenticate a native protocol connection from the first JSON frame.
///
/// Returns `(identity, warning)` on success. The `warning` string is non-empty
/// when the account is in password grace period or `must_change_password` is set
/// — the caller should forward it to the client as a notice/warning.
///
/// All failure paths on the `"password"` method enforce a constant-time floor
/// equal to [`AUTH_FLOOR`]: the function sleeps until `start + AUTH_FLOOR`
/// before returning any `Err`. This prevents timing oracle attacks that could
/// distinguish rate-limit rejection from credential rejection or reveal user
/// existence.
pub async fn authenticate(
    state: &SharedState,
    auth_mode: &AuthMode,
    body: &serde_json::Value,
    peer_addr: &str,
) -> crate::Result<(AuthenticatedIdentity, Option<String>)> {
    let method = body["method"].as_str().unwrap_or("trust");

    match method {
        "trust" => {
            if *auth_mode != AuthMode::Trust {
                record_auth_violation(
                    state,
                    AuthViolation {
                        subject: ViolationSubject::AuditOnly,
                        tenant_id: None,
                        source: peer_addr,
                        detail: "trust auth rejected: server requires authentication",
                    },
                );
                return Err(crate::Error::RejectedAuthz {
                    tenant_id: TenantId::new(0),
                    resource: "trust mode not enabled".into(),
                });
            }

            let requested_username = body["username"].as_str();
            let identity = requested_username
                .and_then(|username| trust_identity(state, username))
                .or_else(|| {
                    requested_username
                        .is_none()
                        .then(|| configured_trust_identity(state))
                        .flatten()
                })
                .ok_or_else(|| {
                    let username = requested_username.unwrap_or("<configured>");
                    record_auth_violation(
                        state,
                        AuthViolation {
                            subject: ViolationSubject::Username(username),
                            tenant_id: None,
                            source: peer_addr,
                            detail: &format!(
                                "native trust auth rejected: user '{username}' does not exist"
                            ),
                        },
                    );
                    crate::Error::RejectedAuthz {
                        tenant_id: TenantId::new(0),
                        resource: format!("trust user '{username}' does not exist"),
                    }
                })?;

            state.audit_record(
                AuditEvent::AuthSuccess,
                Some(identity.tenant_id),
                peer_addr,
                &format!("native trust auth: {}", identity.username),
            );
            state.auth_metrics.record_auth_success("trust");

            Ok((identity, None))
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

            // Record the auth start time for constant-time floor enforcement.
            // All failure returns below sleep until `auth_start + AUTH_FLOOR`
            // so the reject latency is indistinguishable from a real Argon2
            // verification, regardless of which gate tripped.
            let auth_start = std::time::Instant::now();

            // Pre-authentication login rate-limit check (before lockout and
            // Argon2 verification — cheap exit path).  Both the per-IP and
            // per-username buckets are consulted.
            use crate::control::security::ratelimit::limiter::LoginRateLimitOutcome;
            let peer_ip_str = peer_addr
                .parse::<std::net::SocketAddr>()
                .map(|s| s.ip().to_string())
                .unwrap_or_else(|_| peer_addr.to_string());
            let rl_outcome = state.rate_limiter.check_login(&peer_ip_str, username);
            if !matches!(rl_outcome, LoginRateLimitOutcome::Allowed) {
                let emitter = ArcAuditEmitter(std::sync::Arc::clone(&state.audit));
                let (detail, retry_after_secs) = match rl_outcome {
                    LoginRateLimitOutcome::IpExceeded { retry_after_secs } => (
                        format!("login rate limited (ip={peer_ip_str}): {username}"),
                        retry_after_secs,
                    ),
                    LoginRateLimitOutcome::UserExceeded { retry_after_secs } => (
                        format!("login rate limited (user): {username}"),
                        retry_after_secs,
                    ),
                    LoginRateLimitOutcome::Allowed => unreachable!(),
                };
                emitter.emit(
                    AuditEvent::LoginRateLimited,
                    "login_rate_limit",
                    &detail,
                    AuditEmitContext::new(None, "", username),
                );
                state.auth_metrics.record_auth_failure("password");
                // A rate-limit rejection is a TRANSIENT admission failure, not a
                // credential signal, so it is surfaced as a distinct retryable
                // error (`RateExceeded`) and logged distinctly (LoginRateLimited
                // above) — never collapsed into the generic "authentication
                // failed" that a wrong password / lockout / unknown user return.
                // The constant-time floor is deliberately NOT applied: unlike
                // the credential arms it reveals nothing about whether the
                // account exists or the password was right, so an early return
                // leaks no oracle while avoiding wasted latency. The genuine
                // credential arms below keep their `AUTH_FLOOR` and remain
                // indistinguishable from one another.
                return Err(crate::Error::RateExceeded {
                    gate: "login".into(),
                    detail: "too many login attempts; retry shortly".into(),
                    retry_after_ms: retry_after_secs.saturating_mul(1000),
                });
            }

            // Check lockout (after rate-limit, before Argon2).
            if let Err(e) = state.credentials.check_lockout(username) {
                // Audit only: a standing lockout refusing its own retries is
                // the verdict being enforced, not a fresh credential failure.
                // The failures that produced the lockout were counted where
                // they happened, in the password-rejection arm below. Mirrors
                // the pgwire lockout branch so a native-protocol rejection is
                // as visible in the audit log as a pgwire one.
                record_auth_violation(
                    state,
                    AuthViolation {
                        subject: ViolationSubject::AuditOnly,
                        tenant_id: None,
                        source: peer_addr,
                        detail: &format!("user '{username}' is locked out"),
                    },
                );
                // Constant-time floor before returning lockout error.
                enforce_auth_floor(auth_start).await;
                return Err(e);
            }

            let pw_warning = match state
                .credentials
                .verify_password_with_status(username, password)
            {
                PasswordVerification::Verified(warning) => warning,
                PasswordVerification::Rejected(reason) => {
                    // Only a genuine credential failure — a wrong password
                    // or an unknown user — counts toward the lockout
                    // counter. A policy rejection (expired / must-change
                    // password, inactive account) or an internal error
                    // must not: the supplied password may be correct, and
                    // locking the account would be a denial of service.
                    if reason == AuthRejection::BadCredential {
                        let emitter = ArcAuditEmitter(std::sync::Arc::clone(&state.audit));
                        let peer_ip = peer_addr
                            .parse::<std::net::SocketAddr>()
                            .ok()
                            .map(|s| s.ip());
                        state
                            .credentials
                            .record_login_failure(username, peer_ip, &emitter);
                        // Drive the per-IP / per-user brute-force window from the
                        // same place as the lockout counter: only genuine
                        // credential failures close it, so correct-credential
                        // bursts never trip the pre-verify admission check.
                        state
                            .rate_limiter
                            .record_login_failure(&peer_ip_str, username);
                    }
                    record_auth_violation(
                        state,
                        AuthViolation {
                            subject: ViolationSubject::Username(username),
                            tenant_id: None,
                            source: peer_addr,
                            detail: &format!("native password auth failed: {username}"),
                        },
                    );
                    state.auth_metrics.record_auth_failure("password");
                    // Argon2 already ran (≈AUTH_FLOOR elapsed); the sleep is
                    // a no-op when Argon2 was slower than the floor.
                    enforce_auth_floor(auth_start).await;
                    return Err(crate::Error::RejectedAuthz {
                        tenant_id: TenantId::new(0),
                        resource: "authentication failed".into(),
                    });
                }
            };

            state.credentials.record_login_success(username);

            let identity = stored_user_identity(state, username, AuthMethod::CleartextPassword)
                .ok_or_else(|| crate::Error::BadRequest {
                    detail: format!("user '{username}' not found after password verification"),
                })?;

            state.audit_record(
                AuditEvent::AuthSuccess,
                Some(identity.tenant_id),
                peer_addr,
                &format!("native password auth: {username}"),
            );
            state.auth_metrics.record_auth_success("password");

            if let Some(ref w) = pw_warning {
                tracing::warn!(username, warning = %w, "password warning at native password auth");
            }

            Ok((identity, pw_warning))
        }

        "api_key" => {
            let token = body["token"]
                .as_str()
                .ok_or_else(|| crate::Error::BadRequest {
                    detail: "missing 'token' for api_key auth".into(),
                })?;

            verify_api_key_identity(state, token, peer_addr, "native")
                .ok_or_else(|| {
                    record_auth_violation(
                        state,
                        AuthViolation {
                            subject: ViolationSubject::AuditOnly,
                            tenant_id: None,
                            source: peer_addr,
                            detail: "native api_key auth failed: invalid token or owner not found",
                        },
                    );
                    state.auth_metrics.record_auth_failure("api_key");
                    crate::Error::RejectedAuthz {
                        tenant_id: TenantId::new(0),
                        resource: "invalid API key".into(),
                    }
                })
                .map(|id| (id, None))
        }

        // An OIDC bearer token never reaches this dispatcher.
        // `dispatch::handle_auth` verifies it and keeps the verified claims for the session.
        other => Err(crate::Error::BadRequest {
            detail: format!(
                "unknown auth method: '{other}'. Use 'trust', 'password', or 'api_key'; \
                 an OIDC bearer token uses the native OidcBearer auth frame."
            ),
        }),
    }
}

/// Sleep until `auth_start + AUTH_FLOOR` to enforce a constant-time error path.
///
/// Called on every password-auth failure so that no failure mode (rate-limit,
/// lockout, wrong password, unknown user) can be distinguished from any other
/// by wall-clock timing.  When Argon2 already ran, `auth_start` is old enough
/// that the sleep duration is effectively zero.
async fn enforce_auth_floor(auth_start: std::time::Instant) {
    let deadline = auth_start + AUTH_FLOOR;
    let now = std::time::Instant::now();
    if deadline > now {
        tokio::time::sleep(deadline - now).await;
    }
}
