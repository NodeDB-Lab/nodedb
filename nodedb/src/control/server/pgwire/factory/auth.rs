// SPDX-License-Identifier: BUSL-1.1

use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;

use pgwire::api::auth::{AuthSource, LoginInfo, Password};
use pgwire::error::{PgWireError, PgWireResult};

use crate::control::security::audit::AuditEvent;
use crate::control::security::credential::CredentialStore;
use crate::control::security::credential::store::ScramLookup;
use crate::control::state::SharedState;

/// Bridges NodeDB's CredentialStore to pgwire's `AuthSource` trait.
pub struct NodeDbAuthSource {
    credentials: Arc<CredentialStore>,
    state: Arc<SharedState>,
}

impl NodeDbAuthSource {
    pub(super) fn new(credentials: Arc<CredentialStore>, state: Arc<SharedState>) -> Self {
        Self { credentials, state }
    }
}

impl Debug for NodeDbAuthSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeDbAuthSource").finish()
    }
}

#[async_trait]
impl AuthSource for NodeDbAuthSource {
    async fn get_password(&self, login: &LoginInfo) -> PgWireResult<Password> {
        let username = login.user().unwrap_or("unknown");
        let source = login.host();

        // Record auth start time for constant-time floor enforcement on all
        // failure paths (rate-limit, lockout, unknown user).
        let auth_start = std::time::Instant::now();

        // Pre-authentication login rate-limit check — consulted before lockout
        // and before SCRAM credential lookup begins.
        use crate::control::security::ratelimit::limiter::LoginRateLimitOutcome;
        use crate::control::server::session_auth::AUTH_FLOOR;
        let peer_ip_str = source
            .parse::<std::net::SocketAddr>()
            .map(|s| s.ip().to_string())
            .unwrap_or_else(|_| source.to_string());
        let rl_outcome = self.state.rate_limiter.check_login(&peer_ip_str, username);
        if !matches!(rl_outcome, LoginRateLimitOutcome::Allowed) {
            use crate::control::security::audit::{
                ArcAuditEmitter, AuditEmitContext, AuditEmitter,
            };
            let emitter = ArcAuditEmitter(std::sync::Arc::clone(&self.state.audit));
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
            self.state.auth_metrics.record_auth_failure("scram");
            // A rate-limit rejection is a TRANSIENT admission failure, not a
            // credential signal. It is surfaced as a distinct, retryable
            // TOO_MANY_CONNECTIONS (53300) error and logged distinctly
            // (LoginRateLimited above) — never collapsed into the invalid-
            // password error that wrong-password / lockout / unknown-user
            // return. The constant-time AUTH_FLOOR is deliberately skipped here:
            // this arm reveals nothing about account existence or password
            // correctness, so an early return leaks no timing oracle while the
            // genuine credential arms below keep their floor and stay mutually
            // indistinguishable.
            let msg = format!("too many login attempts; retry after {retry_after_secs}s");
            return Err(
                crate::control::server::pgwire::types::error_map::sqlstate_error(
                    nodedb_types::error::sqlstate::TOO_MANY_CONNECTIONS,
                    &msg,
                ),
            );
        }

        // Check lockout before returning credentials.
        if self.credentials.check_lockout(username).is_err() {
            self.state.audit_record(
                AuditEvent::AuthFailure,
                None,
                source,
                &format!("user '{username}' is locked out"),
            );
            // Constant-time floor for lockout rejection.
            let deadline = auth_start + AUTH_FLOOR;
            let now = std::time::Instant::now();
            if deadline > now {
                tokio::time::sleep(deadline - now).await;
            }
            // The wire rejection must be indistinguishable from an ordinary
            // wrong-password failure: announcing "account locked" would
            // confirm the username and leak the lockout state to an
            // unauthenticated probe. The lockout is recorded in the audit
            // log above for operators.
            return Err(PgWireError::InvalidPassword(username.to_owned()));
        }

        match self.credentials.get_scram_credentials(username) {
            ScramLookup::Found(creds) => {
                // A non-empty warning means grace period or must_change_password.
                // pgwire's AuthSource doesn't surface NoticeResponse here; the
                // warning is stored in the factory and must be sent after auth
                // success via the on_startup hook. For now, log it — the
                // post-auth notice path requires plumbing that would touch
                // pgwire's internal state machine. The warning IS surfaced on
                // the native protocol path (see session_auth::authenticate).
                if let Some(ref w) = creds.warning {
                    tracing::warn!(username, warning = %w, "password warning at SCRAM credential fetch");
                }
                Ok(Password::new(Some(creds.salt), creds.salted_password))
            }
            ScramLookup::Rejected(_) => {
                // The lockout counter is driven from a single place — the
                // SASL-failure arm in `AuthStartup::Scram` — so that a
                // credential-lookup rejection here and a wrong-proof
                // failure there are not double-counted. That arm re-derives
                // the rejection reason and counts only genuine credential
                // failures. `get_password` only emits the audit record.
                self.state.audit_record(
                    AuditEvent::AuthFailure,
                    None,
                    source,
                    &format!("SCRAM credential lookup rejected for user: {username}"),
                );
                Err(PgWireError::InvalidPassword(username.to_owned()))
            }
        }
    }
}
