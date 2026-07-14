// SPDX-License-Identifier: BUSL-1.1

//! pgwire connection factory: SCRAM-SHA-256 / Argon2 authentication and
//! session bootstrapping.
//!
//! **Auth scope**: pgwire authenticates exclusively via SCRAM-SHA-256 over
//! the Postgres wire protocol. OIDC bearer tokens are NOT accepted here —
//! the Postgres wire protocol has no clean way to carry a bearer without a
//! non-standard extension or a sidecar proxy. OIDC bearer logins live on
//! the native and HTTP entry points (see `control/security/oidc/`); do not
//! add a JWT branch to this factory.

use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use futures::sink::Sink;

use pgwire::api::auth::{
    AuthSource, DefaultServerParameterProvider, LoginInfo, Password, StartupHandler,
};
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::{ClientInfo, PgWireServerHandlers};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};

use crate::config::auth::AuthMode;
use crate::control::security::audit::{ArcAuditEmitter, AuditEvent};
use crate::control::security::credential::CredentialStore;
use crate::control::security::credential::store::{AuthRejection, ScramLookup};
use crate::control::state::SharedState;

use super::handler::NodeDbPgHandler;

// ── AuthSource for SCRAM-SHA-256 ────────────────────────────────────

/// Bridges NodeDB's CredentialStore to pgwire's `AuthSource` trait.
pub struct NodeDbAuthSource {
    credentials: Arc<CredentialStore>,
    state: Arc<SharedState>,
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
            return Err(super::types::error_map::sqlstate_error(
                nodedb_types::error::sqlstate::TOO_MANY_CONNECTIONS,
                &msg,
            ));
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

// ── Server parameter provider ───────────────────────────────────────

/// Server parameter provider used by BOTH the trust and SCRAM startup paths.
///
/// Wraps pgwire's `DefaultServerParameterProvider` (which carries a fixed set
/// of parameters and has no `server_version_num`) and augments it with
/// `server_version_num` so PostgreSQL clients that inspect the numeric server
/// version at connect time (e.g. drivers gating feature use on it) receive it
/// in the startup `ParameterStatus` burst. `server_version` is overridden to
/// NodeDB's own version string.
#[derive(Debug)]
struct NodeDbParameterProvider {
    inner: DefaultServerParameterProvider,
}

impl NodeDbParameterProvider {
    fn new() -> Self {
        let mut inner = DefaultServerParameterProvider::default();
        inner.server_version = format!("NodeDB {}", crate::version::VERSION);
        Self { inner }
    }
}

impl pgwire::api::auth::ServerParameterProvider for NodeDbParameterProvider {
    fn server_parameters<C>(&self, client: &C) -> Option<std::collections::HashMap<String, String>>
    where
        C: pgwire::api::ClientInfo,
    {
        let mut params = self.inner.server_parameters(client)?;
        params.insert(
            "server_version_num".to_owned(),
            nodedb_types::pg_compat::PG_COMPAT_VERSION_NUM.to_owned(),
        );
        Some(params)
    }
}

fn nodedb_parameter_provider() -> NodeDbParameterProvider {
    NodeDbParameterProvider::new()
}

// ── Factory ─────────────────────────────────────────────────────────

/// Factory that wires together the pgwire handlers.
///
/// Supports trust mode (handshake with trust user-gating) and password mode
/// (SCRAM-SHA-256 via pgwire's SASL implementation). Both paths announce
/// startup parameters through `NodeDbParameterProvider`.
pub struct NodeDbPgHandlerFactory {
    handler: Arc<NodeDbPgHandler>,
    auth_mode: AuthMode,
    credentials: Arc<CredentialStore>,
    state: Arc<SharedState>,
}

impl NodeDbPgHandlerFactory {
    pub fn new(state: Arc<SharedState>, auth_mode: AuthMode) -> Self {
        Self {
            handler: Arc::new(NodeDbPgHandler::new(Arc::clone(&state), auth_mode.clone())),
            auth_mode,
            credentials: Arc::clone(&state.credentials),
            state,
        }
    }

    /// Reclaim an abandoned transaction's overlays and drop the shared session
    /// entry when a pgwire connection ends. Idempotent — a no-op when the
    /// connection had no open transaction.
    pub async fn on_connection_end(&self, addr: &std::net::SocketAddr) {
        self.handler.reclaim_open_txn(addr).await;
        self.handler.sessions.remove(addr);
    }

    /// Whether the connection at `addr` is eligible for idle timeout right now:
    /// its session has zero statements in flight and has been silent for at
    /// least `idle_ms`. Used by the pgwire listener watchdog, which owns the
    /// per-connection task but cannot see inside pgwire's `process_socket`
    /// loop. Returns `false` when the session is missing (nothing to time out).
    pub fn session_idle_eligible(&self, addr: &std::net::SocketAddr, idle_ms: u64) -> bool {
        self.handler.sessions.idle_eligible(
            addr,
            idle_ms,
            crate::control::server::shared::session::now_unix_ms(),
        )
    }
}

impl PgWireServerHandlers for NodeDbPgHandlerFactory {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        self.handler.clone()
    }

    fn extended_query_handler(&self) -> Arc<impl ExtendedQueryHandler> {
        self.handler.clone()
    }

    fn copy_handler(&self) -> Arc<impl pgwire::api::copy::CopyHandler> {
        Arc::new(super::handler::NodeDbCopyHandler {
            state: Arc::clone(&self.state),
            restore_state: Arc::clone(&self.handler.restore_state),
        })
    }

    fn startup_handler(&self) -> Arc<impl StartupHandler> {
        match self.auth_mode {
            AuthMode::Trust => Arc::new(AuthStartup::Trust(self.handler.clone())),
            AuthMode::Password | AuthMode::Certificate => {
                let auth_source = Arc::new(NodeDbAuthSource {
                    credentials: Arc::clone(&self.credentials),
                    state: Arc::clone(&self.state),
                });
                let scram = pgwire::api::auth::sasl::scram::ScramAuth::new(auth_source);
                let params = Arc::new(nodedb_parameter_provider());
                let sasl =
                    pgwire::api::auth::sasl::SASLAuthStartupHandler::new(params).with_scram(scram);
                Arc::new(AuthStartup::Scram {
                    sasl: Box::new(sasl),
                    state: Arc::clone(&self.state),
                    handler: self.handler.clone(),
                })
            }
        }
    }
}

// ── Startup handler dispatch ────────────────────────────────────────

/// Enum dispatch for startup handler — avoids dyn trait object issues.
enum AuthStartup {
    Trust(Arc<NodeDbPgHandler>),
    Scram {
        sasl: Box<pgwire::api::auth::sasl::SASLAuthStartupHandler<NodeDbParameterProvider>>,
        state: Arc<SharedState>,
        /// Handler reference so we can bind the startup `database` param to
        /// the session store after SCRAM succeeds (mirrors the trust path).
        handler: Arc<NodeDbPgHandler>,
    },
}

/// Resolve the pgwire `database` StartupMessage parameter to a `DatabaseId`
/// and bind it to the session store for this connection.
///
/// The key `"database"` is set by clients via `dbname=` or `psql -d <name>`.
/// An absent or empty value is silently ignored — the session will use the
/// server default (DatabaseId::DEFAULT / `"default"`).
/// An unrecognised name is also silently ignored here; the first DDL/DML
/// statement will surface the missing-database error at query time, which
/// matches PostgreSQL behaviour for `psql -d nonexistent` (it succeeds at
/// connect; errors on the first query that requires the db).
fn bind_startup_database<C: pgwire::api::ClientInfo>(
    client: &C,
    addr: &std::net::SocketAddr,
    handler: &NodeDbPgHandler,
) {
    let db_name = match client.metadata().get("database") {
        Some(n) if !n.is_empty() => n.clone(),
        _ => return,
    };

    handler.sessions.ensure_session(*addr);

    let db_id = handler
        .state
        .credentials
        .catalog()
        .get_database_id_by_name(&db_name)
        .ok()
        .flatten();

    if let Some(id) = db_id {
        handler.sessions.set_current_database(addr, id);
    }
    // If the name is not found we leave current_database unset (None).
    // The first query that actually needs a database context will produce
    // the appropriate DATABASE_NOT_FOUND error.
}

#[async_trait]
impl StartupHandler for AuthStartup {
    async fn on_startup<C>(
        &self,
        client: &mut C,
        message: PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + futures::sink::Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        match self {
            AuthStartup::Trust(handler) => {
                // Run the handshake with NodeDB's custom parameter provider so
                // trust clients receive `server_version` / `server_version_num`
                // in the startup ParameterStatus burst — pgwire's default noop
                // path would emit its own hardcoded server_version instead. The
                // trust user-gating (unknown user → reject) is preserved and
                // must run before AuthenticationOk is announced.
                if let PgWireFrontendMessage::Startup(ref startup) = message {
                    pgwire::api::auth::protocol_negotiation(client, startup).await?;
                    pgwire::api::auth::save_startup_parameters_to_metadata(client, startup);
                    // Reject unknown trust users before we announce AuthenticationOk.
                    handler.resolve_trust_user(client).await?;
                    pgwire::api::auth::finish_authentication(client, &nodedb_parameter_provider())
                        .await?;
                }

                let username = client
                    .metadata()
                    .get("user")
                    .cloned()
                    .unwrap_or_else(|| "unknown".to_string());
                let source = client.socket_addr().to_string();
                handler.state.audit_record(
                    AuditEvent::AuthSuccess,
                    None,
                    &source,
                    &format!("trust auth: {username}"),
                );

                // Bind the `database` startup parameter to the session store.
                // `psql -d <name>` sets this key in the pgwire StartupMessage;
                // we resolve it once at handshake time so every query on this
                // connection executes in the declared database context.
                let addr = client.socket_addr();
                bind_startup_database(client, &addr, handler);

                Ok(())
            }
            AuthStartup::Scram {
                sasl,
                state,
                handler,
            } => {
                let was_in_auth = matches!(
                    client.state(),
                    pgwire::api::PgWireConnectionState::AuthenticationInProgress
                );

                let result = sasl.on_startup(client, message).await;

                let username = client
                    .metadata()
                    .get("user")
                    .cloned()
                    .unwrap_or_else(|| "unknown".to_string());
                let source = client.socket_addr().to_string();

                match &result {
                    Ok(())
                        if was_in_auth
                            && matches!(
                                client.state(),
                                pgwire::api::PgWireConnectionState::ReadyForQuery
                            ) =>
                    {
                        // SCRAM succeeded — reset lockout counter and bind database.
                        state.credentials.record_login_success(&username);
                        state.audit_record(
                            AuditEvent::AuthSuccess,
                            None,
                            &source,
                            &format!("SCRAM-SHA-256 auth: {username}"),
                        );
                        // Bind the `database` startup parameter to the session.
                        let addr = client.socket_addr();
                        bind_startup_database(client, &addr, handler);
                    }
                    Err(_) if was_in_auth => {
                        // SCRAM failed. This is the single place the lockout
                        // counter is driven for the SCRAM path. A SASL
                        // failure counts as a credential failure only when
                        // the account's credentials were actually usable
                        // (so the failure is a wrong client proof) or the
                        // user is unknown. A policy rejection from the
                        // credential lookup (expired / must-change password,
                        // inactive or service account) or an internal error
                        // must not count — the password may well be correct.
                        let scram_ip_str = source
                            .parse::<std::net::SocketAddr>()
                            .map(|s| s.ip().to_string())
                            .unwrap_or_else(|_| source.clone());
                        // A SASL failure that was actually caused by the
                        // pre-verify admission gate (rate-limit / DoS ceiling)
                        // must NOT move the brute-force or lockout counters —
                        // the client proof was never even checked. Only a
                        // genuine wrong-proof / unknown-user failure counts.
                        let rate_limited = state
                            .rate_limiter
                            .is_login_rate_limited(&scram_ip_str, &username);
                        let counts = !rate_limited
                            && matches!(
                                state.credentials.get_scram_credentials(&username),
                                ScramLookup::Found(_)
                                    | ScramLookup::Rejected(AuthRejection::BadCredential)
                            );
                        if counts {
                            let emitter = ArcAuditEmitter(std::sync::Arc::clone(&state.audit));
                            let scram_ip =
                                source.parse::<std::net::SocketAddr>().ok().map(|s| s.ip());
                            state
                                .credentials
                                .record_login_failure(&username, scram_ip, &emitter);
                            // Drive the per-IP / per-user brute-force window from
                            // the same genuine-failure site as the lockout
                            // counter.
                            state
                                .rate_limiter
                                .record_login_failure(&scram_ip_str, &username);
                        }
                        state.audit_record(
                            AuditEvent::AuthFailure,
                            None,
                            &source,
                            &format!("SCRAM-SHA-256 auth failed: {username}"),
                        );
                    }
                    _ => {}
                }

                result
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pgwire::api::DefaultClient;
    use pgwire::api::auth::ServerParameterProvider;

    /// The custom provider used by BOTH startup paths must emit NodeDB's own
    /// `server_version` and the PG-compat `server_version_num` in the startup
    /// parameter set, on top of pgwire's default fixed parameters.
    #[test]
    fn parameter_provider_advertises_server_version_num_and_nodedb_version() {
        let addr = "127.0.0.1:5432"
            .parse::<std::net::SocketAddr>()
            .expect("valid socket addr");
        let client: DefaultClient<()> = DefaultClient::new(addr, false);
        let provider = NodeDbParameterProvider::new();

        let params = provider
            .server_parameters(&client)
            .expect("provider must yield parameters");

        assert_eq!(
            params.get("server_version_num").map(String::as_str),
            Some(nodedb_types::pg_compat::PG_COMPAT_VERSION_NUM),
            "startup params must advertise server_version_num, got {params:?}"
        );
        assert_eq!(
            params.get("server_version").cloned(),
            Some(format!("NodeDB {}", crate::version::VERSION)),
            "startup params must advertise NodeDB server_version, got {params:?}"
        );
    }
}
