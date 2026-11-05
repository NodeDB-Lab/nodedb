// SPDX-License-Identifier: BUSL-1.1

use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::sink::Sink;

use pgwire::api::ClientInfo;
use pgwire::api::auth::StartupHandler;
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};

use crate::control::security::audit::{ArcAuditEmitter, AuditEvent};
use crate::control::security::credential::store::{AuthRejection, ScramLookup};
use crate::control::security::escalation::{
    AuthViolation, ViolationSubject, record_auth_violation,
};
use crate::control::security::identity::{AuthMethod, AuthenticatedIdentity};
use crate::control::security::sessions::{KillReason, SessionBindRequest, bind_session};
use crate::control::server::admission::{AdmissionError, ScopedConnectionPermit};
use crate::control::server::session_auth::identity::stored_user_identity;
use crate::control::server::shared::session::{SessionId, now_unix_ms};
use crate::control::state::SharedState;

use super::super::handler::NodeDbPgHandler;
use super::super::types::error_to_sqlstate;
use super::provider::NodeDbParameterProvider;

/// Enum dispatch for startup handler — avoids dyn trait object issues.
pub(crate) enum AuthStartup {
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
/// An absent or empty value leaves `current_database` unset, and the session
/// falls back to the caller's default — the one legitimate meaning of `None`.
///
/// A name that does not resolve is refused here with `3D000`, matching
/// `USE DATABASE`, the native protocol, the HTTP path, and PostgreSQL
/// (`FATAL: database "x" does not exist`). It must not be encoded as `None`:
/// consumers read `None` as "no database selected" and fall back to
/// `DatabaseId::DEFAULT`, so an unresolved name would silently read and write
/// `default` while the client believes it named something else.
///
/// A catalog read failure is refused for the same reason — it says nothing
/// about whether the database exists, so it cannot become a fallback.
///
/// Every refusal is `FATAL`. A non-fatal error leaves the connection open, and
/// on the SCRAM path, where ReadyForQuery is already sent, the client reads the
/// error as the reply to its first statement.
fn bind_startup_database<C: pgwire::api::ClientInfo>(
    client: &C,
    session_id: crate::control::server::shared::session::SessionId,
    handler: &NodeDbPgHandler,
) -> PgWireResult<()> {
    let db_name = match client.metadata().get("database") {
        Some(n) if !n.is_empty() => n.clone(),
        _ => return Ok(()),
    };

    let db_id = handler
        .state
        .credentials
        .catalog()
        .get_database_id_by_name(&db_name)
        .map_err(|e| {
            let (_, code, message) = error_to_sqlstate(&e);
            startup_refusal(code, format!("catalog lookup failed: {message}"))
        })?
        .ok_or_else(|| startup_refusal("3D000", format!("database '{db_name}' does not exist")))?;

    handler.sessions.set_current_database(session_id, db_id);
    Ok(())
}

/// Take this connection's database- and tenant-scoped connection slots and park
/// them on its session.
///
/// The caps are keyed by the bound database and the authenticated tenant, so
/// this runs after [`bind_startup_database`] and after the identity resolves.
/// The database is read the way every pgwire query path reads it, so admission
/// counts the connection against the database its statements will run in.
///
/// [`SessionStore::remove`] releases the slots at connection teardown, which
/// the pgwire listener runs on normal close, timeout, cancellation, and a
/// panicking session alike.
///
/// [`SessionStore::remove`]: crate::control::server::shared::session::SessionStore::remove
fn admit_connection(
    handler: &NodeDbPgHandler,
    identity: &AuthenticatedIdentity,
) -> PgWireResult<()> {
    let db_id = handler
        .sessions
        .get_current_database(handler.session_id)
        .unwrap_or(crate::types::DatabaseId::DEFAULT);

    let permit = ScopedConnectionPermit::acquire(
        &handler.state.admission_registry,
        db_id,
        identity.tenant_id,
    )
    .map_err(|error| admission_refusal(&error))?;

    if !handler
        .sessions
        .set_admission_permit(handler.session_id, permit)
    {
        return Err(startup_refusal(
            nodedb_types::error::sqlstate::INTERNAL_ERROR,
            "internal error: connection session is not registered".to_owned(),
        ));
    }
    Ok(())
}

/// How often a killed connection re-checks that its last statement finished.
const KILL_POLL: Duration = Duration::from_millis(50);
/// How long a killed connection stays silent before it closes, so pgwire flushes
/// the response of the statement that just finished.
const KILL_FLUSH_GRACE_MS: u64 = 200;

/// Register this connection in the session registry and close it on a kill.
///
/// pgwire owns the read loop, and the listener closes a connection by dropping
/// that loop's future. A dropped statement can lose work that follows a committed
/// proposal, such as an audit row. So the watch task closes the connection only
/// with no statement in flight.
/// `resolve_session_identity` refuses new statements of a killed session, so the
/// in-flight count reaches zero. The task exits with the registry row.
fn bind_connection<C: ClientInfo>(
    client: &C,
    handler: &NodeDbPgHandler,
    identity: &AuthenticatedIdentity,
) -> PgWireResult<()> {
    let db_id = handler
        .sessions
        .get_current_database(handler.session_id)
        .unwrap_or(crate::types::DatabaseId::DEFAULT);
    let peer = client.socket_addr().to_string();
    let binding = bind_session(
        &handler.state,
        SessionBindRequest::new(identity, "pgwire", &peer).in_database(db_id),
    )
    .map_err(|error| {
        let (_, code, message) = error_to_sqlstate(&error);
        startup_refusal(code, message)
    })?;
    let binding = Arc::new(binding);
    let mut kill = binding.kill_signal();
    if !handler.sessions.set_binding(handler.session_id, binding) {
        return Err(startup_refusal(
            nodedb_types::error::sqlstate::INTERNAL_ERROR,
            "internal error: connection session is not registered".to_owned(),
        ));
    }

    let SessionId::Connection(connection_id) = handler.session_id else {
        return Err(startup_refusal(
            nodedb_types::error::sqlstate::INTERNAL_ERROR,
            "internal error: pgwire session has no connection id".to_owned(),
        ));
    };
    let sessions = Arc::clone(&handler.sessions);
    let registry = Arc::clone(&handler.registry);
    tokio::spawn(async move {
        while *kill.borrow_and_update() == KillReason::Alive {
            if kill.changed().await.is_err() {
                return;
            }
        }
        while kill.has_changed().is_ok() {
            if sessions.idle_eligible(connection_id, KILL_FLUSH_GRACE_MS, now_unix_ms()) {
                registry.request_cancel(connection_id);
                return;
            }
            tokio::time::sleep(KILL_POLL).await;
        }
    });
    Ok(())
}

/// pgwire refusal for an exhausted connection cap.
///
/// `FATAL` closes the connection. The SQLSTATE and the message are the ones the
/// native protocol reports for the same condition, so a cap refusal reads the
/// same on both protocols.
fn admission_refusal(error: &AdmissionError) -> PgWireError {
    startup_refusal(
        nodedb_types::error::sqlstate::QUOTA_EXCEEDED,
        error.to_string(),
    )
}

/// A `FATAL` startup refusal. pgwire closes the connection after sending it.
fn startup_refusal(code: &str, message: String) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "FATAL".to_owned(),
        code.to_owned(),
        message,
    )))
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
                    // Reject unknown trust users before we announce AuthenticationOk,
                    // then bind the resolved identity for this socket. Trust
                    // identities must remain connection-local, including the
                    // empty-store bootstrap identity.
                    let identity = handler.resolve_trust_user(client)?;
                    handler
                        .sessions
                        .set_identity(handler.session_id, identity.clone());
                    // Before AuthenticationOk, for the same reason the trust
                    // user is: once `finish_authentication` announces
                    // ReadyForQuery the client treats the connection as
                    // established, and a refusal after that point arrives too
                    // late to stop it being used.
                    bind_startup_database(client, handler.session_id, handler)?;
                    // The connection caps are keyed by the database bound just
                    // above, so admission runs here — still ahead of
                    // AuthenticationOk, for the same reason.
                    admit_connection(handler, &identity)?;
                    bind_connection(client, handler, &identity)?;
                    pgwire::api::auth::finish_authentication(
                        client,
                        &super::provider::nodedb_parameter_provider(),
                    )
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
                        //
                        // This lands after AuthenticationOk, unlike the trust
                        // path, and deliberately so: the SASL handler owns the
                        // whole exchange through ReadyForQuery, so the only
                        // earlier hook is the Startup message — before the
                        // client has authenticated. Refusing there would tell
                        // an unauthenticated caller which database names
                        // exist. The session is instead torn down by this
                        // error before it can execute anything, so an
                        // unresolved name still never reaches `default`.
                        bind_startup_database(client, handler.session_id, handler)?;
                        // Admission lands here for the same reason the bind
                        // does — the SASL handler owns the exchange through
                        // ReadyForQuery, so this is the first hook where the
                        // client is authenticated and the database is known.
                        // The error returned here tears the session down
                        // before it can execute a statement, so a refused
                        // connection still never runs anything.
                        //
                        // The identity is re-read from the credential store
                        // under the authenticated username, the way every
                        // pgwire query path resolves a password-mode identity.
                        let identity = stored_user_identity(
                            state,
                            &username,
                            AuthMethod::ScramSha256,
                        )
                        .ok_or_else(|| {
                            PgWireError::UserError(Box::new(ErrorInfo::new(
                                "FATAL".to_owned(),
                                "28000".to_owned(),
                                format!(
                                    "authenticated user '{username}' not found in credential store"
                                ),
                            )))
                        })?;
                        admit_connection(handler, &identity)?;
                        bind_connection(client, handler, &identity)?;
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
                        // Escalation follows the same `counts` gate as the
                        // lockout and brute-force counters: a rate-limited or
                        // policy-rejected SASL failure says nothing about the
                        // client's proof, so it is audited without being
                        // counted against the account.
                        let subject = if counts {
                            ViolationSubject::Username(username.as_str())
                        } else {
                            ViolationSubject::AuditOnly
                        };
                        record_auth_violation(
                            state,
                            AuthViolation {
                                subject,
                                tenant_id: None,
                                source: &source,
                                detail: &format!("SCRAM-SHA-256 auth failed: {username}"),
                            },
                        );
                    }
                    _ => {}
                }

                result
            }
        }
    }
}
