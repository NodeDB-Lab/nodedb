// SPDX-License-Identifier: BUSL-1.1

use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use futures::sink::Sink;

use pgwire::api::ClientInfo;
use pgwire::api::auth::StartupHandler;
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};

use crate::control::security::audit::{ArcAuditEmitter, AuditEvent};
use crate::control::security::credential::store::{AuthRejection, ScramLookup};
use crate::control::state::SharedState;

use super::super::handler::NodeDbPgHandler;
use super::provider::NodeDbParameterProvider;

/// Enum dispatch for startup handler — avoids dyn trait object issues.
pub(super) enum AuthStartup {
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
                    // Reject unknown trust users before we announce AuthenticationOk,
                    // then bind the resolved identity for this socket. Trust
                    // identities must remain connection-local, including the
                    // empty-store bootstrap identity.
                    let identity = handler.resolve_trust_user(client)?;
                    let addr = client.socket_addr();
                    handler.sessions.ensure_session(addr);
                    handler.sessions.set_identity(&addr, identity);
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
