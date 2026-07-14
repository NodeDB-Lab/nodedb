// SPDX-License-Identifier: BUSL-1.1

use std::sync::Arc;

use pgwire::api::PgWireServerHandlers;
use pgwire::api::auth::StartupHandler;
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};

use crate::config::auth::AuthMode;
use crate::control::security::credential::CredentialStore;
use crate::control::state::SharedState;

use super::super::handler::NodeDbPgHandler;
use super::auth::NodeDbAuthSource;
use super::provider::nodedb_parameter_provider;
use super::startup::AuthStartup;

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
        Arc::new(super::super::handler::NodeDbCopyHandler {
            state: Arc::clone(&self.state),
            restore_state: Arc::clone(&self.handler.restore_state),
        })
    }

    fn startup_handler(&self) -> Arc<impl StartupHandler> {
        match self.auth_mode {
            AuthMode::Trust => Arc::new(AuthStartup::Trust(self.handler.clone())),
            AuthMode::Password | AuthMode::Certificate => {
                let auth_source = Arc::new(NodeDbAuthSource::new(
                    Arc::clone(&self.credentials),
                    Arc::clone(&self.state),
                ));
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
