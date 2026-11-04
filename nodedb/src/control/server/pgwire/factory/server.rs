// SPDX-License-Identifier: BUSL-1.1

use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use futures::FutureExt;
use pgwire::api::NoopHandler;
use tokio::sync::watch;
use tracing::warn;

use crate::config::auth::AuthMode;
use crate::control::security::credential::CredentialStore;
use crate::control::server::shared::session::{
    ConnectionId, ConnectionMetadata, ConnectionRegistrationError, SessionStore,
};
use crate::control::state::SharedState;

use super::super::connection_identity::PgConnectionContext;
use super::super::connection_registry::{ConnectionRegistry, ConnectionRegistryError};
use super::super::handler::{ArrayCopyState, NodeDbCopyHandler, NodeDbPgHandler};
use super::super::wire_safe_error::WireSafeErrorHandler;
use super::auth::NodeDbAuthSource;
use super::provider::nodedb_parameter_provider;
use super::startup::AuthStartup;

/// Factory that wires together the pgwire handlers.
///
/// Supports trust mode (handshake with trust user-gating) and password mode
/// (SCRAM-SHA-256 via pgwire's SASL implementation). Both paths announce
/// startup parameters through `NodeDbParameterProvider`.
pub(crate) struct ConnectionHandlers {
    pub(crate) startup: Arc<AuthStartup>,
    pub(crate) query: Arc<NodeDbPgHandler>,
    pub(crate) copy: Arc<NodeDbCopyHandler>,
    pub(crate) cancel: Arc<NoopHandler>,
    /// Last-stop wire safety: rewrites any control byte an error message picked
    /// up from a stored value or a foreign error's `Display` before pgwire
    /// serialises it, since an interior NUL desynchronises the whole frame.
    pub(crate) error: Arc<WireSafeErrorHandler>,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum FactoryConnectionRegistrationError {
    #[error(transparent)]
    Session(#[from] ConnectionRegistrationError),
    #[error(transparent)]
    Registry(#[from] ConnectionRegistryError),
}

struct TeardownCompletion {
    registry: Arc<ConnectionRegistry>,
    id: ConnectionId,
    control: Arc<super::super::connection_registry::ConnectionControl>,
}

impl Drop for TeardownCompletion {
    fn drop(&mut self) {
        self.registry.complete_teardown(self.id, &self.control);
    }
}

pub struct NodeDbPgHandlerFactory {
    auth_mode: AuthMode,
    credentials: Arc<CredentialStore>,
    state: Arc<SharedState>,
    sessions: Arc<SessionStore>,
    restore_state: Arc<crate::control::backup::RestoreState>,
    array_copy: Arc<ArrayCopyState>,
    registry: Arc<ConnectionRegistry>,
}

impl NodeDbPgHandlerFactory {
    pub fn new(state: Arc<SharedState>, auth_mode: AuthMode) -> Self {
        let sessions = Arc::new(SessionStore::new());
        state.connection_tracker.register_pgwire(&sessions);
        Self {
            auth_mode,
            credentials: Arc::clone(&state.credentials),
            state,
            sessions,
            restore_state: Arc::new(crate::control::backup::RestoreState::new()),
            array_copy: Arc::new(ArrayCopyState::new()),
            registry: Arc::new(ConnectionRegistry::new()),
        }
    }

    pub(crate) fn register_connection(
        &self,
        context: PgConnectionContext,
    ) -> Result<watch::Receiver<bool>, FactoryConnectionRegistrationError> {
        self.sessions.register_connection(
            context.id,
            ConnectionMetadata {
                peer_addr: context.peer_addr,
                local_addr: context.local_addr,
            },
        )?;
        match self.registry.register(context.id) {
            Ok(cancel) => Ok(cancel),
            Err(error) => {
                self.sessions.remove(context.id);
                Err(error.into())
            }
        }
    }

    pub(crate) fn connection_handlers(&self, context: PgConnectionContext) -> ConnectionHandlers {
        let handler = Arc::new(NodeDbPgHandler::for_connection(
            Arc::clone(&self.state),
            self.auth_mode.clone(),
            Arc::clone(&self.sessions),
            Arc::clone(&self.restore_state),
            Arc::clone(&self.array_copy),
            Arc::clone(&self.registry),
            context.id,
        ));
        let startup = match self.auth_mode {
            AuthMode::Trust => Arc::new(AuthStartup::Trust(Arc::clone(&handler))),
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
                    handler: Arc::clone(&handler),
                })
            }
        };
        ConnectionHandlers {
            startup,
            query: handler,
            copy: Arc::new(NodeDbCopyHandler {
                state: Arc::clone(&self.state),
                restore_state: Arc::clone(&self.restore_state),
                array_copy: Arc::clone(&self.array_copy),
                sessions: Arc::clone(&self.sessions),
                connection_id: context.id,
            }),
            cancel: Arc::new(NoopHandler),
            error: Arc::new(WireSafeErrorHandler),
        }
    }

    async fn reclaim_open_txn(&self, id: ConnectionId) {
        let handler = NodeDbPgHandler::for_connection(
            Arc::clone(&self.state),
            self.auth_mode.clone(),
            Arc::clone(&self.sessions),
            Arc::clone(&self.restore_state),
            Arc::clone(&self.array_copy),
            Arc::clone(&self.registry),
            id,
        );
        handler.reclaim_open_txn(id.into()).await;
    }

    /// Start exact-ID teardown once. The first caller launches cleanup before
    /// awaiting; every concurrent normal or forced-drain caller waits for the
    /// same sticky completion.
    pub async fn on_connection_end(
        self: &Arc<Self>,
        id: ConnectionId,
        peer_addr: std::net::SocketAddr,
    ) {
        let Some((control, first)) = self.registry.begin_teardown(id) else {
            return;
        };
        if first {
            let factory = Arc::clone(self);
            let cleanup_control = Arc::clone(&control);
            tokio::spawn(async move {
                factory
                    .cleanup_connection(id, peer_addr, cleanup_control)
                    .await;
            });
        }
        ConnectionRegistry::wait_for_teardown(&control).await;
    }

    async fn cleanup_connection(
        self: Arc<Self>,
        id: ConnectionId,
        peer_addr: std::net::SocketAddr,
        control: Arc<super::super::connection_registry::ConnectionControl>,
    ) {
        // Publish completion even if an unexpected panic escapes one of the
        // individually isolated cleanup steps. The guard contains no awaits.
        let _completion = TeardownCompletion {
            registry: Arc::clone(&self.registry),
            id,
            control,
        };
        if AssertUnwindSafe(self.reclaim_open_txn(id))
            .catch_unwind()
            .await
            .is_err()
        {
            warn!(connection_id = id.get(), %peer_addr, "pgwire transaction reclamation panicked");
        }
        if std::panic::catch_unwind(AssertUnwindSafe(|| self.restore_state.cancel(id.get())))
            .is_err()
        {
            warn!(connection_id = id.get(), %peer_addr, "pgwire restore cleanup panicked");
        }
        if std::panic::catch_unwind(AssertUnwindSafe(|| self.array_copy.cancel(id.get()))).is_err()
        {
            warn!(connection_id = id.get(), %peer_addr, "pgwire array COPY cleanup panicked");
        }
        if std::panic::catch_unwind(AssertUnwindSafe(|| {
            self.sessions
                .cleanup_listen_on_disconnect(id.into(), &self.state.notify_bus);
        }))
        .is_err()
        {
            warn!(connection_id = id.get(), %peer_addr, "pgwire LISTEN cleanup panicked");
        }
        if std::panic::catch_unwind(AssertUnwindSafe(|| self.sessions.remove(id))).is_err() {
            warn!(connection_id = id.get(), %peer_addr, "pgwire session removal panicked");
        }
    }

    /// Whether the connection `id` is eligible for idle timeout right now:
    /// its session has zero statements in flight and has been silent for at
    /// least `idle_ms`. Used by the pgwire listener watchdog, which owns the
    /// per-connection task but cannot see inside pgwire's `process_socket`
    /// loop. Returns `false` when the session is missing (nothing to time out).
    pub fn session_idle_eligible(&self, id: ConnectionId, idle_ms: u64) -> bool {
        self.sessions.idle_eligible(
            id,
            idle_ms,
            crate::control::server::shared::session::now_unix_ms(),
        )
    }
}
