// SPDX-License-Identifier: BUSL-1.1

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tracing::{info, warn};

use super::admission::AdmissionRegistry;
use super::native::session::{NativeConnectionResources, NativeSession};
use crate::control::server::shared::session::connection_stream;
use crate::control::server::shared::{ConnectionFutureOutcome, isolate_connection_future};
use crate::control::state::SharedState;

/// TCP accept loop for the Control Plane.
///
/// Listens for incoming client connections and spawns a `Session` task for each.
/// This runs on the Tokio runtime (Send + Sync).
///
/// A shared `Semaphore` limits concurrent connections across all listeners.
/// If no permit is available, the accepted TCP socket is immediately dropped
/// (RST), preventing connection floods from exhausting memory before
/// per-tenant quotas kick in.
///
/// On shutdown: stops accepting, waits up to 30s for active sessions to drain,
/// then aborts remaining connections.
pub struct Listener {
    tcp: TcpListener,
    addr: SocketAddr,
}

/// Parameters for [`Listener::run`].
pub struct ListenerRunParams {
    pub state: Arc<SharedState>,
    pub auth_mode: crate::config::auth::AuthMode,
    pub tls_acceptor: Option<tokio_rustls::TlsAcceptor>,
    pub conn_semaphore: Arc<Semaphore>,
    pub startup_gate: Arc<crate::control::startup::StartupGate>,
    pub bus: crate::control::shutdown::ShutdownBus,
    pub admission: Arc<AdmissionRegistry>,
}

impl Listener {
    /// Bind to the given address.
    pub async fn bind(addr: SocketAddr) -> crate::Result<Self> {
        Self::from_listener(TcpListener::bind(addr).await?)
    }

    /// Serve on a socket that already listens.
    pub fn from_listener(tcp: TcpListener) -> crate::Result<Self> {
        let local_addr = tcp.local_addr()?;
        info!(%local_addr, "control plane listener bound");
        Ok(Self {
            tcp,
            addr: local_addr,
        })
    }

    /// Returns the address the listener is bound to.
    pub fn local_addr(&self) -> SocketAddr {
        self.addr
    }

    /// Run the accept loop, spawning a Tokio task per connection.
    ///
    /// Each session receives a reference to the shared state for dispatching
    /// requests to the Data Plane and accessing the WAL.
    /// Supports optional TLS if a `tls_acceptor` is provided.
    pub async fn run(self, params: ListenerRunParams) -> crate::Result<()> {
        let ListenerRunParams {
            state,
            auth_mode,
            tls_acceptor,
            conn_semaphore,
            startup_gate,
            bus,
            admission,
        } = params;
        // Native cancellation can leave a transaction overlay that must be
        // reclaimed before the Data Plane is allowed to drain. This listener
        // therefore forms a shutdown correctness barrier rather than a
        // best-effort, budget-abortable task.
        let drain_guard = bus.register_critical_task(
            crate::control::shutdown::ShutdownPhase::DrainingListeners,
            "native",
        );
        let mut shutdown_handle = bus.handle();

        let tls_label = if tls_acceptor.is_some() {
            "tls"
        } else {
            "plain"
        };
        info!(
            addr = %self.addr,
            tls = tls_label,
            "native listener bound — waiting for GatewayEnable"
        );

        // Block until startup is complete before accepting real connections.
        startup_gate
            .await_phase(crate::control::startup::StartupPhase::GatewayEnable)
            .await
            .map_err(crate::Error::from)?;

        info!(
            addr = %self.addr,
            tls = tls_label,
            max_permits = conn_semaphore.available_permits(),
            "accepting native connections"
        );

        let mut connections = JoinSet::new();
        let mut cleanups = HashMap::new();

        loop {
            tokio::select! {
                result = self.tcp.accept() => {
                    match result {
                        Ok((stream, peer_addr)) => {
                            // Acquire connection permit. try_acquire is non-blocking:
                            // if no permits are available, drop the socket immediately
                            // (TCP RST) rather than queueing.
                            let permit = match conn_semaphore.clone().try_acquire_owned() {
                                Ok(permit) => permit,
                                Err(_) => {
                                    state.connections_rejected.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                    warn!(
                                        %peer_addr,
                                        active = conn_semaphore.available_permits(),
                                        "connection rejected: max_connections limit reached"
                                    );
                                    // `stream` is dropped here → TCP RST to client.
                                    continue;
                                }
                            };

                            let connection_id = match state.connection_ids.allocate() {
                                Ok(connection_id) => connection_id,
                                Err(error) => {
                                    state.connections_rejected.fetch_add(1, Ordering::Relaxed);
                                    warn!(%peer_addr, %error, "native connection rejected: identifier allocation failed");
                                    continue;
                                }
                            };
                            state.connections_accepted.fetch_add(1, Ordering::Relaxed);
                            info!(%peer_addr, connection_id = %connection_id, "new native connection");
                            let resources =
                                NativeConnectionResources::new(peer_addr, Arc::clone(&state));
                            let cleanup = resources.cleanup();
                            cleanups.insert(connection_id, cleanup.clone());
                            let state_clone = Arc::clone(&state);
                            let mode = auth_mode.clone();
                            let admission_clone = Arc::clone(&admission);
                            if let Some(ref acceptor) = tls_acceptor {
                                let acceptor = acceptor.clone();
                                connections.spawn(async move {
                                    let outcome = isolate_connection_future(connection_stream::scoped(connection_id, async move {
                                        let _permit = permit;
                                        match tokio::time::timeout(
                                            Duration::from_secs(10),
                                            acceptor.accept(stream),
                                        )
                                        .await
                                        {
                                            Ok(Ok(tls_stream)) => {
                                                let session = NativeSession::new_tls(
                                                    tls_stream,
                                                    peer_addr,
                                                    state_clone,
                                                    mode,
                                                    admission_clone,
                                                    _permit,
                                                    resources,
                                                );
                                                if let Err(e) = session.run().await {
                                                    warn!(%peer_addr, error = %e, "TLS session terminated with error");
                                                }
                                            }
                                            Ok(Err(e)) => warn!(%peer_addr, error = %e, "native TLS handshake failed"),
                                            Err(_) => warn!(%peer_addr, "native TLS handshake timed out"),
                                        }
                                    }))
                                    .await;
                                    if matches!(outcome, ConnectionFutureOutcome::Panicked) {
                                        warn!(%peer_addr, "native TLS connection panicked");
                                    }
                                    cleanup.start_and_wait().await;
                                    connection_id
                                });
                            } else {
                                connections.spawn(async move {
                                    let outcome = isolate_connection_future(connection_stream::scoped(connection_id, async move {
                                        let _permit = permit;
                                        let session = NativeSession::new(
                                            stream,
                                            peer_addr,
                                            state_clone,
                                            mode,
                                            admission_clone,
                                            _permit,
                                            resources,
                                        );
                                        if let Err(e) = session.run().await {
                                            warn!(%peer_addr, error = %e, "session terminated with error");
                                        }
                                    }))
                                    .await;
                                    if matches!(outcome, ConnectionFutureOutcome::Panicked) {
                                        warn!(%peer_addr, "native connection panicked");
                                    }
                                    cleanup.start_and_wait().await;
                                    connection_id
                                });
                            }
                        }
                        Err(e) => {
                            warn!(error = %e, "accept failed, retrying");
                        }
                    }
                }
                // Reap completed connections.
                Some(result) = connections.join_next(), if !connections.is_empty() => {
                    match result {
                        Ok(connection_id) => {
                            cleanups.remove(&connection_id);
                            info!(connection_id = %connection_id, "native connection closed");
                        }
                        Err(_) => warn!("native connection task ended unexpectedly"),
                    }
                }
                _ = shutdown_handle.await_phase(crate::control::shutdown::ShutdownPhase::DrainingListeners) => {
                    info!(
                        addr = %self.addr,
                        active = connections.len(),
                        "shutdown signal, draining native connections"
                    );
                    break;
                }
            }
        }

        // Graceful drain: wait for in-flight connections with timeout.
        let drain_timeout = Duration::from_secs(30);
        if !connections.is_empty() {
            info!(
                active = connections.len(),
                timeout_secs = drain_timeout.as_secs(),
                "waiting for native connections to drain"
            );

            let drain_result = tokio::time::timeout(drain_timeout, async {
                while let Some(result) = connections.join_next().await {
                    match result {
                        Ok(connection_id) => {
                            cleanups.remove(&connection_id);
                            info!(connection_id = %connection_id, "drained native connection");
                        }
                        Err(_) => warn!("native connection task ended unexpectedly while draining"),
                    }
                }
            })
            .await;

            if drain_result.is_err() {
                let remaining = connections.len();
                warn!(
                    remaining,
                    "drain timeout exceeded, aborting remaining native connections"
                );
                connections.abort_all();
                // Join every aborted task before shutdown advances: this runs
                // connection-future drops and releases their permits.
                while let Some(result) = connections.join_next().await {
                    if let Ok(connection_id) = result {
                        cleanups.remove(&connection_id);
                    }
                }
            }
        }

        // Aborted tasks cannot return their ID, so cleanup ownership remains in
        // this exact-ID map until after the join. Start every cleanup before any
        // await, then await their sticky completions before reporting drained.
        let retained_cleanups: Vec<_> = cleanups.into_values().collect();
        for cleanup in &retained_cleanups {
            cleanup.start();
        }
        for cleanup in retained_cleanups {
            cleanup.start_and_wait().await;
        }

        info!(addr = %self.addr, "native listener stopped");
        drain_guard.report_drained();
        Ok(())
    }
}
