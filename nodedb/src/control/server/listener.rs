use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tracing::{info, warn};

use super::native::session::NativeSession;
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

impl Listener {
    /// Bind to the given address.
    pub async fn bind(addr: SocketAddr) -> crate::Result<Self> {
        let tcp = TcpListener::bind(addr).await?;
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
    pub async fn run(
        self,
        state: Arc<SharedState>,
        auth_mode: crate::config::auth::AuthMode,
        tls_acceptor: Option<tokio_rustls::TlsAcceptor>,
        conn_semaphore: Arc<Semaphore>,
        startup_gate: Arc<crate::control::startup::StartupGate>,
        bus: crate::control::shutdown::ShutdownBus,
    ) -> crate::Result<()> {
        let drain_guard = bus.register_task(
            crate::control::shutdown::ShutdownPhase::DrainingListeners,
            "native",
            None,
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

                            state.connections_accepted.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            info!(%peer_addr, "new native connection");
                            let state_clone = Arc::clone(&state);
                            let mode = auth_mode.clone();
                            if let Some(ref acceptor) = tls_acceptor {
                                let acceptor = acceptor.clone();
                                connections.spawn(async move {
                                    match tokio::time::timeout(
                                        Duration::from_secs(10),
                                        acceptor.accept(stream),
                                    )
                                    .await
                                    {
                                        Ok(Ok(tls_stream)) => {
                                            let session = NativeSession::new_tls(tls_stream, peer_addr, state_clone, mode);
                                            if let Err(e) = session.run().await {
                                                warn!(%peer_addr, error = %e, "TLS session terminated with error");
                                            }
                                        }
                                        Ok(Err(e)) => {
                                            warn!(%peer_addr, error = %e, "native TLS handshake failed");
                                        }
                                        Err(_) => {
                                            warn!(%peer_addr, "native TLS handshake timed out");
                                        }
                                    }
                                    // Permit is held for the session's lifetime and
                                    // released on drop when this future completes.
                                    drop(permit);
                                    peer_addr
                                });
                            } else {
                                let session = NativeSession::new(stream, peer_addr, state_clone, mode);
                                connections.spawn(async move {
                                    if let Err(e) = session.run().await {
                                        warn!(%peer_addr, error = %e, "session terminated with error");
                                    }
                                    drop(permit);
                                    peer_addr
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
                    if let Ok(peer_addr) = result {
                        info!(%peer_addr, "native connection closed");
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
                    if let Ok(peer_addr) = result {
                        info!(%peer_addr, "drained native connection");
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
            }
        }

        info!(addr = %self.addr, "native listener stopped");
        drain_guard.report_drained();
        Ok(())
    }
}
