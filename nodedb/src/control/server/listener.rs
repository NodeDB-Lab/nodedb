use std::net::SocketAddr;
use std::sync::Arc;

use tokio::net::TcpListener;
use tracing::{info, warn};

use super::session::Session;
use crate::control::state::SharedState;

/// TCP accept loop for the Control Plane.
///
/// Listens for incoming client connections and spawns a `Session` task for each.
/// This runs on the Tokio runtime (Send + Sync).
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
        mut shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> crate::Result<()> {
        let tls_label = if tls_acceptor.is_some() {
            "tls"
        } else {
            "plain"
        };
        info!(addr = %self.addr, tls = tls_label, "accepting native connections");

        loop {
            tokio::select! {
                result = self.tcp.accept() => {
                    match result {
                        Ok((stream, peer_addr)) => {
                            info!(%peer_addr, "new native connection");
                            let state_clone = Arc::clone(&state);
                            let mode = auth_mode.clone();
                            if let Some(ref acceptor) = tls_acceptor {
                                let acceptor = acceptor.clone();
                                tokio::spawn(async move {
                                    match acceptor.accept(stream).await {
                                        Ok(tls_stream) => {
                                            let session = Session::new_tls(tls_stream, peer_addr, state_clone, mode);
                                            if let Err(e) = session.run().await {
                                                warn!(%peer_addr, error = %e, "TLS session terminated with error");
                                            }
                                        }
                                        Err(e) => {
                                            warn!(%peer_addr, error = %e, "native TLS handshake failed");
                                        }
                                    }
                                });
                            } else {
                                let session = Session::new(stream, peer_addr, state_clone, mode);
                                tokio::spawn(async move {
                                    if let Err(e) = session.run().await {
                                        warn!(%peer_addr, error = %e, "session terminated with error");
                                    }
                                });
                            }
                        }
                        Err(e) => {
                            warn!(error = %e, "accept failed, retrying");
                        }
                    }
                }
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        info!(addr = %self.addr, "shutdown signal received, stopping listener");
                        break;
                    }
                }
            }
        }

        Ok(())
    }
}
