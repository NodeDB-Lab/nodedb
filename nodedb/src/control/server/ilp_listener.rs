// SPDX-License-Identifier: BUSL-1.1

//! ILP (InfluxDB Line Protocol) TCP listener for timeseries ingest.
//!
//! Accepts plain TCP connections on the configured port. Each connection
//! reads newline-delimited ILP lines, parses them, and dispatches
//! `TimeseriesIngest` plans to the Data Plane via SPSC.
//!
//! Protocol: native Hello/Auth prelude followed by one ILP line per newline.
//! The prelude is mandatory; direct unauthenticated ILP clients are rejected.
//!
//! This module holds the accept loop. One connection's prelude, admission
//! and ingest loop live in `ilp_connection`.

use std::net::SocketAddr;
use std::sync::Arc;

use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tracing::{debug, info, warn};

use crate::bridge::envelope::RequestStream;
use crate::config::auth::AuthMode;
use crate::control::server::conn_stream::ConnStream;
use crate::control::server::shared::session::connection_stream;
use crate::control::server::shared::{ConnectionFutureOutcome, isolate_connection_future};
use crate::control::state::SharedState;

#[path = "ilp_batch/mod.rs"]
mod ilp_batch;
#[path = "ilp_connection.rs"]
mod ilp_connection;
#[path = "ilp_drop.rs"]
mod ilp_drop;
#[path = "ilp_line_read.rs"]
mod ilp_line_read;
pub(crate) use ilp_batch::{
    IlpMeasurementBatch, flush_authenticated_ilp_batch, spawn_schema_projection_merge,
};
use ilp_connection::handle_ilp_connection;

/// ILP TCP listener.
pub struct IlpListener {
    tcp: TcpListener,
    addr: SocketAddr,
}

impl IlpListener {
    /// Bind to the given address.
    pub async fn bind(addr: SocketAddr) -> crate::Result<Self> {
        Self::from_listener(TcpListener::bind(addr).await.map_err(crate::Error::Io)?)
    }

    /// Serve on a socket that already listens.
    pub fn from_listener(tcp: TcpListener) -> crate::Result<Self> {
        let local_addr = tcp.local_addr().map_err(crate::Error::Io)?;
        info!(%local_addr, "ILP TCP listener bound");
        Ok(Self {
            tcp,
            addr: local_addr,
        })
    }

    /// Returns the local address the listener is bound to.
    pub fn local_addr(&self) -> std::net::SocketAddr {
        self.addr
    }

    /// Run the accept loop until shutdown.
    pub async fn run(
        self,
        state: Arc<SharedState>,
        auth_mode: AuthMode,
        conn_semaphore: Arc<Semaphore>,
        tls_acceptor: Option<tokio_rustls::TlsAcceptor>,
        startup_gate: Arc<crate::control::startup::StartupGate>,
        bus: crate::control::shutdown::ShutdownBus,
    ) -> crate::Result<()> {
        // This JoinSet owns active ILP connection tasks until their graceful
        // drain (or forced abort) completes, so it gates later shutdown phases.
        let drain_guard = bus.register_critical_task(
            crate::control::shutdown::ShutdownPhase::DrainingListeners,
            "ilp",
        );
        let mut shutdown_handle = bus.handle();

        let tls_label = if tls_acceptor.is_some() {
            "tls"
        } else {
            "plain"
        };
        info!(addr = %self.addr, tls = tls_label, "ILP listener bound — waiting for GatewayEnable");

        startup_gate
            .await_phase(crate::control::startup::StartupPhase::GatewayEnable)
            .await
            .map_err(crate::Error::from)?;

        info!(addr = %self.addr, tls = tls_label, "ILP listener accepting connections");

        let mut connections = tokio::task::JoinSet::new();

        loop {
            tokio::select! {
                result = self.tcp.accept() => {
                    match result {
                        Ok((stream, peer)) => {
                            let permit = match conn_semaphore.clone().try_acquire_owned() {
                                Ok(p) => p,
                                Err(_) => {
                                    debug!(%peer, "ILP connection rejected: max connections");
                                    continue;
                                }
                            };
                            // The connection's writes run on the stream of the
                            // id it takes from the shared allocator.
                            let connection_id = match state.connection_ids.allocate() {
                                Ok(id) => id,
                                Err(error) => {
                                    warn!(%peer, %error, "ILP connection rejected: identifier allocation failed");
                                    continue;
                                }
                            };
                            let request_stream = RequestStream::client(state.node_id, connection_id);
                            let state = Arc::clone(&state);
                            let auth_mode = auth_mode.clone();

                            if let Some(ref acceptor) = tls_acceptor {
                                let acceptor = acceptor.clone();
                                connections.spawn(async move {
                                    let outcome = isolate_connection_future(connection_stream::scoped(request_stream, async move {
                                        let _permit = permit;
                                        match tokio::time::timeout(
                                            std::time::Duration::from_secs(10),
                                            acceptor.accept(stream),
                                        )
                                        .await
                                        {
                                            Ok(Ok(tls_stream)) => {
                                                let cs = ConnStream::tls(tls_stream);
                                                if let Err(e) = handle_ilp_connection(cs, peer, &state, &auth_mode).await {
                                                    warn!(%peer, error = %e, "ILP TLS connection error (data may be lost)");
                                                }
                                            }
                                            Ok(Err(e)) => {
                                                warn!(%peer, error = %e, "ILP TLS handshake failed");
                                            }
                                            Err(_) => {
                                                warn!(%peer, "ILP TLS handshake timed out");
                                            }
                                        }
                                        peer
                                    }))
                                    .await;
                                    if matches!(outcome, ConnectionFutureOutcome::Panicked) {
                                        warn!(%peer, "ILP TLS connection panicked");
                                    }
                                    peer
                                });
                            } else {
                                connections.spawn(async move {
                                    let outcome = isolate_connection_future(connection_stream::scoped(request_stream, async move {
                                        let _permit = permit;
                                        let cs = ConnStream::plain(stream);
                                        if let Err(e) = handle_ilp_connection(cs, peer, &state, &auth_mode).await {
                                            warn!(%peer, error = %e, "ILP connection error (data may be lost)");
                                        }
                                        peer
                                    }))
                                    .await;
                                    if matches!(outcome, ConnectionFutureOutcome::Panicked) {
                                        warn!(%peer, "ILP connection panicked");
                                    }
                                    peer
                                });
                            }
                        }
                        Err(e) => {
                            warn!(error = %e, "ILP accept error");
                        }
                    }
                }
                result = connections.join_next(), if !connections.is_empty() => {
                    if matches!(result, Some(Err(_))) {
                        warn!("ILP connection task ended unexpectedly");
                    }
                }
                _ = shutdown_handle.await_phase(crate::control::shutdown::ShutdownPhase::DrainingListeners) => {
                    info!(addr = %self.addr, "ILP listener shutting down");
                    break;
                }
            }
        }

        // Drain remaining connections with timeout.
        let drain = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while connections.join_next().await.is_some() {}
        });
        if drain.await.is_err() {
            warn!(addr = %self.addr, "ILP connection drain timed out; aborting remaining tasks");
            connections.abort_all();
            while connections.join_next().await.is_some() {}
        }
        drain_guard.report_drained();
        Ok(())
    }
}
