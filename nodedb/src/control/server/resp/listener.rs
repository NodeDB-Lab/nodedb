// SPDX-License-Identifier: BUSL-1.1

//! RESP TCP listener: accepts connections and spawns session handlers.
//!
//! Runs on the Control Plane (Tokio). Each connection is handled by a
//! dedicated async task that reads RESP frames, dispatches to the Data
//! Plane via SPSC bridge, and writes responses back.

use std::net::SocketAddr;
use std::sync::Arc;

use futures::future::BoxFuture;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tracing::{debug, info, warn};

use crate::control::security::sessions::session_ended;
use crate::control::server::conn_stream::ConnStream;
use crate::control::state::SharedState;

use super::codec::RespParser;
use super::command::RespCommand;
use super::handler;
use super::session::RespSession;

/// Default port for the RESP protocol listener.
pub const DEFAULT_RESP_PORT: u16 = 6381;

/// RESP protocol listener.
pub struct RespListener {
    tcp: TcpListener,
    addr: SocketAddr,
}

impl RespListener {
    /// Bind the RESP listener to the given address.
    pub async fn bind(addr: SocketAddr) -> crate::Result<Self> {
        let tcp = TcpListener::bind(addr)
            .await
            .map_err(|e| crate::Error::Config {
                detail: format!("failed to bind RESP listener on {addr}: {e}"),
            })?;
        Self::from_listener(tcp)
    }

    /// Serve on a socket that already listens.
    pub fn from_listener(tcp: TcpListener) -> crate::Result<Self> {
        let local_addr = tcp.local_addr().map_err(|e| crate::Error::Config {
            detail: format!("failed to get RESP local address: {e}"),
        })?;
        info!(%local_addr, "RESP (Redis-compatible) listener bound");
        Ok(Self {
            tcp,
            addr: local_addr,
        })
    }

    /// Local address the listener is bound to.
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Run the accept loop. Blocks until shutdown signal.
    pub async fn run(
        self,
        state: Arc<SharedState>,
        conn_semaphore: Arc<Semaphore>,
        tls_acceptor: Option<tokio_rustls::TlsAcceptor>,
        startup_gate: Arc<crate::control::startup::StartupGate>,
        bus: crate::control::shutdown::ShutdownBus,
    ) -> crate::Result<()> {
        // This JoinSet owns active RESP connection tasks until their graceful
        // drain (or forced abort) completes, so it gates later shutdown phases.
        let drain_guard = bus.register_critical_task(
            crate::control::shutdown::ShutdownPhase::DrainingListeners,
            "resp",
        );
        let mut shutdown_handle = bus.handle();

        let tls_label = if tls_acceptor.is_some() {
            "tls"
        } else {
            "plain"
        };
        info!(addr = %self.addr, tls = tls_label, "RESP listener bound — waiting for GatewayEnable");

        startup_gate
            .await_phase(crate::control::startup::StartupPhase::GatewayEnable)
            .await
            .map_err(crate::Error::from)?;

        info!(addr = %self.addr, tls = tls_label, "RESP listener accepting connections");

        let mut connections = tokio::task::JoinSet::new();

        loop {
            tokio::select! {
                result = self.tcp.accept() => {
                    match result {
                        Ok((stream, peer)) => {
                            let permit = match conn_semaphore.clone().try_acquire_owned() {
                                Ok(p) => p,
                                Err(_) => {
                                    debug!(%peer, "RESP connection rejected: max connections reached");
                                    continue;
                                }
                            };

                            let state = Arc::clone(&state);

                            if let Some(ref acceptor) = tls_acceptor {
                                let acceptor = acceptor.clone();
                                connections.spawn(async move {
                                    match tokio::time::timeout(
                                        std::time::Duration::from_secs(10),
                                        acceptor.accept(stream),
                                    )
                                    .await
                                    {
                                        Ok(Ok(tls_stream)) => {
                                            let cs = ConnStream::tls(tls_stream);
                                            if let Err(e) = handle_connection(cs, peer, &state).await {
                                                debug!(%peer, error = %e, "RESP TLS connection error");
                                            }
                                        }
                                        Ok(Err(e)) => {
                                            warn!(%peer, error = %e, "RESP TLS handshake failed");
                                        }
                                        Err(_) => {
                                            warn!(%peer, "RESP TLS handshake timed out");
                                        }
                                    }
                                    drop(permit);
                                });
                            } else {
                                connections.spawn(async move {
                                    let cs = ConnStream::plain(stream);
                                    if let Err(e) = handle_connection(cs, peer, &state).await {
                                        debug!(%peer, error = %e, "RESP connection error");
                                    }
                                    drop(permit);
                                });
                            }
                        }
                        Err(e) => {
                            warn!(error = %e, "RESP accept error");
                        }
                    }
                }
                _ = shutdown_handle.await_phase(crate::control::shutdown::ShutdownPhase::DrainingListeners) => {
                    info!("RESP listener shutting down");
                    break;
                }
            }
        }

        // Drain active connections (5 second timeout).
        let drain_deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        while !connections.is_empty() {
            tokio::select! {
                _ = connections.join_next() => {}
                _ = tokio::time::sleep_until(drain_deadline) => {
                    debug!(
                        remaining = connections.len(),
                        "RESP drain timeout, aborting remaining connections"
                    );
                    connections.abort_all();
                    break;
                }
            }
        }

        drain_guard.report_drained();
        Ok(())
    }
}

/// Tell the client why its session ended. The caller closes the connection.
async fn write_session_end(stream: &mut ConnStream, ended: &crate::Error, write_buf: &mut Vec<u8>) {
    write_buf.clear();
    super::codec::RespValue::from_error(ended).serialize(write_buf);
    let _ = stream.write_all(write_buf).await;
}

/// Handle a single RESP connection.
///
/// The connection future is boxed, once per connection. Every command path
/// nests inside it, and unboxed it can overflow the compiler's layout depth
/// limit in the listener's connection task.
fn handle_connection(
    stream: ConnStream,
    peer: SocketAddr,
    state: &SharedState,
) -> BoxFuture<'_, crate::Result<()>> {
    Box::pin(serve_connection(stream, peer, state))
}

async fn serve_connection(
    mut stream: ConnStream,
    peer: SocketAddr,
    state: &SharedState,
) -> crate::Result<()> {
    debug!(%peer, "RESP connection accepted");

    let mut session = RespSession {
        peer_addr: peer.to_string(),
        transport: stream.transport_security(),
        ..RespSession::default()
    };
    let mut parser = RespParser::new();
    let mut read_buf = [0u8; 8192];
    let mut write_buf = Vec::with_capacity(4096);

    loop {
        // Read data from the socket, racing the kill signal. A command in
        // progress finishes first: the kill is observed here or in the refresh
        // ahead of the next pipelined command.
        let read = tokio::select! {
            biased;
            ended = session_ended(&mut session.binding) => {
                write_session_end(&mut stream, &ended, &mut write_buf).await;
                return Ok(());
            }
            read = stream.read(&mut read_buf) => read,
        };
        let n = match read {
            Ok(0) => {
                debug!(%peer, "RESP connection closed");
                return Ok(());
            }
            Ok(n) => n,
            Err(e) => {
                debug!(%peer, error = %e, "RESP read error");
                return Err(crate::Error::Bridge {
                    detail: format!("RESP read: {e}"),
                });
            }
        };

        if let Err(e) = parser.feed(&read_buf[..n]) {
            warn!(%peer, error = %e, "RESP frame exceeds parser limits");
            let resp = super::codec::RespValue::err(format!("ERR protocol error: {e}"));
            write_buf.clear();
            resp.serialize(&mut write_buf);
            let _ = stream.write_all(&write_buf).await;
            return Ok(());
        }

        // Process all complete frames in the buffer.
        loop {
            match parser.try_parse() {
                Ok(Some(value)) => {
                    let Some(cmd) = RespCommand::parse(&value) else {
                        let resp = super::codec::RespValue::err("ERR invalid command format");
                        write_buf.clear();
                        resp.serialize(&mut write_buf);
                        stream
                            .write_all(&write_buf)
                            .await
                            .map_err(|e| crate::Error::Bridge {
                                detail: format!("RESP write: {e}"),
                            })?;
                        continue;
                    };

                    // Check for QUIT.
                    if cmd.name == "QUIT" {
                        let resp = super::codec::RespValue::ok();
                        write_buf.clear();
                        resp.serialize(&mut write_buf);
                        let _ = stream.write_all(&write_buf).await;
                        return Ok(());
                    }

                    if let Err(ended) = session.refresh(state) {
                        write_session_end(&mut stream, &ended, &mut write_buf).await;
                        return Ok(());
                    }

                    // SUBSCRIBE/PSUBSCRIBE take over the connection (enter push mode).
                    if cmd.name == "SUBSCRIBE" {
                        if super::handler_pubsub::handle_subscribe(
                            &cmd,
                            &session,
                            state,
                            &mut stream,
                        )
                        .await?
                        {
                            return Ok(());
                        }
                        continue;
                    }
                    if cmd.name == "PSUBSCRIBE" {
                        if super::handler_pubsub::handle_psubscribe(
                            &cmd,
                            &session,
                            state,
                            &mut stream,
                        )
                        .await?
                        {
                            return Ok(());
                        }
                        continue;
                    }

                    // Execute the command with timing for slow-log.
                    let start = std::time::Instant::now();
                    if let Some(binding) = &session.binding {
                        // The command name only. RESP arguments carry values and AUTH passwords.
                        binding.set_statement(&cmd.name);
                    }
                    let resp = handler::execute(&cmd, &mut session, state).await;
                    if let Some(binding) = &session.binding {
                        binding.clear_statement();
                    }
                    let elapsed = start.elapsed();

                    // Slow-log: commands exceeding 10ms threshold.
                    if elapsed.as_millis() > 10 {
                        tracing::warn!(
                            target: "nodedb::kv::slowlog",
                            %peer,
                            command = %cmd.name,
                            collection = %session.collection,
                            elapsed_ms = elapsed.as_millis() as u64,
                            "slow KV command"
                        );
                    }

                    // Write response.
                    write_buf.clear();
                    resp.serialize(&mut write_buf);
                    stream
                        .write_all(&write_buf)
                        .await
                        .map_err(|e| crate::Error::Bridge {
                            detail: format!("RESP write: {e}"),
                        })?;
                }
                Ok(None) => break, // Need more data.
                Err(e) => {
                    warn!(%peer, error = %e, "RESP parse error");
                    let resp = super::codec::RespValue::err(format!("ERR protocol error: {e}"));
                    write_buf.clear();
                    resp.serialize(&mut write_buf);
                    let _ = stream.write_all(&write_buf).await;
                    return Ok(());
                }
            }
        }
    }
}
