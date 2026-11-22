// SPDX-License-Identifier: BUSL-1.1

use std::collections::HashMap;
use std::net::SocketAddr;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::{FutureExt, future::join_all};
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tracing::{info, warn};

use crate::config::auth::AuthMode;
use crate::control::state::SharedState;

use super::connection_identity::PgConnectionContext;
use super::factory::NodeDbPgHandlerFactory;

/// PostgreSQL wire protocol listener.
///
/// Accepts TCP connections and handles them using the pgwire crate.
/// Optionally supports TLS (SSLRequest negotiation + upgrade).
/// Runs on the Control Plane (Tokio).
pub struct PgListener {
    tcp: TcpListener,
    addr: SocketAddr,
}

fn forced_drain_cleanup_ids(
    active_connections: &HashMap<
        crate::control::server::shared::session::ConnectionId,
        PgConnectionContext,
    >,
) -> Vec<crate::control::server::shared::session::ConnectionId> {
    active_connections.keys().copied().collect()
}

impl PgListener {
    pub async fn bind(addr: SocketAddr) -> crate::Result<Self> {
        Self::from_listener(TcpListener::bind(addr).await?)
    }

    /// Serve on a socket that already listens.
    pub fn from_listener(tcp: TcpListener) -> crate::Result<Self> {
        let local_addr = tcp.local_addr()?;
        info!(%local_addr, "pgwire listener bound");
        Ok(Self {
            tcp,
            addr: local_addr,
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.addr
    }

    /// Run the accept loop for pgwire connections.
    ///
    /// `tls_acceptor`: if Some, pgwire will negotiate SSL on SSLRequest.
    /// If None, all connections are plaintext.
    ///
    /// On shutdown signal:
    /// 1. Stop accepting new connections.
    /// 2. Wait up to `drain_timeout` for in-flight connections to finish.
    /// 3. Abort remaining connections after timeout.
    pub async fn run(
        self,
        state: Arc<SharedState>,
        auth_mode: AuthMode,
        tls_acceptor: Option<pgwire::tokio::TlsAcceptor>,
        conn_semaphore: Arc<Semaphore>,
        startup_gate: Arc<crate::control::startup::StartupGate>,
        bus: crate::control::shutdown::ShutdownBus,
    ) -> crate::Result<()> {
        let conn_state = Arc::clone(&state);
        // Session-timeout policy, read once at listener startup (config is fixed
        // for the process lifetime). `idle` closes a connection that has been
        // silent between statements for this many seconds — including an
        // idle-in-transaction connection holding a staged-write overlay.
        // `absolute` caps total connection lifetime. `0` disables either.
        let idle_timeout_secs = conn_state.idle_timeout_secs();
        let absolute_timeout_secs = conn_state.session_absolute_timeout_secs();
        let factory = Arc::new(NodeDbPgHandlerFactory::new(state, auth_mode));

        // Active pgwire sessions drain for up to `network.drain_timeout_secs`.
        // Their cleanup must finish before Data/Event/WAL shutdown begins.
        let drain_guard = bus.register_critical_task(
            crate::control::shutdown::ShutdownPhase::DrainingListeners,
            "pgwire",
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
            "pgwire listener bound — waiting for GatewayEnable"
        );

        // Block here until GatewayEnable fires. The socket is already bound
        // so the OS accepts the TCP SYN; the three-way handshake completes
        // but the application call to `accept()` is deferred until startup
        // finishes. This satisfies the k8s pattern: port appears open (no
        // connection refused) but /healthz still returns 503.
        startup_gate
            .await_phase(crate::control::startup::StartupPhase::GatewayEnable)
            .await
            .map_err(crate::Error::from)?;

        info!(
            addr = %self.addr,
            tls = tls_label,
            max_permits = conn_semaphore.available_permits(),
            "accepting pgwire connections"
        );

        let mut connections = JoinSet::new();
        // Owned exclusively by this listener task. IDs remain authoritative
        // when an aborted task cannot return its completion value.
        let mut active_connections = HashMap::new();

        loop {
            tokio::select! {
                result = self.tcp.accept() => {
                    match result {
                        Ok((stream, peer_addr)) => {
                            let local_addr = match stream.local_addr() {
                                Ok(addr) => addr,
                                Err(error) => {
                                    warn!(%peer_addr, %error, "pgwire connection rejected: local address unavailable");
                                    continue;
                                }
                            };
                            let connection_id = match conn_state.connection_ids.allocate() {
                                Ok(id) => id,
                                Err(error) => {
                                    warn!(%peer_addr, %error, "pgwire connection rejected: identifier allocation failed");
                                    continue;
                                }
                            };
                            let context = PgConnectionContext { id: connection_id, peer_addr, local_addr };
                            let permit = match conn_semaphore.clone().try_acquire_owned() {
                                Ok(permit) => permit,
                                Err(_) => {
                                    conn_state.connections_rejected.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                    warn!(
                                        %peer_addr,
                                        "pgwire connection rejected: max_connections limit reached"
                                    );
                                    continue;
                                }
                            };
                            let cancel = match factory.register_connection(context) {
                                Ok(cancel) => cancel,
                                Err(error) => {
                                    warn!(%peer_addr, %error, "pgwire connection rejected: session registration failed");
                                    continue;
                                }
                            };

                            conn_state.connections_accepted.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            info!(%peer_addr, "new pgwire connection");
                            active_connections.insert(connection_id, context);
                            let factory = Arc::clone(&factory);
                            let tls = tls_acceptor.clone();
                            let idle = idle_timeout_secs;
                            let absolute = absolute_timeout_secs;
                            connections.spawn(async move {
                                let connection = async {
                                    run_with_watchdog(
                                        stream, tls, &factory, context, cancel, idle, absolute,
                                    )
                                    .await;
                                };
                                if AssertUnwindSafe(connection).catch_unwind().await.is_err() {
                                    warn!(%peer_addr, "pgwire connection task panicked");
                                }
                                // Reclaim any abandoned-transaction Data-Plane
                                // overlays and remove the shared session entry.
                                // The factory makes both steps panic-isolated
                                // and idempotent, including forced-drain repeats.
                                factory.on_connection_end(connection_id, peer_addr).await;
                                drop(permit);
                                connection_id
                            });
                        }
                        Err(e) => {
                            warn!(error = %e, "pgwire accept failed, retrying");
                        }
                    }
                }
                // Reap completed connections to avoid unbounded growth.
                Some(result) = connections.join_next(), if !connections.is_empty() => {
                    if let Ok(connection_id) = result
                        && let Some(context) = active_connections.remove(&connection_id)
                    {
                        info!(connection_id = connection_id.get(), peer_addr = %context.peer_addr, "pgwire connection closed");
                    }
                }
                _ = shutdown_handle.await_phase(crate::control::shutdown::ShutdownPhase::DrainingListeners) => {
                    info!(
                        addr = %self.addr,
                        active = connections.len(),
                        "shutdown signal, draining pgwire connections"
                    );
                    break;
                }
            }
        }

        // Graceful drain: wait for in-flight connections with timeout.
        let drain_timeout = Duration::from_secs(conn_state.tuning.network.drain_timeout_secs);
        if !connections.is_empty() {
            info!(
                active = connections.len(),
                timeout_secs = drain_timeout.as_secs(),
                "waiting for pgwire connections to drain"
            );

            let drain_result = tokio::time::timeout(drain_timeout, async {
                while let Some(result) = connections.join_next().await {
                    if let Ok(connection_id) = result
                        && let Some(context) = active_connections.remove(&connection_id)
                    {
                        info!(connection_id = connection_id.get(), peer_addr = %context.peer_addr, "drained pgwire connection");
                    }
                }
            })
            .await;

            if drain_result.is_err() {
                let remaining = connections.len();
                let cleanup_ids = forced_drain_cleanup_ids(&active_connections);
                warn!(
                    remaining,
                    "drain timeout exceeded, aborting remaining pgwire connections"
                );
                connections.abort_all();
                while connections.join_next().await.is_some() {}
                // Aborted tasks do not execute their tail cleanup. Wait for
                // all cancellation completions first, then reclaim each
                // listener-owned connection exactly once before reporting drain.
                join_all(cleanup_ids.iter().filter_map(|id| {
                    active_connections
                        .get(id)
                        .map(|context| factory.on_connection_end(*id, context.peer_addr))
                }))
                .await;
                active_connections.clear();
            }
        }

        info!(addr = %self.addr, "pgwire listener stopped");
        drain_guard.report_drained();
        Ok(())
    }
}

/// Run the panic-isolated pgwire connection loop under an idle + absolute
/// session-timeout watchdog.
///
/// pgwire owns the framed connection loop and is hard-typed to a `TcpStream`,
/// so timeouts cannot be enforced inside it. Instead we race the
/// socket future against a bounded re-check tick: on each wake, close the
/// connection — by dropping the future, which drops the socket — if the
/// absolute lifetime is exceeded or the connection is idle-eligible (zero
/// in-flight statements AND silent past the idle window). A statement in
/// flight keeps the connection non-idle, so a legitimately long-running query
/// is never idle-killed. The caller runs `on_connection_end` afterwards to
/// reclaim any staged overlay and drop the session entry.
///
/// Only invoked when at least one of `idle`/`absolute` is non-zero; the
/// all-disabled path skips the watchdog entirely (no wakeups).
async fn run_with_watchdog(
    stream: tokio::net::TcpStream,
    tls: Option<pgwire::tokio::TlsAcceptor>,
    factory: &Arc<NodeDbPgHandlerFactory>,
    context: PgConnectionContext,
    mut cancel: tokio::sync::watch::Receiver<bool>,
    idle: u64,
    absolute: u64,
) {
    let started = Instant::now();
    let mut fut = std::pin::pin!(super::connection::run(
        stream,
        tls,
        Arc::clone(factory),
        context,
    ));
    if idle == 0 && absolute == 0 {
        if *cancel.borrow() {
            return;
        }
        tokio::select! {
            _ = &mut fut => {}
            _ = cancel.changed() => {
                info!(connection_id = context.id.get(), peer_addr = %context.peer_addr, "pgwire connection cancelled");
            }
        }
        return;
    }
    // Bounded re-check tick — never a busy loop. Cancellation is a sticky,
    // exact-ID watch value; no relay task or unbounded channel is involved.
    let tick = Duration::from_secs(1);
    loop {
        if *cancel.borrow() {
            info!(connection_id = context.id.get(), peer_addr = %context.peer_addr, "pgwire connection cancelled");
            break;
        }
        tokio::select! {
            _ = &mut fut => break,
            changed = cancel.changed() => {
                if changed.is_err() || *cancel.borrow() {
                    info!(connection_id = context.id.get(), peer_addr = %context.peer_addr, "pgwire connection cancelled");
                    break;
                }
            }
            _ = tokio::time::sleep(tick) => {
                if absolute > 0 && started.elapsed().as_secs() >= absolute {
                    info!(connection_id = context.id.get(), peer_addr = %context.peer_addr, absolute, "pgwire absolute session timeout, closing");
                    break;
                }
                if idle > 0
                    && factory.session_idle_eligible(context.id, idle.saturating_mul(1000))
                {
                    info!(connection_id = context.id.get(), peer_addr = %context.peer_addr, idle, "pgwire idle session timeout, closing");
                    break;
                }
            }
        }
    }
    // On a timeout break the loop exits with `fut` still pending; it is dropped
    // here at scope end, which closes the socket. The caller then runs the
    // connection-end teardown hook.
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn forced_drain_snapshots_exact_active_connection_ids() {
        use crate::control::server::shared::session::ConnectionId;

        let peer: SocketAddr = "127.0.0.1:7001".parse().expect("valid address");
        let local: SocketAddr = "127.0.0.1:5432".parse().expect("valid address");
        let first = ConnectionId::new(1).expect("nonzero identifier");
        let second = ConnectionId::new(2).expect("nonzero identifier");
        let mut active = HashMap::from([
            (
                first,
                PgConnectionContext {
                    id: first,
                    peer_addr: peer,
                    local_addr: local,
                },
            ),
            (
                second,
                PgConnectionContext {
                    id: second,
                    peer_addr: peer,
                    local_addr: local,
                },
            ),
        ]);
        active.remove(&first);

        assert_eq!(forced_drain_cleanup_ids(&active), vec![second]);
    }

    #[test]
    fn active_map_keeps_duplicate_peer_contexts_by_distinct_id() {
        use crate::control::server::shared::session::ConnectionId;

        let peer: SocketAddr = "127.0.0.1:7001".parse().expect("valid address");
        let local: SocketAddr = "127.0.0.1:5432".parse().expect("valid address");
        let first = ConnectionId::new(1).expect("nonzero identifier");
        let second = ConnectionId::new(2).expect("nonzero identifier");
        let active = HashMap::from([
            (
                first,
                PgConnectionContext {
                    id: first,
                    peer_addr: peer,
                    local_addr: local,
                },
            ),
            (
                second,
                PgConnectionContext {
                    id: second,
                    peer_addr: peer,
                    local_addr: local,
                },
            ),
        ]);

        assert_eq!(active.len(), 2);
        assert_eq!(active.get(&first).expect("first context").peer_addr, peer);
        assert_eq!(active.get(&second).expect("second context").peer_addr, peer);
    }
}
