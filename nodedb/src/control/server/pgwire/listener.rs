// SPDX-License-Identifier: BUSL-1.1

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tracing::{info, warn};

use pgwire::tokio::process_socket;

use crate::config::auth::AuthMode;
use crate::control::state::SharedState;

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

impl PgListener {
    pub async fn bind(addr: SocketAddr) -> crate::Result<Self> {
        let tcp = TcpListener::bind(addr).await?;
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

        // Register with the shutdown bus so the sequencer waits for us to drain
        // before advancing past DrainingListeners.
        let drain_guard = bus.register_task(
            crate::control::shutdown::ShutdownPhase::DrainingListeners,
            "pgwire",
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

        loop {
            tokio::select! {
                result = self.tcp.accept() => {
                    match result {
                        Ok((stream, peer_addr)) => {
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

                            conn_state.connections_accepted.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            info!(%peer_addr, "new pgwire connection");
                            let factory = Arc::clone(&factory);
                            let tls = tls_acceptor.clone();
                            let idle = idle_timeout_secs;
                            let absolute = absolute_timeout_secs;
                            connections.spawn(async move {
                                if idle == 0 && absolute == 0 {
                                    // No session timeouts configured: run the
                                    // plain socket loop with no watchdog wakeups.
                                    if let Err(e) =
                                        process_socket(stream, tls, Arc::clone(&factory)).await
                                    {
                                        warn!(%peer_addr, error = %e, "pgwire session error");
                                    }
                                } else {
                                    run_with_watchdog(
                                        stream, tls, &factory, peer_addr, idle, absolute,
                                    )
                                    .await;
                                }
                                // Reclaim any abandoned-transaction Data-Plane
                                // overlays and drop the shared session entry now
                                // that the connection has ended (whether it ended
                                // on its own or was force-closed by the watchdog).
                                factory.on_connection_end(&peer_addr).await;
                                drop(permit);
                                peer_addr
                            });
                        }
                        Err(e) => {
                            warn!(error = %e, "pgwire accept failed, retrying");
                        }
                    }
                }
                // Reap completed connections to avoid unbounded growth.
                Some(result) = connections.join_next(), if !connections.is_empty() => {
                    if let Ok(peer_addr) = result {
                        info!(%peer_addr, "pgwire connection closed");
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
        let drain_timeout = Duration::from_secs(30);
        if !connections.is_empty() {
            info!(
                active = connections.len(),
                timeout_secs = drain_timeout.as_secs(),
                "waiting for pgwire connections to drain"
            );

            let drain_result = tokio::time::timeout(drain_timeout, async {
                while let Some(result) = connections.join_next().await {
                    if let Ok(peer_addr) = result {
                        info!(%peer_addr, "drained pgwire connection");
                    }
                }
            })
            .await;

            if drain_result.is_err() {
                let remaining = connections.len();
                warn!(
                    remaining,
                    "drain timeout exceeded, aborting remaining pgwire connections"
                );
                connections.abort_all();
            }
        }

        info!(addr = %self.addr, "pgwire listener stopped");
        drain_guard.report_drained();
        Ok(())
    }
}

/// Run `process_socket` under an idle + absolute session-timeout watchdog.
///
/// pgwire's `process_socket` owns the connection loop and is hard-typed to a
/// `TcpStream`, so timeouts cannot be enforced inside it. Instead we race the
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
    peer_addr: SocketAddr,
    idle: u64,
    absolute: u64,
) {
    let started = Instant::now();
    let mut fut = std::pin::pin!(process_socket(stream, tls, Arc::clone(factory)));
    // Bounded re-check tick — never a busy loop. Activity may advance the idle
    // deadline between wakes, which is recomputed via `session_idle_eligible`.
    let tick = Duration::from_secs(1);
    loop {
        tokio::select! {
            r = &mut fut => {
                if let Err(e) = r {
                    warn!(%peer_addr, error = %e, "pgwire session error");
                }
                break;
            }
            _ = tokio::time::sleep(tick) => {
                if absolute > 0 && started.elapsed().as_secs() >= absolute {
                    info!(%peer_addr, absolute, "pgwire absolute session timeout, closing");
                    break;
                }
                if idle > 0
                    && factory.session_idle_eligible(&peer_addr, idle.saturating_mul(1000))
                {
                    info!(%peer_addr, idle, "pgwire idle session timeout, closing");
                    break;
                }
            }
        }
    }
    // On a timeout break the loop exits with `fut` still pending; it is dropped
    // here at scope end, which closes the socket. The caller then runs the
    // connection-end teardown hook.
}
