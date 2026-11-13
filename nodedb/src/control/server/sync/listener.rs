// SPDX-License-Identifier: BUSL-1.1

//! WebSocket listener for NodeDB-Lite sync connections.
//!
//! Accepts loopback-only `ws://` connections on the Tokio Control Plane for a
//! local TLS-terminating proxy. Each connection spawns a sync session with full
//! RLS, audit, DLQ, and rate limiting. Public plaintext binds are rejected.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::net::TcpListener;
use tokio::task::{JoinHandle, JoinSet};
use tracing::{info, warn};

use crate::control::server::reserved_socket::ReservedSocket;
use crate::control::server::shared::{ConnectionFutureOutcome, isolate_connection_future};
use crate::control::shutdown::{ShutdownBus, ShutdownPhase};
use crate::control::state::SharedState;

use super::rate_limit::RateLimitConfig;
use super::session_handler::handle_sync_session;

/// Configuration for the sync WebSocket listener.
///
/// Sessions authenticate presented JWTs against the server-wide `[auth.jwt]`
/// providers reached through `SharedState`, so this carries no verification
/// material of its own.
#[derive(Debug, Clone)]
pub struct SyncListenerConfig {
    pub listen_addr: SocketAddr,
    pub max_sessions: usize,
    pub idle_timeout_secs: u64,
    pub rate_limit: RateLimitConfig,
    /// How long shutdown waits for admitted connections to finish before
    /// aborting them. Injectable so a test need not wait out the production
    /// value.
    pub drain_timeout: std::time::Duration,
}

impl Default for SyncListenerConfig {
    fn default() -> Self {
        Self {
            // Loopback, not `0.0.0.0`: the listen address always comes from
            // `ServerConfig::sync_addr()` in production, so the default must
            // be the conservative one rather than an implicit all-interfaces
            // bind for anything that fills it in from `Default`.
            listen_addr: std::net::SocketAddr::from((
                std::net::Ipv4Addr::LOCALHOST,
                crate::config::server::DEFAULT_SYNC_PORT,
            )),
            max_sessions: 1024,
            idle_timeout_secs: 300,
            rate_limit: RateLimitConfig::default(),
            drain_timeout: std::time::Duration::from_secs(30),
        }
    }
}

/// Sync listener state (shared across all sessions).
pub struct SyncListenerState {
    pub active_sessions: AtomicU64,
    pub connections_accepted: AtomicU64,
    pub connections_rejected: AtomicU64,
    /// Deltas that applied nothing because every operation they carried was
    /// already present, summed over all closed sessions.
    pub deltas_deduplicated: AtomicU64,
    /// Operations discarded by the CRDT merge as already-known, summed over all
    /// closed sessions.
    ///
    /// The per-session close line reports the same fact for one client; this is
    /// the same fact for the listener, so a trend is visible without correlating
    /// log lines by session id.
    pub ops_trimmed: AtomicU64,
    pub config: SyncListenerConfig,
}

impl SyncListenerState {
    pub fn new(config: SyncListenerConfig) -> Self {
        Self {
            active_sessions: AtomicU64::new(0),
            connections_accepted: AtomicU64::new(0),
            connections_rejected: AtomicU64::new(0),
            deltas_deduplicated: AtomicU64::new(0),
            ops_trimmed: AtomicU64::new(0),
            config,
        }
    }

    /// Fold one finished session's delta accounting into the listener totals.
    ///
    /// Called once per session, from the same place that emits the close line,
    /// so the two can never disagree about what that session did.
    pub fn fold_closed_session(&self, deltas_deduplicated: u64, ops_trimmed: u64) {
        self.deltas_deduplicated
            .fetch_add(deltas_deduplicated, Ordering::Relaxed);
        self.ops_trimmed.fetch_add(ops_trimmed, Ordering::Relaxed);
    }

    pub fn can_accept(&self) -> bool {
        self.active_sessions.load(Ordering::Relaxed) < self.config.max_sessions as u64
    }

    pub fn session_rejected(&self) {
        self.connections_rejected.fetch_add(1, Ordering::Relaxed);
    }
}

/// Owns accounting for exactly one accepted sync connection.
///
/// The guard is moved into the connection task before its WebSocket upgrade.
/// Consequently normal completion, a caught panic, and Tokio task cancellation
/// all release the active-session slot through the same `Drop` path.
struct SyncSessionGuard {
    state: Arc<SyncListenerState>,
    sequence: u64,
}

impl SyncSessionGuard {
    /// Reserve a never-reused accepted-connection sequence. Exhaustion is
    /// fail-closed rather than allowing an atomic counter to wrap and collide
    /// with an earlier session's registry key.
    fn open(state: Arc<SyncListenerState>) -> Option<Self> {
        let sequence = state
            .connections_accepted
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                current.checked_add(1)
            })
            .ok()?
            .checked_add(1)?;
        state.active_sessions.fetch_add(1, Ordering::Relaxed);
        Some(Self { state, sequence })
    }

    fn session_id(&self, addr: SocketAddr) -> String {
        format!("sync-{addr}-{}", self.sequence)
    }
}

impl Drop for SyncSessionGuard {
    fn drop(&mut self) {
        self.state.active_sessions.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Owns cleanup of all registry entries for one sync session.
///
/// Cleanup starts synchronously from `Drop`, before its first await, so task
/// cancellation and panic unwinding cannot strand a registered session.
struct SyncRegistrationCleanup {
    shared: Arc<SharedState>,
    session_id: String,
    started: std::sync::atomic::AtomicBool,
}

impl SyncRegistrationCleanup {
    fn new(shared: Arc<SharedState>, session_id: String) -> Self {
        Self {
            shared,
            session_id,
            started: std::sync::atomic::AtomicBool::new(false),
        }
    }

    fn start(self: &Arc<Self>) -> Option<tokio::task::JoinHandle<()>> {
        let handle = tokio::runtime::Handle::try_current().ok()?;
        if self
            .started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return None;
        }
        let cleanup = Arc::clone(self);
        Some(handle.spawn(async move {
            cleanup.run().await;
        }))
    }

    async fn run(self: Arc<Self>) {
        unregister_sync_session(&self.shared, &self.session_id).await;
    }
}

/// Remove every registry binding associated with a sync connection.
///
/// This is idempotent so a new handshake can revoke its previous binding
/// before it authenticates a replacement identity, and final connection
/// cleanup can safely run afterward.
pub(super) async fn unregister_sync_session(shared: &SharedState, session_id: &str) {
    shared.shape_registry.remove_session(session_id);
    shared.crdt_sync_delivery.unregister(session_id);
    shared.array_delivery.unregister(session_id);
    shared.array_subscriber_cursors.remove_session(session_id);
    shared.array_merger_registry.remove_session(session_id);
    shared.definition_sync_fanout.unregister(session_id);

    let mut presence = shared.presence.write().await;
    let outbound = presence.unregister_session(session_id);
    let senders = presence.senders().clone();
    drop(presence);
    outbound.send_all(&senders);
}

/// Ensures normal completion awaits registry cleanup while cancellation and
/// panic unwinding detach the exact same cleanup task.
pub(super) struct SyncRegistrationCleanupGuard {
    cleanup: Option<Arc<SyncRegistrationCleanup>>,
    handle: Option<tokio::task::JoinHandle<()>>,
}

impl SyncRegistrationCleanupGuard {
    pub(super) fn new(shared: Option<Arc<SharedState>>, session_id: String) -> Self {
        Self {
            cleanup: shared
                .map(|shared| Arc::new(SyncRegistrationCleanup::new(shared, session_id))),
            handle: None,
        }
    }

    fn start(&mut self) {
        if self.handle.is_none()
            && let Some(cleanup) = self.cleanup.as_ref()
        {
            self.handle = cleanup.start();
        }
    }

    pub(super) async fn finish(mut self) {
        self.start();
        if let Some(handle) = self.handle.take() {
            let _ = handle.await;
        }
    }
}

impl Drop for SyncRegistrationCleanupGuard {
    fn drop(&mut self) {
        self.start();
    }
}

/// Run the full accepted-connection future under the protocol-neutral panic
/// boundary while retaining session accounting until the future is dropped.
async fn run_accepted_session<T>(
    guard: SyncSessionGuard,
    future: impl std::future::Future<Output = T>,
) -> ConnectionFutureOutcome<T> {
    let _guard = guard;
    isolate_connection_future(future).await
}

/// Bind the sync listener address without listening.
///
/// Boot reserves every protocol socket up front, before the node joins the
/// cluster, and fails loudly on a port conflict while nothing is exposed.
/// It listens only once the node can serve. See
/// `bootstrap::listeners::bind_listeners`.
pub fn reserve_sync_listener(addr: SocketAddr) -> crate::Result<ReservedSocket> {
    // Plaintext `ws://` sync must terminate TLS at a loopback proxy: reject any
    // public bind here so both the fail-fast boot path (`bind_listeners`) and
    // the convenience `start_sync_listener` path are covered by one guard.
    //
    // This guard, not `check_transport_security`, is this listener's transport
    // control, and the omission is deliberate. The socket always sees
    // cleartext because TLS is terminated by the proxy in front of it, so
    // applying the TLS policy here would refuse every session under
    // `reject_cleartext` and make the only supported deployment impossible.
    // Refusing to bind anywhere but loopback is the stronger guarantee: no
    // sync byte crosses a network in the clear, whatever the policy says.
    // Contrast the OTLP receivers, which bind `0.0.0.0` and therefore cannot
    // prove a proxy is in front — they do consult the policy and fail closed.
    if !addr.ip().is_loopback() {
        return Err(crate::Error::Config {
            detail: format!(
                "plaintext sync listener {addr} must bind to loopback behind a TLS-terminating proxy"
            ),
        });
    }
    ReservedSocket::bind(addr)
}

/// Bind the sync listener address and listen at once.
pub async fn bind_sync_listener(addr: SocketAddr) -> crate::Result<TcpListener> {
    reserve_sync_listener(addr)?.listen()
}

/// Start the sync WebSocket listener with full security context.
///
/// Binds and serves in one step. Boot uses [`reserve_sync_listener`] +
/// [`serve_sync_listener`] instead so the bind is fail-fast; this is the
/// convenience path for callers that own the whole lifecycle (tests, tools)
/// and can provide that lifecycle's canonical shutdown bus.
pub async fn start_sync_listener(
    config: SyncListenerConfig,
    shared: Option<Arc<SharedState>>,
    shutdown_bus: ShutdownBus,
) -> crate::Result<Arc<SyncListenerState>> {
    let listener = bind_sync_listener(config.listen_addr).await?;
    Ok(serve_sync_listener(listener, config, shared, shutdown_bus).await)
}

/// Serve sync sessions on an already-bound listener.
///
/// The caller supplies the process's canonical shutdown bus. The listener
/// holds a critical drain guard until its accept loop, presence sweeper, and
/// every admitted connection task have stopped.
pub async fn serve_sync_listener(
    listener: TcpListener,
    config: SyncListenerConfig,
    shared: Option<Arc<SharedState>>,
    shutdown_bus: ShutdownBus,
) -> Arc<SyncListenerState> {
    // Surface the actually-bound address. For a fixed port this is a no-op; for
    // an ephemeral port (`:0`) it records the OS-assigned port so the caller can
    // discover where the listener is reachable.
    let mut config = config;
    if let Ok(bound) = listener.local_addr() {
        config.listen_addr = bound;
    }

    let state = Arc::new(SyncListenerState::new(config));
    let drain_guard = shutdown_bus.register_critical_task(ShutdownPhase::DrainingListeners, "sync");
    let state_clone = Arc::clone(&state);
    tokio::spawn(async move {
        run_sync_listener(listener, state_clone, shared, shutdown_bus, drain_guard).await;
    });

    state
}

async fn run_sync_listener(
    listener: TcpListener,
    state: Arc<SyncListenerState>,
    shared: Option<Arc<SharedState>>,
    shutdown_bus: ShutdownBus,
    drain_guard: crate::control::shutdown::DrainGuard,
) {
    let mut shutdown_handle = shutdown_bus.handle();
    let mut connections = JoinSet::new();
    let mut presence_sweeper = spawn_presence_sweeper(shared.as_ref()).await;

    info!(addr = %state.config.listen_addr, "sync WebSocket listener started");

    loop {
        tokio::select! {
            result = listener.accept() => {
                match result {
                    Ok((stream, addr)) => {
                        if !state.can_accept() {
                            state.session_rejected();
                            warn!(%addr, "sync: max sessions reached, rejecting");
                            continue;
                        }

                        // Create the guard after admission and move it into the task
                        // before WebSocket upgrade or session processing can panic.
                        let Some(guard) = SyncSessionGuard::open(Arc::clone(&state)) else {
                            state.session_rejected();
                            warn!(%addr, "sync: accepted-session identity exhausted, rejecting");
                            continue;
                        };
                        let session_id = guard.session_id(addr);
                        let state_clone = Arc::clone(&state);
                        let shared_clone = shared.clone();

                        connections.spawn(async move {
                            let outcome = run_accepted_session(guard, async {
                                match tokio_tungstenite::accept_async(stream).await {
                                    Ok(ws) => {
                                        info!(%addr, "sync: WebSocket connection established");
                                        handle_sync_session(
                                            ws,
                                            addr,
                                            session_id,
                                            &state_clone,
                                            shared_clone,
                                        )
                                        .await;
                                    }
                                    Err(error) => {
                                        warn!(%addr, error = %error, "sync: WebSocket upgrade failed");
                                    }
                                }
                            })
                            .await;
                            if matches!(outcome, ConnectionFutureOutcome::Panicked) {
                                // Do not inspect a panic payload: it may contain client
                                // data or application internals.
                                warn!(%addr, "sync connection panicked; closing connection");
                            }
                        });
                    }
                    Err(error) => warn!(%error, "sync: accept failed"),
                }
            }
            Some(result) = connections.join_next(), if !connections.is_empty() => {
                if let Err(error) = result {
                    warn!(%error, "sync connection task ended unexpectedly");
                }
            }
            _ = shutdown_handle.await_phase(ShutdownPhase::DrainingListeners) => {
                info!(
                    addr = %state.config.listen_addr,
                    active = connections.len(),
                    "shutdown signal, draining sync connections"
                );
                break;
            }
        }
    }

    if let Some(sweeper) = presence_sweeper.take() {
        sweeper.abort();
        let _ = sweeper.await;
    }

    if !connections.is_empty()
        && tokio::time::timeout(state.config.drain_timeout, async {
            while connections.join_next().await.is_some() {}
        })
        .await
        .is_err()
    {
        warn!(
            remaining = connections.len(),
            "sync drain timeout exceeded, aborting remaining connections"
        );
        connections.abort_all();
        while connections.join_next().await.is_some() {}
    }

    info!(addr = %state.config.listen_addr, "sync listener stopped");
    drain_guard.report_drained();
}

async fn spawn_presence_sweeper(shared: Option<&Arc<SharedState>>) -> Option<JoinHandle<()>> {
    let shared = shared?;
    let presence = Arc::clone(&shared.presence);
    let sweep_interval_ms = presence.read().await.sweep_interval_ms();
    Some(tokio::spawn(async move {
        let mut interval =
            tokio::time::interval(std::time::Duration::from_millis(sweep_interval_ms));
        loop {
            interval.tick().await;
            let mut mgr = presence.write().await;
            let outbound = mgr.sweep_expired();
            let senders = mgr.senders().clone();
            drop(mgr);
            outbound.send_all(&senders);
        }
    }))
}

#[cfg(test)]
mod tests {
    use std::net::Ipv4Addr;
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::sync::{mpsc, oneshot};

    use super::*;
    use crate::bridge::dispatch::Dispatcher;
    use crate::control::server::sync::presence::SessionSender;
    use crate::control::server::sync::shape::definition::{ShapeDefinition, ShapeScope, ShapeType};
    use crate::control::shutdown::{ShutdownPhase, ShutdownWatch};
    use crate::event::crdt_sync::types::DeliveryConfig;
    use crate::wal::WalManager;

    /// Binding to an address that's already occupied must surface as `Err`,
    /// not panic or silently succeed — this is the behavior `bind_listeners`
    /// relies on to fail boot on a sync port conflict instead of logging a
    /// non-fatal warning and coming up sync-less.
    #[tokio::test]
    async fn bind_sync_listener_returns_err_on_occupied_port() {
        // Reserve an ephemeral port via a real listener so we know it's taken.
        let occupied = TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
            .await
            .expect("bind ephemeral listener to reserve a port");
        let addr = occupied
            .local_addr()
            .expect("local addr of reserved listener");

        let result = bind_sync_listener(addr).await;

        assert!(
            result.is_err(),
            "expected bind_sync_listener to return Err when the address is already bound"
        );
    }

    /// `start_sync_listener` must propagate the same bind failure — it is the
    /// path tests and tools use, and it must not diverge from the boot path.
    #[tokio::test]
    async fn start_sync_listener_returns_err_on_occupied_port() {
        let occupied = TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
            .await
            .expect("bind ephemeral listener to reserve a port");
        let addr = occupied
            .local_addr()
            .expect("local addr of reserved listener");

        let cfg = SyncListenerConfig {
            listen_addr: addr,
            ..Default::default()
        };

        let (bus, _) = ShutdownBus::new(Arc::new(ShutdownWatch::new()));
        assert!(
            start_sync_listener(cfg, None, bus).await.is_err(),
            "expected start_sync_listener to return Err when the address is already bound"
        );
    }

    /// The default must not be an implicit all-interfaces bind: production
    /// always sets `listen_addr` from `ServerConfig::sync_addr()`, so anything
    /// falling back to `Default` should get the conservative address, and its
    /// port must agree with the config default.
    #[test]
    fn default_listen_addr_is_loopback_on_the_config_default_port() {
        let cfg = SyncListenerConfig::default();
        assert_eq!(cfg.listen_addr.ip(), Ipv4Addr::LOCALHOST);
        assert_eq!(
            cfg.listen_addr.port(),
            crate::config::server::DEFAULT_SYNC_PORT
        );
    }

    #[test]
    fn default_plaintext_sync_listener_is_loopback_only() {
        assert!(SyncListenerConfig::default().listen_addr.ip().is_loopback());
    }

    #[tokio::test]
    async fn public_plaintext_sync_bind_is_rejected() {
        let config = SyncListenerConfig {
            listen_addr: "0.0.0.0:9090".parse().unwrap(),
            ..SyncListenerConfig::default()
        };
        let (bus, _) = ShutdownBus::new(Arc::new(ShutdownWatch::new()));
        let error = match start_sync_listener(config, None, bus).await {
            Ok(_) => panic!("public plaintext listener unexpectedly started"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("TLS-terminating proxy"));
    }

    /// Boot binds `ServerConfig::sync_addr`, never `SyncListenerConfig`'s
    /// default — so asserting that default is loopback proved nothing. Runs
    /// the derivation and the guard together, which is what catches it.
    #[tokio::test]
    async fn config_derived_sync_addr_binds_under_a_routable_host() {
        let mut config = crate::config::server::ServerConfig::default();
        config.server.host = std::net::IpAddr::V4(Ipv4Addr::UNSPECIFIED);
        // Ephemeral: test the guard, not whether 9090 is free.
        config.server.ports.sync = 0;

        let addr = config.sync_addr();
        assert!(
            addr.ip().is_loopback(),
            "sync_addr must stay on loopback under a routable host: {addr}"
        );
        bind_sync_listener(addr)
            .await
            .expect("boot must derive an address this guard accepts");
    }

    /// An explicit routable sync bind is refused, not silently rewritten.
    #[tokio::test]
    async fn explicit_routable_sync_host_is_still_refused() {
        let mut config = crate::config::server::ServerConfig::default();
        config.server.sync_host = Some(std::net::IpAddr::V4(Ipv4Addr::UNSPECIFIED));
        config.server.ports.sync = 0;

        let error = bind_sync_listener(config.sync_addr())
            .await
            .expect_err("an explicit routable sync_host must be refused");
        assert!(error.to_string().contains("TLS-terminating proxy"));
    }

    #[tokio::test]
    async fn shutdown_critical_barrier_stops_admitted_sync_tasks() {
        let listener = TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
            .await
            .expect("bind sync listener");
        let addr = listener.local_addr().expect("sync listener address");
        let (bus, _) = ShutdownBus::new(Arc::new(ShutdownWatch::new()));
        let state = serve_sync_listener(
            listener,
            SyncListenerConfig {
                listen_addr: addr,
                // The admitted task below never finishes, so the production
                // drain would be waited out in full.
                drain_timeout: Duration::from_millis(200),
                ..SyncListenerConfig::default()
            },
            None,
            bus.clone(),
        )
        .await;

        let _client = tokio::net::TcpStream::connect(addr)
            .await
            .expect("connect a stalled sync handshake");
        tokio::time::timeout(Duration::from_secs(1), async {
            while state.active_sessions.load(Ordering::Relaxed) != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("sync listener must own the admitted connection task");

        let mut shutdown = bus.handle();
        let sequencer = bus.initiate();
        shutdown.await_phase(ShutdownPhase::Closed).await;
        sequencer.await.expect("shutdown sequencer must complete");

        assert_eq!(state.active_sessions.load(Ordering::Relaxed), 0);
    }

    fn listener_state() -> Arc<SyncListenerState> {
        // A short drain: the admitted task in these tests never finishes, so
        // the production default would be waited out in full.
        let config = SyncListenerConfig {
            drain_timeout: Duration::from_millis(200),
            ..SyncListenerConfig::default()
        };
        Arc::new(SyncListenerState::new(config))
    }

    fn test_shared_state() -> (Arc<SharedState>, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("create sync cleanup test directory");
        let wal = Arc::new(
            WalManager::open_for_testing(&dir.path().join("sync-cleanup.wal"))
                .expect("open sync cleanup test WAL"),
        );
        let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
        let shared = SharedState::new(dispatcher, wal).expect("construct sync cleanup SharedState");
        (shared, dir)
    }

    async fn wait_for_detached_cleanup(mut complete: impl FnMut() -> bool) {
        tokio::time::timeout(Duration::from_secs(2), async {
            while !complete() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("detached sync registration cleanup must complete promptly");
    }

    #[tokio::test]
    async fn panicking_accepted_session_releases_accounting() {
        let state = listener_state();
        let guard = SyncSessionGuard::open(Arc::clone(&state))
            .expect("fresh listener state must allocate an accepted-session identity");
        let outcome = run_accepted_session(guard, async {
            panic!("sync panic payload must remain private");
        })
        .await;

        assert_eq!(outcome, ConnectionFutureOutcome::Panicked);
        assert_eq!(state.active_sessions.load(Ordering::Relaxed), 0);
        assert_eq!(state.connections_accepted.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn aborted_accepted_session_releases_accounting() {
        let state = listener_state();
        let guard = SyncSessionGuard::open(Arc::clone(&state))
            .expect("fresh listener state must allocate an accepted-session identity");
        let task = tokio::spawn(run_accepted_session(guard, std::future::pending::<()>()));
        assert_eq!(state.active_sessions.load(Ordering::Relaxed), 1);

        task.abort();
        let _ = task.await;

        assert_eq!(state.active_sessions.load(Ordering::Relaxed), 0);
        assert_eq!(state.connections_accepted.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn accepted_sessions_from_the_same_peer_have_unique_ids() {
        let state = listener_state();
        let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, 41234));
        let first = SyncSessionGuard::open(Arc::clone(&state))
            .expect("first accepted session must allocate an identity");
        let second = SyncSessionGuard::open(Arc::clone(&state))
            .expect("second accepted session must allocate an identity");

        assert_ne!(first.session_id(addr), second.session_id(addr));
        assert_eq!(state.connections_accepted.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn accepted_session_identity_exhaustion_is_fail_closed() {
        let state = listener_state();
        state
            .connections_accepted
            .store(u64::MAX, Ordering::Relaxed);

        assert!(SyncSessionGuard::open(Arc::clone(&state)).is_none());
        assert_eq!(state.active_sessions.load(Ordering::Relaxed), 0);
        assert_eq!(state.connections_accepted.load(Ordering::Relaxed), u64::MAX);
    }

    #[tokio::test]
    async fn dropped_cleanup_guard_removes_every_session_registry() {
        let (shared, _dir) = test_shared_state();
        let session_id = "sync-cleanup-drop-regression".to_owned();

        let shape_baseline = shared.shape_registry.active_sessions();
        let crdt_baseline = shared.crdt_sync_delivery.session_count();
        let array_delivery_baseline = shared.array_delivery.active_sessions();
        let definition_baseline = shared.definition_sync_fanout.active_sessions();
        let presence_baseline = shared.presence.read().await.senders().len();

        shared.shape_registry.subscribe(
            &session_id,
            ShapeScope {
                tenant_id: 1,
                database_id: crate::types::DatabaseId::DEFAULT,
            },
            ShapeDefinition {
                shape_id: "cleanup-shape".into(),
                tenant_id: 1,
                shape_type: ShapeType::Document {
                    collection: "cleanup_collection".into(),
                    predicate: Vec::new(),
                },
                description: "cleanup regression shape".into(),
                field_filter: Vec::new(),
            },
        );
        let (crdt_rx, crdt_control_rx) = shared.crdt_sync_delivery.register(
            session_id.clone(),
            7,
            1,
            crate::types::DatabaseId::DEFAULT,
            &DeliveryConfig::default(),
        );
        let array_delivery_rx = shared.array_delivery.register(session_id.clone());
        shared
            .array_subscriber_cursors
            .register(&session_id, "cleanup_array", None);
        let merger = shared.array_merger_registry.get_or_create(
            &session_id,
            crate::types::DatabaseId::DEFAULT,
            1,
            "cleanup_array",
        );
        let definition_rx = shared.definition_sync_fanout.register(
            session_id.clone(),
            1,
            crate::types::DatabaseId::DEFAULT,
        );
        let (presence_tx, _presence_rx) = mpsc::channel(1);
        shared
            .presence
            .write()
            .await
            .register_session(session_id.clone(), SessionSender::new(presence_tx));

        assert_eq!(shared.shape_registry.active_sessions(), shape_baseline + 1);
        assert_eq!(shared.crdt_sync_delivery.session_count(), crdt_baseline + 1);
        assert_eq!(
            shared.array_delivery.active_sessions(),
            array_delivery_baseline + 1
        );
        assert!(
            shared
                .array_subscriber_cursors
                .get(&session_id, "cleanup_array")
                .is_some()
        );
        assert_eq!(
            shared.definition_sync_fanout.active_sessions(),
            definition_baseline + 1
        );
        assert!(
            shared
                .presence
                .read()
                .await
                .senders()
                .contains_key(&session_id)
        );

        let (guard_created_tx, guard_created_rx) = oneshot::channel();
        let cleanup_shared = Arc::clone(&shared);
        let cleanup_session_id = session_id.clone();
        let guard_owner = tokio::spawn(async move {
            let _guard =
                SyncRegistrationCleanupGuard::new(Some(cleanup_shared), cleanup_session_id);
            let _ = guard_created_tx.send(());
            std::future::pending::<()>().await;
        });
        guard_created_rx
            .await
            .expect("cleanup guard owner must start before cancellation");
        guard_owner.abort();
        let _ = guard_owner.await;

        wait_for_detached_cleanup(|| {
            shared.shape_registry.active_sessions() == shape_baseline
                && shared.crdt_sync_delivery.session_count() == crdt_baseline
                && crdt_rx.is_closed()
                && crdt_control_rx.is_closed()
                && shared.array_delivery.active_sessions() == array_delivery_baseline
                && array_delivery_rx.is_closed()
                && shared
                    .array_subscriber_cursors
                    .get(&session_id, "cleanup_array")
                    .is_none()
                && shared.definition_sync_fanout.active_sessions() == definition_baseline
                && definition_rx.is_closed()
        })
        .await;
        let replacement_merger = shared.array_merger_registry.get_or_create(
            &session_id,
            crate::types::DatabaseId::DEFAULT,
            1,
            "cleanup_array",
        );
        assert!(
            !Arc::ptr_eq(&merger, &replacement_merger),
            "cleanup must remove the prior session-scoped array merger"
        );
        shared.array_merger_registry.remove_session(&session_id);
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let presence = shared.presence.read().await;
                if presence.senders().len() == presence_baseline
                    && !presence.senders().contains_key(&session_id)
                {
                    break;
                }
                drop(presence);
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("detached cleanup must unregister presence promptly");
    }

    #[tokio::test]
    async fn handshake_reset_unregisters_prior_session_bindings() {
        let (shared, _dir) = test_shared_state();
        let session_id = "sync-handshake-reset-regression".to_owned();

        shared.shape_registry.subscribe(
            &session_id,
            ShapeScope {
                tenant_id: 1,
                database_id: crate::types::DatabaseId::DEFAULT,
            },
            ShapeDefinition {
                shape_id: "reset-shape".into(),
                tenant_id: 1,
                shape_type: ShapeType::Document {
                    collection: "reset_collection".into(),
                    predicate: Vec::new(),
                },
                description: "handshake reset regression shape".into(),
                field_filter: Vec::new(),
            },
        );
        let (crdt_rx, crdt_control_rx) = shared.crdt_sync_delivery.register(
            session_id.clone(),
            7,
            1,
            crate::types::DatabaseId::DEFAULT,
            &DeliveryConfig::default(),
        );
        let array_delivery_rx = shared.array_delivery.register(session_id.clone());
        shared
            .array_subscriber_cursors
            .register(&session_id, "reset_array", None);
        let merger = shared.array_merger_registry.get_or_create(
            &session_id,
            crate::types::DatabaseId::DEFAULT,
            1,
            "reset_array",
        );
        let definition_rx = shared.definition_sync_fanout.register(
            session_id.clone(),
            1,
            crate::types::DatabaseId::DEFAULT,
        );
        let (presence_tx, _presence_rx) = mpsc::channel(1);
        shared
            .presence
            .write()
            .await
            .register_session(session_id.clone(), SessionSender::new(presence_tx));

        unregister_sync_session(&shared, &session_id).await;

        assert_eq!(shared.shape_registry.active_sessions(), 0);
        assert_eq!(shared.crdt_sync_delivery.session_count(), 0);
        assert!(crdt_rx.is_closed());
        assert!(crdt_control_rx.is_closed());
        assert_eq!(shared.array_delivery.active_sessions(), 0);
        assert!(array_delivery_rx.is_closed());
        assert!(
            shared
                .array_subscriber_cursors
                .get(&session_id, "reset_array")
                .is_none()
        );
        let replacement_merger = shared.array_merger_registry.get_or_create(
            &session_id,
            crate::types::DatabaseId::DEFAULT,
            1,
            "reset_array",
        );
        assert!(
            !Arc::ptr_eq(&merger, &replacement_merger),
            "handshake reset must remove the old array merger"
        );
        shared.array_merger_registry.remove_session(&session_id);
        assert_eq!(shared.definition_sync_fanout.active_sessions(), 0);
        assert!(definition_rx.is_closed());
        assert!(
            !shared
                .presence
                .read()
                .await
                .senders()
                .contains_key(&session_id)
        );
    }

    #[tokio::test]
    async fn completed_accepted_session_releases_accounting() {
        let state = listener_state();
        let guard = SyncSessionGuard::open(Arc::clone(&state))
            .expect("fresh listener state must allocate an accepted-session identity");
        let outcome = run_accepted_session(guard, async {}).await;

        assert_eq!(outcome, ConnectionFutureOutcome::Completed(()));
        assert_eq!(state.active_sessions.load(Ordering::Relaxed), 0);
        assert_eq!(state.connections_accepted.load(Ordering::Relaxed), 1);
    }
}
