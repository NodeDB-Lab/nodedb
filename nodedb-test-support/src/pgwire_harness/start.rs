// SPDX-License-Identifier: BUSL-1.1

//! Single-core `TestServer::start`, plus `take_dir` for handing the data
//! directory to a subsequent restart.

use std::sync::Arc;
use std::time::Duration;

use nodedb::bridge::dispatch::Dispatcher;
use nodedb::config::auth::AuthMode;
use nodedb::control::server::pgwire::listener::PgListener;
use nodedb::control::state::SharedState;
use nodedb::event::{EventPlane, create_event_bus};
use nodedb::wal::WalManager;

use super::support::{bind_http_listener, bind_native_listener, init_test_memory_governor};
use super::types::{TestClient, TestDataDir, TestServer};

/// Knobs for spawning a `TestServer`. `Default` reproduces the historical
/// `TestServer::start` behaviour: trust-mode auth, lockout disabled.
pub(super) struct StartConfig {
    /// pgwire authentication mode.
    pub auth_mode: AuthMode,
    /// When `Some((max_failed, lockout_secs))`, configures the credential
    /// store's lockout policy before it is shared. `None` leaves lockout
    /// disabled (`max_failed_logins = 0`).
    pub lockout: Option<(u32, u64)>,
    /// Override for `QueryTuning::columnar_flush_threshold`.  `None` keeps the
    /// system default (65 536 rows).  Set to a small value (e.g. `4`) in tests
    /// that need to observe segment-flush behaviour without inserting 65k rows.
    pub columnar_flush_threshold: Option<usize>,
    /// When `Some`, installs a cluster routing table on the node's
    /// `SharedState` (`cluster_routing`) before the state is shared.  A
    /// single-node `TestServer` is normally `cluster_routing == None`; the
    /// Raft snapshot builder requires a routing table to resolve a group's
    /// vShards, so round-trip tests inject one here.
    pub routing: Option<nodedb_cluster::RoutingTable>,
    /// Idle session timeout in seconds applied to the node's `SharedState`
    /// before it is shared. `0` (the default) leaves the idle watchdog
    /// disabled; a small value (e.g. `1`) lets tests exercise the pgwire
    /// listener's idle-timeout force-close and overlay reclamation.
    pub idle_timeout_secs: u64,
    /// Absolute session lifetime in seconds applied to `SharedState` before it
    /// is shared. `0` (the default) disables the absolute cap.
    pub session_absolute_timeout_secs: u64,
}

impl Default for StartConfig {
    fn default() -> Self {
        Self {
            auth_mode: AuthMode::Trust,
            lockout: None,
            columnar_flush_threshold: None,
            routing: None,
            idle_timeout_secs: 0,
            session_absolute_timeout_secs: 0,
        }
    }
}

#[allow(dead_code)]
impl TestServer {
    /// Spawn a single-core NodeDB server and connect via pgwire (trust mode).
    pub async fn start() -> Self {
        Self::start_with_config(StartConfig::default()).await
    }

    /// Spawn a single-core NodeDB server with a lowered
    /// `QueryTuning::columnar_flush_threshold` so tests can observe
    /// segment-flush behaviour on small datasets without inserting 65k rows.
    ///
    /// All other settings stay at their defaults (trust-mode auth, lockout
    /// disabled).  Mirrors `ClusterTestHarness::spawn_three_with_columnar_flush_threshold`.
    pub async fn start_with_columnar_flush_threshold(flush_threshold: usize) -> Self {
        Self::start_with_config(StartConfig {
            columnar_flush_threshold: Some(flush_threshold),
            ..Default::default()
        })
        .await
    }

    /// Spawn a single-core server in pgwire **password mode** (SCRAM-SHA-256)
    /// with the credential lockout policy enabled (`5` failures → `300s`).
    ///
    /// The harness user `nodedb` keeps password `nodedb`; the returned
    /// client authenticates with it. Tests can then mutate the credential
    /// store and open further connections to exercise the SCRAM auth path.
    pub async fn start_password() -> Self {
        Self::start_with_config(StartConfig {
            auth_mode: AuthMode::Password,
            lockout: Some((5, 300)),
            ..Default::default()
        })
        .await
    }

    /// Spawn a single-core NodeDB server with a short idle session timeout so
    /// tests can exercise the pgwire listener's idle-timeout watchdog: an
    /// idle-in-transaction connection is force-closed after `idle_secs`,
    /// triggering `on_connection_end` overlay reclamation. All other settings
    /// stay at their defaults (trust-mode auth, lockout disabled, no absolute
    /// cap).
    pub async fn start_with_idle_timeout(idle_secs: u64) -> Self {
        Self::start_with_config(StartConfig {
            idle_timeout_secs: idle_secs,
            ..Default::default()
        })
        .await
    }

    /// Spawn a single-core NodeDB server with a cluster routing table
    /// installed on `SharedState::cluster_routing`.
    ///
    /// Single-node `TestServer`s are normally `cluster_routing == None`, but
    /// the production Raft snapshot builder/applier resolve a group's vShards
    /// through the routing table. Snapshot round-trip tests inject one with
    /// `RoutingTable::uniform(...)` so the builder can filter and the applier
    /// can rebind. All other settings stay at their defaults (trust-mode auth,
    /// lockout disabled).
    pub async fn start_with_routing(routing: nodedb_cluster::RoutingTable) -> Self {
        Self::start_with_config(StartConfig {
            routing: Some(routing),
            ..Default::default()
        })
        .await
    }

    /// Spawn a single-core NodeDB server and connect via pgwire.
    pub(super) async fn start_with_config(cfg: StartConfig) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("test.wal");
        let wal = Arc::new(WalManager::open_for_testing(&wal_path).unwrap());

        let (dispatcher, data_sides) = Dispatcher::new(1, 64);
        let (event_producers, event_consumers) = create_event_bus(1);

        // Use catalog-backed credential store (required for CREATE FUNCTION/TRIGGER/PROCEDURE).
        let catalog_path = dir.path().join("system.redb");
        let mut credential_store =
            nodedb::control::security::credential::store::CredentialStore::open(&catalog_path)
                .unwrap();
        // Apply the lockout policy before the store is shared — `set_lockout_policy`
        // needs `&mut`, so it cannot be called once wrapped in an `Arc`.
        if let Some((max_failed, lockout_secs)) = cfg.lockout {
            credential_store.set_lockout_policy(max_failed, lockout_secs, 0);
        }
        let credentials = Arc::new(credential_store);
        // Provision the harness superuser `nodedb` so Trust-mode strict
        // identity resolution accepts the default test connection. The
        // bootstrap exception in the handler only fires when the store
        // is empty, which would break as soon as any DDL creates a user.
        let _ = credentials.create_user(
            "nodedb",
            "nodedb",
            nodedb::types::TenantId::new(1),
            vec![nodedb::control::security::identity::Role::Superuser],
        );
        // Ensure the built-in `default` database (id 0) is present in the
        // catalog so `USE DATABASE default` and `\c default` work in tests.
        // Idempotent: no-op if the descriptor is already there.
        let _ = credentials.catalog().bootstrap_default_database();
        let mut shared =
            SharedState::new_with_credentials(dispatcher, Arc::clone(&wal), credentials)
                .expect("build shared state");
        // Inject a fixed test KEK so backup tests produce encrypted envelopes.
        // Deterministic 32-byte key — same value every test run.
        if let Some(s) = Arc::get_mut(&mut shared) {
            s.backup_kek = Some(Arc::new([0x42u8; 32]));
            s.governor = init_test_memory_governor();
            if let Some(routing) = cfg.routing {
                s.cluster_routing = Some(std::sync::Arc::new(std::sync::RwLock::new(routing)));
            }
            s.set_session_timeouts_for_test(
                cfg.idle_timeout_secs,
                cfg.session_absolute_timeout_secs,
            );
        }
        let shared = shared;

        // Data Plane core. Share the SharedState's array_catalog so DDL
        // mutations made by the SQL converter are visible to the handler
        // (without this, CP and DP would each carry independent catalogs
        // and `OpenArray` post-DROP-and-recreate would see stale state).
        let mut core_stop_txs = Vec::new();
        let mut core_handles = Vec::new();
        for (idx, (data_side, event_producer)) in
            data_sides.into_iter().zip(event_producers).enumerate()
        {
            let (core_stop_tx, core_stop_rx) = std::sync::mpsc::channel::<()>();
            let core_handle =
                crate::core_loop_runner::spawn_core_loop(crate::core_loop_runner::CoreLoopSpawn {
                    idx,
                    data_side,
                    core_dir: dir.path().to_path_buf(),
                    core_array_catalog: shared.array_catalog.clone(),
                    event_producer,
                    // Share the Control-Plane `SystemMetrics` so the Data-Plane
                    // core updates the same `active_txn_overlays` gauge tests read.
                    core_metrics: shared.system_metrics.clone(),
                    governor: shared.governor.clone(),
                    replay: None,
                    graph_tuning: nodedb_types::config::tuning::GraphTuning::default(),
                    query_tuning: {
                        let mut qt = nodedb_types::config::tuning::QueryTuning::default();
                        if let Some(threshold) = cfg.columnar_flush_threshold {
                            qt.columnar_flush_threshold = threshold;
                        }
                        qt
                    },
                    stop_rx: core_stop_rx,
                });
            core_stop_txs.push(core_stop_tx);
            core_handles.push(core_handle);
        }

        // Response poller.
        let shared_poller = Arc::clone(&shared);
        let (poller_shutdown_tx, mut poller_shutdown_rx) = tokio::sync::watch::channel(false);
        let poller_handle = tokio::spawn(async move {
            loop {
                shared_poller.poll_and_route_responses();
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_millis(1)) => {}
                    _ = poller_shutdown_rx.changed() => break,
                }
            }
        });

        let watermark_store =
            Arc::new(nodedb::event::watermark::WatermarkStore::open(dir.path()).unwrap());
        let trigger_dlq = Arc::new(std::sync::Mutex::new(
            nodedb::event::trigger::TriggerDlq::open(dir.path()).unwrap(),
        ));
        let event_plane = EventPlane::spawn(
            event_consumers,
            Arc::clone(&wal),
            watermark_store,
            Arc::clone(&shared),
            trigger_dlq,
            Arc::clone(&shared.cdc_router),
            Arc::clone(&shared.shutdown),
        );

        // PgWire listener.
        let pg_listener = PgListener::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let pg_addr = pg_listener.local_addr();

        // Create a shutdown bus wrapping the shared.shutdown watch so that
        // bus.initiate() also signals the flat ShutdownWatch.
        let (shutdown_bus, _) =
            nodedb::control::shutdown::ShutdownBus::new(Arc::clone(&shared.shutdown));
        let conn_semaphore = Arc::new(tokio::sync::Semaphore::new(128));
        let shared_pg = Arc::clone(&shared);
        // Use the startup gate already on SharedState (a pre-fired placeholder
        // from `new_inner`). The listener starts accepting immediately.
        let test_startup_gate = Arc::clone(&shared.startup);
        let bus_pg = shutdown_bus.clone();
        let pg_sem = Arc::clone(&conn_semaphore);
        let listener_auth_mode = cfg.auth_mode.clone();
        let pg_handle = tokio::spawn(async move {
            pg_listener
                .run(
                    shared_pg,
                    listener_auth_mode,
                    None,
                    pg_sem,
                    test_startup_gate,
                    bus_pg,
                )
                .await
                .unwrap();
        });

        // Native (MessagePack) listener — same SharedState, ephemeral port.
        let (native_port, native_handle) =
            bind_native_listener(&shared, &shutdown_bus, Arc::clone(&conn_semaphore)).await;
        let (http_port, http_handle) = bind_http_listener(&shared, &shutdown_bus).await;

        tokio::time::sleep(Duration::from_millis(50)).await;

        // Connect client. Password / certificate mode supplies the harness
        // user's credentials so the SCRAM handshake completes.
        let conn_str = match cfg.auth_mode {
            AuthMode::Password | AuthMode::Certificate => format!(
                "host=127.0.0.1 port={} user=nodedb password=nodedb dbname=nodedb",
                pg_addr.port()
            ),
            AuthMode::Trust => format!(
                "host=127.0.0.1 port={} user=nodedb dbname=nodedb",
                pg_addr.port()
            ),
        };
        let (client, connection) = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls)
            .await
            .expect("pgwire connect failed");

        let conn_handle = tokio::spawn(async move {
            let _ = connection.await;
        });

        Self {
            client: TestClient::new(client),
            pg_port: pg_addr.port(),
            native_port,
            http_port,
            shared,
            conn_handle: Some(conn_handle),
            shutdown_bus: Some(shutdown_bus),
            poller_shutdown_tx: Some(poller_shutdown_tx),
            core_stop_txs: Some(core_stop_txs),
            pg_handle: Some(pg_handle),
            native_handle: Some(native_handle),
            http_handle: Some(http_handle),
            poller_handle: Some(poller_handle),
            core_handles: Some(core_handles),
            event_plane: Some(event_plane),
            _dir: dir,
        }
    }

    /// Consume the data directory from a live server so it outlives the
    /// server's lifetime. The server continues to run until dropped, but
    /// ownership of the temp dir moves to the caller so the files survive
    /// the `Drop` of `TestServer`.
    ///
    /// The returned `TestDataDir` must be kept alive until the caller is
    /// done with the on-disk state (i.e., after `open_on_path` returns).
    pub fn take_dir(mut self) -> (Self, TestDataDir) {
        // Replace the TempDir inside self with a new one (data plane has
        // already loaded everything, so the new "empty" dir is unused).
        // We do this by reconstructing with a sentinel. The original dir
        // is returned to the caller via TestDataDir.
        let original_dir = {
            // SAFETY: we swap the dir out before drop so neither the old
            // nor the new TempDir is double-freed.
            let placeholder = tempfile::tempdir().unwrap();
            std::mem::replace(&mut self._dir, placeholder)
        };
        (self, TestDataDir(original_dir))
    }
}
