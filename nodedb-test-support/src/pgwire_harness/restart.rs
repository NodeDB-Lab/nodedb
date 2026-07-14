// SPDX-License-Identifier: BUSL-1.1

//! Restart-against-existing-data lifecycle: `graceful_shutdown` releases
//! every WAL / redb handle, then `open_on_path` reopens the same directory
//! (WAL replay + catalog re-registration) — the way durability tests verify
//! persistence across a restart.

use std::sync::Arc;
use std::time::Duration;

use nodedb::bridge::dispatch::Dispatcher;
use nodedb::config::auth::AuthMode;
use nodedb::control::server::pgwire::listener::PgListener;
use nodedb::control::state::SharedState;
use nodedb::event::{EventPlane, create_event_bus};
use nodedb::types::TenantId;
use nodedb::wal::WalManager;

use super::support::{bind_http_listener, bind_native_listener, init_test_memory_governor};
use super::types::{TestClient, TestDataDir, TestServer};

#[allow(dead_code)]
impl TestServer {
    /// Consume the server, send shutdown signals, and await all core threads.
    ///
    /// Use this before `TestServer::open_on_path` to guarantee that the
    /// redb environment and WAL file handles are fully released before a
    /// second server opens the same directory.
    pub async fn graceful_shutdown(mut self) {
        // Drop the client first so the underlying socket closes and the
        // server-side pgwire session can drain before we try to reopen redb.
        let _ = self.client.take();
        let _ = self.shared.wal.sync();
        if let Some(bus) = self.shutdown_bus.take() {
            bus.initiate();
        }
        // Stop driving the client connection first so the server-side pgwire
        // session drops its Arc<SharedState> before we try to reopen catalog redb.
        if let Some(h) = self.conn_handle.take() {
            h.abort();
            let _ = h.await;
        }
        if let Some(tx) = self.poller_shutdown_tx.take() {
            let _ = tx.send(true);
        }
        if let Some(txs) = self.core_stop_txs.take() {
            for tx in &txs {
                let _ = tx.send(());
            }
        }
        // Wait for Data Plane core threads — they own all engine redb handles.
        if let Some(handles) = self.core_handles.take() {
            for h in handles {
                h.abort();
                let _ = h.await;
            }
        }
        // Abort and join the poller so it drops its Arc<SharedState> clone.
        if let Some(h) = self.poller_handle.take() {
            h.abort();
            let _ = h.await;
        }
        // Let the listener complete its shutdown-bus drain. If it stalls,
        // fall back to abort so the test cannot hang indefinitely.
        if let Some(h) = self.pg_handle.take() {
            let mut h = h;
            match tokio::time::timeout(Duration::from_secs(2), &mut h).await {
                Ok(_) => {}
                Err(_) => {
                    h.abort();
                    let _ = h.await;
                }
            }
        }
        if let Some(h) = self.native_handle.take() {
            let mut h = h;
            match tokio::time::timeout(Duration::from_secs(2), &mut h).await {
                Ok(_) => {}
                Err(_) => {
                    h.abort();
                    let _ = h.await;
                }
            }
        }
        if let Some(h) = self.http_handle.take() {
            let mut h = h;
            match tokio::time::timeout(Duration::from_secs(2), &mut h).await {
                Ok(_) => {}
                Err(_) => {
                    h.abort();
                    let _ = h.await;
                }
            }
        }
        // Wait for event consumers so they drop their Arc<SharedState> and
        // WatermarkStore (redb) clones before we return.
        if let Some(ep) = self.event_plane.take() {
            ep.shutdown_and_join().await;
        }
        // Await every loop registered with LoopRegistry (scheduler, retention,
        // alert eval, etc.) so their Arc<SharedState> clones are released before
        // we return. The shutdown signal was already sent above via bus.initiate().
        self.shared
            .loop_registry
            .shutdown_all(std::time::Duration::from_secs(5))
            .await;
        // conn_handle, shared, _dir all drop here, releasing the remaining
        // Arc<SharedState> and CredentialStore redb handle.
    }

    /// Open a server backed by an existing data directory.
    ///
    /// The WAL, redb stores, and array catalog inside `dir` are reopened
    /// in-place, so any data written by a previous server is immediately
    /// visible. This is the correct way to test WAL-based durability: write
    /// data, call `take_dir()`, drop the first server, then call
    /// `open_on_path()` on the saved dir.
    pub async fn open_on_path(dir: TestDataDir) -> (Self, TestDataDir) {
        let data_dir = TestDataDir(dir.0);
        let server = Self::start_on_dir_ref(data_dir.path(), None, AuthMode::Trust, true).await;
        (server, data_dir)
    }

    /// Reopen an empty credential store in password mode without provisioning
    /// the normal harness superuser. The returned server has no preconnected
    /// harness client; callers must open the connection under test themselves.
    pub async fn open_on_path_empty_store_password(dir: TestDataDir) -> (Self, TestDataDir) {
        let data_dir = TestDataDir(dir.0);
        let server = Self::start_on_dir_ref(data_dir.path(), None, AuthMode::Password, false).await;
        (server, data_dir)
    }

    /// Open a server backed by an existing data directory with a custom
    /// `columnar_flush_threshold`.
    ///
    /// The threshold only affects ingest-time flushing while the reopened
    /// server is running (e.g. during a restore into the restarted server).
    /// Pass the same value used for the original server to keep behaviour
    /// consistent across the restart boundary.
    pub async fn open_on_path_with_columnar_flush_threshold(
        dir: TestDataDir,
        flush_threshold: usize,
    ) -> (Self, TestDataDir) {
        let data_dir = TestDataDir(dir.0);
        let server = Self::start_on_dir_ref(
            data_dir.path(),
            Some(flush_threshold),
            AuthMode::Trust,
            true,
        )
        .await;
        (server, data_dir)
    }

    /// Internal: start a server rooted at an existing path without taking
    /// ownership of the directory wrapper.
    async fn start_on_dir_ref(
        dir_path: &std::path::Path,
        columnar_flush_threshold: Option<usize>,
        auth_mode: AuthMode,
        provision_superuser: bool,
    ) -> Self {
        let wal_path = dir_path.join("test.wal");
        let wal = Arc::new(WalManager::open_for_testing(&wal_path).unwrap());
        let wal_records: Arc<[nodedb_wal::WalRecord]> =
            Arc::from(wal.replay().unwrap().into_boxed_slice());
        let replay_tombstones = nodedb_wal::extract_tombstones(&wal_records);

        let (dispatcher, data_sides) = Dispatcher::new(1, 64);
        let (event_producers, event_consumers) = create_event_bus(1);

        let catalog_path = dir_path.join("system.redb");
        let credentials = Arc::new(
            nodedb::control::security::credential::store::CredentialStore::open(&catalog_path)
                .unwrap(),
        );
        if provision_superuser {
            let _ = credentials.create_user(
                "nodedb",
                "nodedb",
                TenantId::new(1),
                vec![nodedb::control::security::identity::Role::Superuser],
            );
        }
        let mut shared =
            SharedState::new_with_credentials(dispatcher, Arc::clone(&wal), credentials)
                .expect("build shared state");
        if let Some(s) = Arc::get_mut(&mut shared) {
            s.backup_kek = Some(Arc::new([0x42u8; 32]));
            s.governor = init_test_memory_governor();
        }
        let shared = shared;
        nodedb::bootstrap::credentials::replay_surrogate_wal(&shared, &wal_records);
        // Restore in-memory synonym registry from the persisted catalog.
        let catalog = shared.credentials.catalog();
        if let Err(e) = shared.synonym_registry.reload_from_catalog(catalog) {
            eprintln!("pgwire_harness: failed to reload synonym groups: {e}");
        }
        let catalog = shared.credentials.catalog();
        if let Ok(entries) = catalog.load_all_arrays()
            && let Ok(mut guard) = shared.array_catalog.write()
        {
            for entry in entries {
                let _ = guard.register(entry);
            }
        }

        let mut core_stop_txs = Vec::new();
        let mut core_handles = Vec::new();
        for (idx, (data_side, event_producer)) in
            data_sides.into_iter().zip(event_producers).enumerate()
        {
            let (core_stop_tx, core_stop_rx) = std::sync::mpsc::channel::<()>();
            let replay = (!wal_records.is_empty()).then(|| crate::core_loop_runner::WalReplay {
                records: Arc::clone(&wal_records),
                tombstones: replay_tombstones.clone(),
                // Single-core pgwire harness (`Dispatcher::new(1, ..)`).
                num_cores: 1,
            });
            let core_handle =
                crate::core_loop_runner::spawn_core_loop(crate::core_loop_runner::CoreLoopSpawn {
                    idx,
                    data_side,
                    core_dir: dir_path.to_path_buf(),
                    core_array_catalog: shared.array_catalog.clone(),
                    event_producer,
                    core_metrics: None,
                    governor: shared.governor.clone(),
                    replay,
                    graph_tuning: nodedb_types::config::tuning::GraphTuning::default(),
                    query_tuning: {
                        let mut qt = nodedb_types::config::tuning::QueryTuning::default();
                        if let Some(threshold) = columnar_flush_threshold {
                            qt.columnar_flush_threshold = threshold;
                        }
                        qt
                    },
                    stop_rx: core_stop_rx,
                });
            core_stop_txs.push(core_stop_tx);
            core_handles.push(core_handle);
        }

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

        nodedb::bootstrap::schema_rehydrate::rehydrate_schema_registry(&shared)
            .await
            .expect("schema rehydration on restart");

        // Re-register every persisted continuous aggregate on the local
        // Data Plane manager: the registry is per-core in-memory state
        // and is otherwise lost across restart.
        nodedb::control::server::shared::ddl::neutral::continuous_agg::register_persisted_continuous_aggregates(
            &shared,
        )
        .await;

        let watermark_store =
            Arc::new(nodedb::event::watermark::WatermarkStore::open(dir_path).unwrap());
        let trigger_dlq = Arc::new(std::sync::Mutex::new(
            nodedb::event::trigger::TriggerDlq::open(dir_path).unwrap(),
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

        let pg_listener = PgListener::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let pg_addr = pg_listener.local_addr();

        let (shutdown_bus, _) =
            nodedb::control::shutdown::ShutdownBus::new(Arc::clone(&shared.shutdown));
        let conn_semaphore = Arc::new(tokio::sync::Semaphore::new(128));
        let shared_pg = Arc::clone(&shared);
        let test_startup_gate = Arc::clone(&shared.startup);
        let bus_pg = shutdown_bus.clone();
        let pg_sem = Arc::clone(&conn_semaphore);
        let listener_auth_mode = auth_mode.clone();
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

        let (native_port, native_handle) =
            bind_native_listener(&shared, &shutdown_bus, Arc::clone(&conn_semaphore)).await;
        let (http_port, http_handle) = bind_http_listener(&shared, &shutdown_bus).await;

        tokio::time::sleep(Duration::from_millis(50)).await;

        let (client, conn_handle) = if provision_superuser {
            let conn_str = match auth_mode {
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
            (TestClient::new(client), Some(conn_handle))
        } else {
            (TestClient::empty(), None)
        };

        // Use a new temp dir as the placeholder _dir (data is already open from dir_path).
        let placeholder_dir = tempfile::tempdir().unwrap();

        Self {
            client,
            pg_port: pg_addr.port(),
            native_port,
            http_port,
            shared,
            conn_handle,
            shutdown_bus: Some(shutdown_bus),
            poller_shutdown_tx: Some(poller_shutdown_tx),
            core_stop_txs: Some(core_stop_txs),
            pg_handle: Some(pg_handle),
            native_handle: Some(native_handle),
            http_handle: Some(http_handle),
            poller_handle: Some(poller_handle),
            core_handles: Some(core_handles),
            event_plane: Some(event_plane),
            _dir: placeholder_dir,
        }
    }
}
