// SPDX-License-Identifier: BUSL-1.1

//! Multi-core `TestServer::start_multicores` — every core receives Register
//! dispatches and participates in the cross-core schema-visibility barrier,
//! so tests can assert a schema change is visible on every core before the
//! DDL returns success.

use std::sync::Arc;
use std::time::Duration;

use nodedb::bridge::dispatch::Dispatcher;
use nodedb::config::auth::AuthMode;
use nodedb::control::server::pgwire::listener::PgListener;
use nodedb::control::state::SharedState;
use nodedb::event::{EventPlane, EventPlaneConfig, create_event_bus};
use nodedb::wal::WalManager;

use super::support::{bind_http_listener, bind_native_listener, init_test_memory_governor};
use super::types::{TestClient, TestServer};

#[allow(dead_code)]
impl TestServer {
    /// Spawn an N-core NodeDB server and connect via pgwire.
    ///
    /// All N cores receive Register dispatches and are covered by the
    /// cross-core schema visibility barrier.  Use this variant in tests
    /// that verify schema changes are visible on every core before DDL
    /// returns success.
    pub async fn start_multicores(num_cores: usize) -> Self {
        assert!(num_cores >= 1, "num_cores must be at least 1");
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("test.wal");
        let wal = Arc::new(WalManager::open_for_testing(&wal_path).unwrap());

        let (dispatcher, data_sides) = Dispatcher::new(num_cores, 64);
        let (event_producers, event_consumers) = create_event_bus(num_cores);

        let catalog_path = dir.path().join("system.redb");
        let credentials = Arc::new(
            nodedb::control::security::credential::store::CredentialStore::open(&catalog_path)
                .unwrap(),
        );
        credentials
            .bootstrap_trust_superuser("nodedb")
            .expect("bootstrap trust superuser");
        let mut shared =
            SharedState::new_with_credentials(dispatcher, Arc::clone(&wal), credentials, false)
                .expect("build shared state");
        if let Some(s) = Arc::get_mut(&mut shared) {
            s.backup_kek = Some(Arc::new([0x42u8; 32]));
            s.governor = init_test_memory_governor();
        }
        let shared = shared;

        let mut core_stop_txs = Vec::new();
        let mut core_handles = Vec::new();
        for (idx, (data_side, event_producer)) in
            data_sides.into_iter().zip(event_producers).enumerate()
        {
            let (core_stop_tx, core_stop_rx) = std::sync::mpsc::channel::<()>();
            let core_handle =
                crate::core_loop_runner::spawn_core_loop(crate::core_loop_runner::CoreLoopSpawn {
                    idx,
                    num_cores,
                    data_side,
                    core_dir: dir.path().to_path_buf(),
                    core_array_catalog: shared.array_catalog.clone(),
                    event_producer,
                    core_metrics: None,
                    governor: shared.governor.clone(),
                    replay: None,
                    graph_tuning: nodedb_types::config::tuning::GraphTuning::default(),
                    query_tuning: nodedb_types::config::tuning::QueryTuning::default(),
                    // Seeded from the SAME durable catalog production reads, so a
                    // harness restart reconstructs cores the way a real one does.
                    // An empty catalog yields an empty seed, which is exactly what
                    // production gets booting a fresh data dir.
                    doc_config_seed: nodedb::bootstrap::data_plane::load_doc_config_registry_from(
                        shared.credentials.catalog(),
                    ),
                    stop_rx: core_stop_rx,
                });
            core_stop_txs.push(core_stop_tx);
            core_handles.push(core_handle);
        }

        let shared_poller = Arc::clone(&shared);
        let (poller_shutdown_tx, mut poller_shutdown_rx) = tokio::sync::watch::channel(false);
        let poller_handle = tokio::spawn(async move {
            let mut last_rate_reset = std::time::Instant::now();
            loop {
                shared_poller.poll_and_route_responses();
                super::support::reset_tenant_rate_counters_each_second(
                    &shared_poller,
                    &mut last_rate_reset,
                );
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
        let (shutdown_bus, _) =
            nodedb::control::shutdown::ShutdownBus::new(Arc::clone(&shared.shutdown));
        let event_plane = EventPlane::spawn(EventPlaneConfig {
            consumers_rx: event_consumers,
            wal: Arc::clone(&wal),
            watermark_store,
            shared_state: Arc::clone(&shared),
            trigger_dlq,
            cdc_router: Arc::clone(&shared.cdc_router),
            shutdown: Arc::clone(&shared.shutdown),
            shutdown_bus: shutdown_bus.clone(),
        });

        let pg_listener = PgListener::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let pg_addr = pg_listener.local_addr();

        let conn_semaphore = Arc::new(tokio::sync::Semaphore::new(128));
        let shared_pg = Arc::clone(&shared);
        let test_startup_gate = Arc::clone(&shared.startup);
        let bus_pg = shutdown_bus.clone();
        let pg_sem = Arc::clone(&conn_semaphore);
        let pg_handle = tokio::spawn(async move {
            pg_listener
                .run(
                    shared_pg,
                    AuthMode::Trust,
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

        let conn_str = format!(
            "host=127.0.0.1 port={} user=nodedb dbname=default",
            pg_addr.port()
        );
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
}
