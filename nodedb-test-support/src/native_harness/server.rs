// SPDX-License-Identifier: BUSL-1.1

//! `NativeTestServer` — spawns a single-core NodeDB server with the native
//! (MessagePack/JSON) protocol listener bound to an ephemeral port.

use std::sync::Arc;
use std::time::Duration;

use nodedb::bridge::dispatch::Dispatcher;
use nodedb::config::auth::AuthMode;
use nodedb::control::server::listener::Listener;
use nodedb::control::state::SharedState;
use nodedb::data::executor::core_loop::CoreLoop;
use nodedb::event::{EventPlane, create_event_bus};
use nodedb::wal::WalManager;

/// A running native-protocol test server.
pub struct NativeTestServer {
    pub addr: std::net::SocketAddr,
    pub shared: Arc<SharedState>,
    pub(super) shutdown_bus: nodedb::control::shutdown::ShutdownBus,
    pub(super) poller_shutdown_tx: tokio::sync::watch::Sender<bool>,
    pub(super) core_stop_tx: std::sync::mpsc::Sender<()>,
    pub(super) _listener_handle: tokio::task::JoinHandle<()>,
    pub(super) _poller_handle: tokio::task::JoinHandle<()>,
    pub(super) _core_handle: tokio::task::JoinHandle<()>,
    pub(super) _event_plane: EventPlane,
    pub(super) _dir: tempfile::TempDir,
}

impl NativeTestServer {
    /// Spawn a single-core NodeDB server with the native listener bound to
    /// an ephemeral `127.0.0.1` port (trust-mode auth).
    pub async fn start() -> Self {
        Self::start_with_auth_mode(AuthMode::Trust).await
    }

    /// Spawn a single-core server that requires an explicit native Auth frame.
    pub async fn start_authenticated() -> Self {
        Self::start_with_auth_mode(AuthMode::Password).await
    }

    async fn start_with_auth_mode(auth_mode: AuthMode) -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal_path = dir.path().join("test.wal");
        let wal = Arc::new(WalManager::open_for_testing(&wal_path).expect("open wal"));

        let (dispatcher, data_sides) = Dispatcher::new(1, 64);
        let (event_producers, event_consumers) = create_event_bus(1);

        // Use catalog-backed credential store (mirrors pgwire_harness::start)
        // so DDL apply (`apply_locally_if_needed`) and planner reads
        // (`OriginCatalog::get_collection`) resolve against a real catalog and
        // collections created over the native protocol are visible.
        let catalog_path = dir.path().join("system.redb");
        let credential_store =
            nodedb::control::security::credential::store::CredentialStore::open(&catalog_path)
                .expect("open credential store");
        let credentials = Arc::new(credential_store);
        // Provision the harness superuser `nodedb` so Trust-mode strict
        // identity resolution accepts the default test connection.
        let _ = credentials.create_user(
            "nodedb",
            "nodedb",
            nodedb::types::TenantId::new(1),
            vec![nodedb::control::security::identity::Role::Superuser],
        );
        // Ensure the built-in `default` database (id 0) is present in the
        // catalog so the default connection database works in tests.
        let _ = credentials.catalog().bootstrap_default_database();
        let shared = SharedState::new_with_credentials(dispatcher, Arc::clone(&wal), credentials)
            .expect("build shared state");

        let data_side = data_sides.into_iter().next().expect("data side");
        let core_dir = dir.path().to_path_buf();
        let event_producer = event_producers.into_iter().next().expect("event producer");
        let core_array_catalog = shared.array_catalog.clone();
        let (core_stop_tx, core_stop_rx) = std::sync::mpsc::channel::<()>();
        let _core_handle = tokio::task::spawn_blocking(move || {
            let mut core = CoreLoop::open_with_array_catalog(
                0,
                data_side.request_rx,
                data_side.response_tx,
                &core_dir,
                std::sync::Arc::new(nodedb_types::OrdinalClock::new()),
                core_array_catalog,
            )
            .expect("open core");
            core.set_event_producer(event_producer);
            while matches!(
                core_stop_rx.try_recv(),
                Err(std::sync::mpsc::TryRecvError::Empty)
            ) {
                core.tick();
                std::thread::sleep(Duration::from_millis(1));
            }
        });

        let shared_poller = Arc::clone(&shared);
        let (poller_shutdown_tx, mut poller_shutdown_rx) = tokio::sync::watch::channel(false);
        let _poller_handle = tokio::spawn(async move {
            loop {
                shared_poller.poll_and_route_responses();
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_millis(1)) => {}
                    _ = poller_shutdown_rx.changed() => break,
                }
            }
        });

        let watermark_store = Arc::new(
            nodedb::event::watermark::WatermarkStore::open(dir.path()).expect("watermark"),
        );
        let trigger_dlq = Arc::new(std::sync::Mutex::new(
            nodedb::event::trigger::TriggerDlq::open(dir.path()).expect("trigger dlq"),
        ));
        let _event_plane = EventPlane::spawn(
            event_consumers,
            Arc::clone(&wal),
            watermark_store,
            Arc::clone(&shared),
            trigger_dlq,
            Arc::clone(&shared.cdc_router),
            Arc::clone(&shared.shutdown),
        );

        let listener = Listener::bind("127.0.0.1:0".parse().expect("addr"))
            .await
            .expect("bind");
        let addr = listener.local_addr();

        let (shutdown_bus, _) =
            nodedb::control::shutdown::ShutdownBus::new(Arc::clone(&shared.shutdown));
        let shared_listener = Arc::clone(&shared);
        let test_startup_gate = Arc::clone(&shared.startup);
        let bus_listener = shutdown_bus.clone();
        let _listener_handle = tokio::spawn(async move {
            listener
                .run(nodedb::control::server::listener::ListenerRunParams {
                    state: shared_listener,
                    auth_mode,
                    tls_acceptor: None,
                    conn_semaphore: Arc::new(tokio::sync::Semaphore::new(128)),
                    startup_gate: test_startup_gate,
                    bus: bus_listener,
                    admission: Arc::new(
                        nodedb::control::server::admission::AdmissionRegistry::new(),
                    ),
                })
                .await
                .expect("listener");
        });

        tokio::time::sleep(Duration::from_millis(50)).await;

        Self {
            addr,
            shared,
            shutdown_bus,
            poller_shutdown_tx,
            core_stop_tx,
            _listener_handle,
            _poller_handle,
            _core_handle,
            _event_plane,
            _dir: dir,
        }
    }

    /// Shut down the server and give background tasks time to unwind.
    pub async fn shutdown(self) {
        self.shutdown_bus.initiate();
        let _ = self.poller_shutdown_tx.send(true);
        let _ = self.core_stop_tx.send(());
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
