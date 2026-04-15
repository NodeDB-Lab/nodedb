//! Shared pgwire end-to-end test harness.
//!
//! Spawns a full NodeDB server (Data Plane core + pgwire listener + response poller)
//! and provides a connected `tokio_postgres::Client` for SQL execution.

use std::sync::Arc;
use std::time::Duration;

use nodedb::bridge::dispatch::Dispatcher;
use nodedb::config::auth::AuthMode;
use nodedb::control::server::pgwire::listener::PgListener;
use nodedb::control::state::SharedState;
use nodedb::data::executor::core_loop::CoreLoop;
use nodedb::event::{EventPlane, create_event_bus};
use nodedb::wal::WalManager;

/// A running test server with a connected pgwire client.
pub struct TestServer {
    pub client: tokio_postgres::Client,
    _conn_handle: tokio::task::JoinHandle<()>,
    shutdown_bus: nodedb::control::shutdown::ShutdownBus,
    poller_shutdown_tx: tokio::sync::watch::Sender<bool>,
    core_stop_tx: std::sync::mpsc::Sender<()>,
    _pg_handle: tokio::task::JoinHandle<()>,
    _poller_handle: tokio::task::JoinHandle<()>,
    _core_handle: tokio::task::JoinHandle<()>,
    _event_plane: EventPlane,
    _dir: tempfile::TempDir,
}

#[allow(dead_code)]
impl TestServer {
    /// Spawn a single-core NodeDB server and connect via pgwire.
    pub async fn start() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("test.wal");
        let wal = Arc::new(WalManager::open_for_testing(&wal_path).unwrap());

        let (dispatcher, data_sides) = Dispatcher::new(1, 64);
        let (event_producers, event_consumers) = create_event_bus(1);

        // Use catalog-backed credential store (required for CREATE FUNCTION/TRIGGER/PROCEDURE).
        let catalog_path = dir.path().join("system.redb");
        let credentials = Arc::new(
            nodedb::control::security::credential::store::CredentialStore::open(&catalog_path)
                .unwrap(),
        );
        let shared = SharedState::new_with_credentials(dispatcher, Arc::clone(&wal), credentials);

        // Data Plane core.
        let data_side = data_sides.into_iter().next().unwrap();
        let core_dir = dir.path().to_path_buf();
        let event_producer = event_producers.into_iter().next().unwrap();
        let (core_stop_tx, core_stop_rx) = std::sync::mpsc::channel::<()>();
        let core_handle = tokio::task::spawn_blocking(move || {
            let mut core =
                CoreLoop::open(0, data_side.request_rx, data_side.response_tx, &core_dir).unwrap();
            core.set_event_producer(event_producer);
            while matches!(
                core_stop_rx.try_recv(),
                Err(std::sync::mpsc::TryRecvError::Empty)
            ) {
                core.tick();
                std::thread::sleep(Duration::from_millis(1));
            }
        });

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
        let shared_pg = Arc::clone(&shared);
        // Use the startup gate already on SharedState (a pre-fired placeholder
        // from `new_inner`). The listener starts accepting immediately.
        let test_startup_gate = Arc::clone(&shared.startup);
        let bus_pg = shutdown_bus.clone();
        let pg_handle = tokio::spawn(async move {
            pg_listener
                .run(
                    shared_pg,
                    AuthMode::Trust,
                    None,
                    Arc::new(tokio::sync::Semaphore::new(128)),
                    test_startup_gate,
                    bus_pg,
                )
                .await
                .unwrap();
        });

        tokio::time::sleep(Duration::from_millis(50)).await;

        // Connect client.
        let conn_str = format!(
            "host=127.0.0.1 port={} user=nodedb dbname=nodedb",
            pg_addr.port()
        );
        let (client, connection) = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls)
            .await
            .expect("pgwire connect failed");

        let conn_handle = tokio::spawn(async move {
            let _ = connection.await;
        });

        Self {
            client,
            _conn_handle: conn_handle,
            shutdown_bus,
            poller_shutdown_tx,
            core_stop_tx,
            _pg_handle: pg_handle,
            _poller_handle: poller_handle,
            _core_handle: core_handle,
            _event_plane: event_plane,
            _dir: dir,
        }
    }

    /// Execute a SQL statement, returning the text of each row's first column.
    pub async fn query_text(&self, sql: &str) -> Result<Vec<String>, String> {
        match self.client.simple_query(sql).await {
            Ok(msgs) => {
                let mut rows = Vec::new();
                for msg in msgs {
                    if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
                        rows.push(row.get(0).unwrap_or("").to_string());
                    }
                }
                Ok(rows)
            }
            Err(e) => Err(pg_error_detail(&e)),
        }
    }

    /// Execute a SQL statement expecting success (no result needed).
    pub async fn exec(&self, sql: &str) -> Result<(), String> {
        match self.client.simple_query(sql).await {
            Ok(_) => Ok(()),
            Err(e) => Err(pg_error_detail(&e)),
        }
    }

    /// Execute a SQL statement expecting an error containing the given substring.
    pub async fn expect_error(&self, sql: &str, expected_substring: &str) {
        match self.client.simple_query(sql).await {
            Ok(_) => panic!("expected error containing '{expected_substring}', got success"),
            Err(e) => {
                let msg = pg_error_detail(&e);
                assert!(
                    msg.to_lowercase()
                        .contains(&expected_substring.to_lowercase()),
                    "expected error containing '{expected_substring}', got: {msg}"
                );
            }
        }
    }
}

/// Extract detailed error message from a tokio-postgres error.
///
/// tokio-postgres `Error::to_string()` just returns "db error" — useless for debugging.
/// This function extracts the actual server message from the `DbError` if available.
fn pg_error_detail(e: &tokio_postgres::Error) -> String {
    if let Some(db_err) = e.as_db_error() {
        format!(
            "{}: {} (SQLSTATE {})",
            db_err.severity(),
            db_err.message(),
            db_err.code().code()
        )
    } else {
        format!("{e:?}")
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.shutdown_bus.initiate();
        let _ = self.poller_shutdown_tx.send(true);
        let _ = self.core_stop_tx.send(());
    }
}
