// SPDX-License-Identifier: BUSL-1.1

//! Core harness types: the running [`TestServer`], its [`TestClient`]
//! wrapper, and the [`TestDataDir`] handle for cross-restart persistence
//! tests.

use std::sync::Arc;

use nodedb::control::state::SharedState;
use nodedb::event::EventPlane;

pub struct TestClient(Option<tokio_postgres::Client>);

impl TestClient {
    pub(super) fn new(client: tokio_postgres::Client) -> Self {
        Self(Some(client))
    }

    pub(super) fn empty() -> Self {
        Self(None)
    }

    pub(super) fn take(&mut self) -> Option<tokio_postgres::Client> {
        self.0.take()
    }

    pub(super) fn as_ref(&self) -> &tokio_postgres::Client {
        self.0.as_ref().expect("test client already closed")
    }
}

impl std::ops::Deref for TestClient {
    type Target = tokio_postgres::Client;

    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

/// A running test server, normally with a connected pgwire harness client.
/// Empty-credential-store authentication fixtures intentionally leave `client`
/// disconnected so tests can choose the identity to connect as.
pub struct TestServer {
    pub client: TestClient,
    pub pg_port: u16,
    /// Native protocol (MessagePack) listener port. Bound to a fresh
    /// `127.0.0.1:0` per harness so `NativeClient::connect` can reach
    /// it without configuration.
    pub native_port: u16,
    /// HTTP (REST) listener port, bound to a fresh `127.0.0.1:0` per harness.
    pub http_port: u16,
    /// Underlying shared state — exposed so integration tests can drive
    /// store-level side effects (e.g. seeding a session handle with a
    /// specific `ClientFingerprint`) before hitting the wire.
    #[allow(dead_code)]
    pub shared: Arc<SharedState>,
    pub(super) conn_handle: Option<tokio::task::JoinHandle<()>>,
    // Fields wrapped in Option so that `graceful_shutdown(self)` can `.take()`
    // them without moving out of a type that has a `Drop` impl (E0509).
    // `Drop` checks each one and is a no-op when already taken.
    pub(super) shutdown_bus: Option<nodedb::control::shutdown::ShutdownBus>,
    pub(super) poller_shutdown_tx: Option<tokio::sync::watch::Sender<bool>>,
    pub(super) core_stop_txs: Option<Vec<std::sync::mpsc::Sender<()>>>,
    pub(super) pg_handle: Option<tokio::task::JoinHandle<()>>,
    pub(super) native_handle: Option<tokio::task::JoinHandle<()>>,
    pub(super) http_handle: Option<tokio::task::JoinHandle<()>>,
    pub(super) poller_handle: Option<tokio::task::JoinHandle<()>>,
    pub(super) core_handles: Option<Vec<tokio::task::JoinHandle<()>>>,
    pub(super) event_plane: Option<EventPlane>,
    pub(super) _dir: tempfile::TempDir,
}

/// A data directory whose lifetime is decoupled from a `TestServer` instance.
///
/// Obtaining this handle via `TestServer::take_dir()` lets a test shut down
/// one server, inspect or verify the on-disk state, and then call
/// `TestServer::open_on_path()` to reopen against the same files — verifying
/// WAL recovery and persistence across restarts.
pub struct TestDataDir(pub tempfile::TempDir);

impl TestDataDir {
    pub fn path(&self) -> &std::path::Path {
        self.0.path()
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        // Each field is `None` when `graceful_shutdown` already ran; skip in that case.
        if let Some(bus) = self.shutdown_bus.take() {
            bus.initiate();
        }
        if let Some(tx) = self.poller_shutdown_tx.take() {
            let _ = tx.send(true);
        }
        if let Some(txs) = self.core_stop_txs.take() {
            for tx in &txs {
                let _ = tx.send(());
            }
        }
        let _ = self.client.take();
        if let Some(h) = self.conn_handle.take() {
            h.abort();
        }
        if let Some(h) = self.pg_handle.take() {
            h.abort();
        }
        if let Some(h) = self.native_handle.take() {
            h.abort();
        }
        if let Some(h) = self.http_handle.take() {
            h.abort();
        }
        if let Some(h) = self.poller_handle.take() {
            h.abort();
        }
        if let Some(handles) = self.core_handles.take() {
            for h in handles {
                h.abort();
            }
        }
    }
}
