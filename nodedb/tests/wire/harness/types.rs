// SPDX-License-Identifier: BUSL-1.1

//! Core harness types: the running [`TestServer`], its [`TestClient`]
//! wrapper, and the [`TestDataDir`] handle for cross-restart persistence
//! tests.

use super::process::SpawnedServer;

pub struct TestClient(Option<tokio_postgres::Client>);

impl TestClient {
    pub(super) fn new(client: tokio_postgres::Client) -> Self {
        Self(Some(client))
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

/// A running `nodedb` server subprocess, normally with a connected pgwire
/// harness client.
#[allow(dead_code)]
pub struct TestServer {
    pub client: TestClient,
    pub pg_port: u16,
    /// Native protocol (MessagePack) listener port.
    pub native_port: u16,
    /// HTTP (REST) listener port — also where `/healthz` was polled.
    pub http_port: u16,
    /// RESP (Redis protocol) listener port.
    pub resp_port: u16,
    /// `None` once `graceful_shutdown` has consumed it.
    pub(super) spawned: Option<SpawnedServer>,
    pub(super) conn_handle: Option<tokio::task::JoinHandle<()>>,
    // Swapped out by `take_dir`, which replaces this with a fresh empty
    // placeholder so `Drop` never removes a directory the caller now owns.
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
        // Dropping `spawned` (when still `Some`) runs `SpawnedServer::Drop`,
        // which kills and reaps the child. `None` here means
        // `graceful_shutdown` already did that cleanly.
        self.spawned.take();
        let _ = self.client.take();
        if let Some(h) = self.conn_handle.take() {
            h.abort();
        }
    }
}
