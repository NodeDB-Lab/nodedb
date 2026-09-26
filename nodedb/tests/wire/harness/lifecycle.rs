// SPDX-License-Identifier: BUSL-1.1

//! `start*` / `open_on_path*` constructors, `take_dir`, and
//! `graceful_shutdown` on [`TestServer`].
//!
//! Each constructor spawns a real `nodedb` subprocess (see `process.rs`)
//! against a temp directory and connects a `tokio_postgres::Client` to it.

use std::time::Duration;

use super::config_toml::{AuthMode, TuningOverrides};
use super::process::{self, SpawnedServer};
use super::types::{TestClient, TestDataDir, TestServer};

impl TestServer {
    /// Spawn a single-core NodeDB server and connect via pgwire (trust mode).
    /// The `/healthz` wait in `process.rs` covers Calvin sequencer readiness,
    /// so a cross-shard write issued right after this returns cannot lose the
    /// post-boot election race.
    pub async fn start() -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        let spawned = process::spawn(dir.path(), AuthMode::Trust, TuningOverrides::none(), 1);
        Self::connect_and_build(spawned, dir, AuthMode::Trust).await
    }

    /// Spawn a NodeDB server with `cores` Data Plane cores and connect via
    /// pgwire (trust mode). Exercises fan-out/gather across cores, which a
    /// single-core server cannot.
    pub async fn start_multicores(cores: usize) -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        let spawned = process::spawn(dir.path(), AuthMode::Trust, TuningOverrides::none(), cores);
        Self::connect_and_build(spawned, dir, AuthMode::Trust).await
    }

    /// Spawn a single-core server in pgwire password mode (SCRAM-SHA-256)
    /// with the credential lockout policy enabled (`5` failures -> `300s`).
    /// The harness user `nodedb` keeps password `nodedb`.
    pub async fn start_password() -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        let spawned = process::spawn(dir.path(), AuthMode::Password, TuningOverrides::none(), 1);
        Self::connect_and_build(spawned, dir, AuthMode::Password).await
    }

    /// Spawn a single-core NodeDB server with `NODEDB_FAILPOINTS=spec` set on
    /// the subprocess (see `nodedb_types::fail_point::FAILPOINTS_ENV`
    /// format), so a fail point compiled in under the `failpoints` Cargo
    /// feature fires inside the spawned server. Only meaningful when the
    /// `nodedb` binary under test was itself built with `--features
    /// failpoints` — a plain build ignores the variable and the fail point
    /// never fires, since `fail_point_err!` compiles to nothing without it.
    #[cfg(feature = "failpoints")]
    pub async fn start_with_failpoints(spec: &str) -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        let spawned = process::spawn_with_failpoints(
            dir.path(),
            AuthMode::Trust,
            TuningOverrides::none(),
            1,
            Some(spec),
        );
        Self::connect_and_build(spawned, dir, AuthMode::Trust).await
    }

    /// Spawn a single-core NodeDB server with a lowered
    /// `columnar_flush_threshold` so tests can observe segment-flush
    /// behaviour on small datasets without inserting 65k rows.
    pub async fn start_with_columnar_flush_threshold(flush_threshold: usize) -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        let spawned = process::spawn(
            dir.path(),
            AuthMode::Trust,
            TuningOverrides::columnar_flush(flush_threshold),
            1,
        );
        Self::connect_and_build(spawned, dir, AuthMode::Trust).await
    }

    /// Spawn a single-core NodeDB server with a lowered auto-ANALYZE
    /// mutation floor so a test can trip the trigger on a small write count.
    pub async fn start_with_auto_analyze_threshold(min_mutations: u64) -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        let spawned = process::spawn(
            dir.path(),
            AuthMode::Trust,
            TuningOverrides::auto_analyze(min_mutations),
            1,
        );
        Self::connect_and_build(spawned, dir, AuthMode::Trust).await
    }

    /// Spawn a single-core NodeDB server with a lowered scan streaming chunk
    /// size so a test drives the chunked-streaming scan path on a small seed.
    pub async fn start_with_stream_chunk_size(rows: usize) -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        let spawned = process::spawn(
            dir.path(),
            AuthMode::Trust,
            TuningOverrides::stream_chunk(rows),
            1,
        );
        Self::connect_and_build(spawned, dir, AuthMode::Trust).await
    }

    /// Spawn a single-core NodeDB server with a lowered timeseries memtable
    /// budget so every ingest flushes its rows to a partition, and a read
    /// exercises the partition path on a handful of rows.
    pub async fn start_with_timeseries_memtable_budget(bytes: usize) -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        let spawned = process::spawn(
            dir.path(),
            AuthMode::Trust,
            TuningOverrides::timeseries_memtable_budget(bytes),
            1,
        );
        Self::connect_and_build(spawned, dir, AuthMode::Trust).await
    }

    /// Spawn a single-core NodeDB server with `single_node_calvin = false`:
    /// no cluster topology, so the planner emits the single-node plan forms
    /// (`ArrayOp::{Put, Delete, Slice, ...}`) rather than the `ClusterArrayOp`
    /// routing wrappers. Single-vShard transactions commit through the
    /// single-shard path; cross-shard interactive transactions are rejected.
    pub async fn start_standalone() -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        let spawned = process::spawn(
            dir.path(),
            AuthMode::Trust,
            TuningOverrides::standalone(),
            1,
        );
        Self::connect_and_build(spawned, dir, AuthMode::Trust).await
    }

    /// Open a server backed by an existing data directory, reopened in place
    /// so a previous server's data is visible after boot. `dir` is not
    /// consumed — ownership stays with the caller.
    pub async fn open_on_path(dir: TestDataDir) -> (Self, TestDataDir) {
        let spawned = process::spawn(dir.path(), AuthMode::Trust, TuningOverrides::none(), 1);
        let placeholder = tempfile::tempdir().expect("placeholder tempdir");
        let server = Self::connect_and_build(spawned, placeholder, AuthMode::Trust).await;
        (server, dir)
    }

    /// [`Self::open_on_path`] for a server booted with
    /// [`Self::start_standalone`]: no cluster topology, so no Raft groups.
    pub async fn open_on_path_standalone(dir: TestDataDir) -> (Self, TestDataDir) {
        let spawned = process::spawn(
            dir.path(),
            AuthMode::Trust,
            TuningOverrides::standalone(),
            1,
        );
        let placeholder = tempfile::tempdir().expect("placeholder tempdir");
        let server = Self::connect_and_build(spawned, placeholder, AuthMode::Trust).await;
        (server, dir)
    }

    /// Open a server backed by an existing data directory with a custom
    /// `columnar_flush_threshold`. Pass the same value the original server
    /// used to keep flush behaviour consistent across the restart boundary.
    pub async fn open_on_path_with_columnar_flush_threshold(
        dir: TestDataDir,
        flush_threshold: usize,
    ) -> (Self, TestDataDir) {
        let spawned = process::spawn(
            dir.path(),
            AuthMode::Trust,
            TuningOverrides::columnar_flush(flush_threshold),
            1,
        );
        let placeholder = tempfile::tempdir().expect("placeholder tempdir");
        let server = Self::connect_and_build(spawned, placeholder, AuthMode::Trust).await;
        (server, dir)
    }

    /// Consume the data directory from a live server so it outlives the
    /// server's lifetime, surviving the `Drop` of `TestServer`.
    pub fn take_dir(mut self) -> (Self, TestDataDir) {
        let placeholder = tempfile::tempdir().expect("placeholder tempdir");
        let original_dir = std::mem::replace(&mut self._dir, placeholder);
        (self, TestDataDir(original_dir))
    }

    /// Consume the server, close the harness connection, and send `SIGTERM`
    /// to the subprocess, waiting for its own graceful shutdown (WAL sync)
    /// to complete before returning.
    pub async fn graceful_shutdown(mut self) {
        let _ = self.client.take();
        if let Some(h) = self.conn_handle.take() {
            h.abort();
            let _ = h.await;
        }
        if let Some(spawned) = self.spawned.take() {
            spawned.graceful_shutdown().await;
        }
    }

    /// Connect the harness client to a just-spawned server and assemble the
    /// `TestServer`. `dir` becomes the new `_dir` field.
    async fn connect_and_build(
        spawned: SpawnedServer,
        dir: tempfile::TempDir,
        auth_mode: AuthMode,
    ) -> Self {
        let conn_str = match auth_mode {
            AuthMode::Password => format!(
                "host=127.0.0.1 port={} user=nodedb password=nodedb dbname=default",
                spawned.ports.pgwire
            ),
            AuthMode::Trust => format!(
                "host=127.0.0.1 port={} user=nodedb dbname=default",
                spawned.ports.pgwire
            ),
        };
        // /healthz reports ready before pgwire finishes its own startup gate;
        // retry briefly rather than treating a refused connection as fatal.
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        let (client, connection) = loop {
            match tokio_postgres::connect(&conn_str, tokio_postgres::NoTls).await {
                Ok(pair) => break pair,
                Err(_) if std::time::Instant::now() < deadline => {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                Err(e) => panic!("pgwire connect failed: {e}"),
            }
        };
        let conn_handle = tokio::spawn(async move {
            let _ = connection.await;
        });

        Self {
            client: TestClient::new(client),
            pg_port: spawned.ports.pgwire,
            native_port: spawned.ports.native,
            http_port: spawned.ports.http,
            resp_port: spawned.ports.resp,
            spawned: Some(spawned),
            conn_handle: Some(conn_handle),
            _dir: dir,
        }
    }
}
