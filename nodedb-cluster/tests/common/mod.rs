//! Shared integration-test helpers: a full in-process `TestNode`
//! that owns a transport, catalog, topology, lifecycle tracker, and
//! the two background tasks (`serve`, `run`) a production-shaped
//! node needs.
//!
//! Every integration test file under `nodedb-cluster/tests/` imports
//! this module via `mod common;` — by convention the subdirectory
//! form (`tests/common/mod.rs`) is NOT picked up by cargo as a test
//! crate itself, so it can contain the shared scaffolding without
//! showing up in the test harness.
//!
//! Three construction entry points:
//!
//! - [`TestNode::spawn`] — fresh transport + fresh temp data dir.
//!   The original "just give me a node" path used by the simple
//!   bootstrap+join tests.
//! - [`TestNode::spawn_with_transport`] — caller provides a
//!   pre-bound `NexarTransport` so it can learn every node's
//!   ephemeral address before any `start_cluster` call. Needed by
//!   the race test, where all five seeds must exist in the config
//!   of every node at startup.
//! - [`TestNode::spawn_with_data_dir`] — caller owns the data
//!   directory as a `PathBuf` (keeping the `TempDir` alive in the
//!   test scope) and the `TestNode` reuses it. Needed by the
//!   idempotent restart test where the same catalog must survive
//!   across two distinct `TestNode` lifetimes.

#![allow(dead_code)] // Not every test file uses every helper.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Duration;

use nodedb_cluster::{
    CacheApplier, ClusterCatalog, ClusterConfig, ClusterLifecycleState, ClusterLifecycleTracker,
    ClusterTopology, MetadataCache, NexarTransport, RaftLoop, start_cluster,
};

/// Build a `NexarTransport` with a tighter-than-production RPC
/// timeout for tests. Production default is 5 s × 3 retries = ~15 s
/// per failed peer contact. 4 s leaves enough headroom for legitimate
/// Raft RPCs under contention while still cutting the join-failure
/// tests (which retry against a dead seed) substantially.
pub fn test_transport(node_id: u64) -> Result<NexarTransport, nodedb_cluster::ClusterError> {
    NexarTransport::with_timeout(
        node_id,
        "127.0.0.1:0".parse().unwrap(),
        Duration::from_secs(4),
    )
}
use nodedb_raft::message::LogEntry;
use tempfile::TempDir;
use tokio::sync::watch;

/// No-op `CommitApplier` — we care about cluster formation, not
/// state machine application.
pub struct NoopApplier;

impl nodedb_cluster::CommitApplier for NoopApplier {
    fn apply_committed(&self, _group_id: u64, entries: &[LogEntry]) -> u64 {
        entries.last().map(|e| e.index).unwrap_or(0)
    }
}

/// Everything one cluster node needs to run in-process for a test.
///
/// The catalog is held exclusively through `raft_loop.catalog`
/// (via `with_catalog`) so that dropping `raft_loop` releases the
/// redb file lock — there is no separate `_catalog` field to
/// extend the lifetime beyond the raft loop.
pub struct TestNode {
    /// Owns the temp dir when we created it ourselves. `None` when
    /// the test owns the directory via its own `TempDir` handle
    /// (see `spawn_with_data_dir`).
    _data_dir: Option<TempDir>,
    /// Always-valid path to the data directory, whether we own the
    /// underlying `TempDir` or the test does.
    pub data_dir_path: PathBuf,
    pub transport: Arc<NexarTransport>,
    pub topology: Arc<RwLock<ClusterTopology>>,
    pub lifecycle: ClusterLifecycleTracker,
    pub node_id: u64,
    /// Live view of the replicated metadata state. Shared with the
    /// `CacheApplier` installed on the `RaftLoop`; tests read from
    /// this to assert that DDL committed on one node has been
    /// applied on every other node.
    pub metadata_cache: Arc<RwLock<MetadataCache>>,
    /// Handle to the `MultiRaft` behind the raft loop, so tests can
    /// propose metadata entries directly without going through a
    /// pgwire client. Matches the production path — pgwire handlers
    /// will call into the same `MultiRaft::propose_to_group`.
    pub multi_raft: Arc<std::sync::Mutex<nodedb_cluster::MultiRaft>>,
    /// Held so tests can call `begin_shutdown` directly on the
    /// raft loop rather than relying on the external watch
    /// channel to propagate. This is the production-shaped path
    /// for graceful shutdown — every detached `tokio::spawn` task
    /// inside `raft_loop::tick::do_tick` subscribes to the same
    /// cooperative-shutdown watch and exits on signal, which is
    /// what lets per-group redb log files release their locks in
    /// time for a subsequent in-process restart.
    raft_loop: Arc<RaftLoop<NoopApplier>>,
    shutdown_tx: watch::Sender<bool>,
    serve_handle: tokio::task::JoinHandle<()>,
    run_handle: tokio::task::JoinHandle<()>,
}

impl TestNode {
    /// Fresh transport + fresh temp data dir. Convenience for tests
    /// that don't care about address or lifetime control.
    pub async fn spawn(
        node_id: u64,
        seed_nodes: Vec<SocketAddr>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let transport = Arc::new(test_transport(node_id)?);
        Self::spawn_with_transport(node_id, transport, seed_nodes).await
    }

    /// Use a pre-bound transport so the caller knows the listen
    /// address before start_cluster runs. Fresh temp data dir.
    pub async fn spawn_with_transport(
        node_id: u64,
        transport: Arc<NexarTransport>,
        seed_nodes: Vec<SocketAddr>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let data_dir = tempfile::tempdir()?;
        let data_dir_path = data_dir.path().to_path_buf();
        Self::spawn_inner(
            node_id,
            transport,
            seed_nodes,
            data_dir_path,
            Some(data_dir),
        )
        .await
    }

    /// Reuse an existing data directory. The caller is responsible
    /// for keeping the directory alive (typically by holding a
    /// `TempDir` in the test's own scope).
    ///
    /// Used to test the `restart()` path: spawn → shutdown → spawn
    /// again on the same data dir, verifying the catalog is
    /// authoritative and no duplicate topology entries or Raft group
    /// disruption occurs.
    pub async fn spawn_with_data_dir(
        node_id: u64,
        data_dir: &Path,
        seed_nodes: Vec<SocketAddr>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let transport = Arc::new(test_transport(node_id)?);
        Self::spawn_inner(node_id, transport, seed_nodes, data_dir.to_path_buf(), None).await
    }

    async fn spawn_inner(
        node_id: u64,
        transport: Arc<NexarTransport>,
        seed_nodes: Vec<SocketAddr>,
        data_dir_path: PathBuf,
        owned_data_dir: Option<TempDir>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let catalog = Arc::new(ClusterCatalog::open(&data_dir_path.join("cluster.redb"))?);
        let listen_addr = transport.local_addr();

        // Empty seeds → imply single-node bootstrap by listing only
        // our own address. Otherwise use whatever the caller supplied.
        let seeds = if seed_nodes.is_empty() {
            vec![listen_addr]
        } else {
            seed_nodes
        };

        let config = ClusterConfig {
            node_id,
            listen_addr,
            seed_nodes: seeds,
            num_groups: 2,
            replication_factor: 3,
            data_dir: data_dir_path.clone(),
            force_bootstrap: false,
            // Fast retry policy: 2 s ceiling keeps the join-failure
            // tests (especially `cluster_join_leader_crash`) under
            // ~5 s of sleeping instead of the production ~64 s.
            join_retry: nodedb_cluster::JoinRetryPolicy {
                max_attempts: 8,
                max_backoff_secs: 2,
            },
        };

        let lifecycle = ClusterLifecycleTracker::new();
        let state = start_cluster(&config, &catalog, &transport, &lifecycle).await?;
        // Match the main binary: the caller is responsible for the
        // final `Ready` transition once the node is wired up. The
        // node count is whatever the node observed at the moment of
        // transition.
        lifecycle.to_ready(state.topology.node_count());

        let topology = Arc::new(RwLock::new(state.topology));
        // Real in-memory metadata cache, driven by a `CacheApplier`
        // installed on the raft loop. Every test can read this
        // directly to assert DDL replication.
        let metadata_cache = Arc::new(RwLock::new(MetadataCache::new()));
        let metadata_applier: Arc<dyn nodedb_cluster::MetadataApplier> =
            Arc::new(CacheApplier::new(metadata_cache.clone()));
        let raft_loop = Arc::new(
            RaftLoop::new(
                state.multi_raft,
                transport.clone(),
                topology.clone(),
                NoopApplier,
            )
            .with_metadata_applier(metadata_applier)
            // Attach the catalog so the server-side `join_flow`
            // can read the real `cluster_id` from it and echo it
            // back on every `JoinResponse`. Without this, joined
            // nodes would fall through to the test-only
            // `self.node_id` fallback in `join_flow`, which is
            // fine for unit tests but produces confusing behavior
            // in end-to-end tests like `cluster_join_idempotent`.
            // The catalog is also what the flow persists to after
            // a successful `AddLearner` commit.
            .with_catalog(catalog.clone()),
        );
        let multi_raft = raft_loop.multi_raft_handle();

        // transport.serve(raft_loop) runs the inbound RPC handler
        // end-to-end — the production code path this whole harness
        // exists to exercise.
        let (shutdown_tx, shutdown_rx_serve) = watch::channel(false);
        let shutdown_rx_run = shutdown_tx.subscribe();

        let transport_for_serve = transport.clone();
        let handler_for_serve = raft_loop.clone();
        let serve_handle = tokio::spawn(async move {
            let _ = transport_for_serve
                .serve(handler_for_serve, shutdown_rx_serve)
                .await;
        });

        let raft_loop_for_run = raft_loop.clone();
        let run_handle = tokio::spawn(async move {
            raft_loop_for_run.run(shutdown_rx_run).await;
        });

        // Drop our own handle on the catalog — `raft_loop.catalog`
        // (set via `with_catalog` above) is now the sole owner
        // inside the TestNode lifetime. When `raft_loop` drops,
        // the catalog's redb file releases its lock.
        drop(catalog);

        Ok(Self {
            _data_dir: owned_data_dir,
            data_dir_path,
            transport,
            topology,
            lifecycle,
            node_id,
            metadata_cache,
            multi_raft,
            raft_loop,
            shutdown_tx,
            serve_handle,
            run_handle,
        })
    }

    /// Propose a `MetadataEntry` directly to the metadata Raft group
    /// on this node. Fails if this node is not the group-0 leader.
    ///
    /// This is the path integration tests use to inject replicated
    /// DDL without bringing up a pgwire client. Production uses the
    /// same underlying `RaftLoop::propose_to_metadata_group`.
    pub fn propose_metadata(
        &self,
        entry: &nodedb_cluster::MetadataEntry,
    ) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        let bytes = nodedb_cluster::encode_entry(entry)?;
        Ok(self.raft_loop.propose_to_metadata_group(bytes)?)
    }

    /// `true` if this node currently believes itself to be the
    /// leader of the metadata raft group (group 0).
    pub fn is_metadata_leader(&self) -> bool {
        for status in self.raft_loop.group_statuses() {
            if status.group_id == nodedb_cluster::METADATA_GROUP_ID {
                return status.leader_id == self.node_id;
            }
        }
        false
    }

    /// Number of committed `CatalogDdl` entries observed by this
    /// node's cache applier. After batch 1e the cluster crate
    /// treats catalog DDL payloads as opaque — this counter is
    /// what tests assert on for replication correctness.
    pub fn catalog_entries_applied(&self) -> u64 {
        self.metadata_cache
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .catalog_entries_applied
    }

    pub fn listen_addr(&self) -> SocketAddr {
        self.transport.local_addr()
    }

    pub fn topology_size(&self) -> usize {
        self.topology
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .node_count()
    }

    pub fn lifecycle_state(&self) -> ClusterLifecycleState {
        self.lifecycle.current()
    }

    pub fn topology_contains(&self, node_id: u64) -> bool {
        self.topology
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .contains(node_id)
    }

    /// Collect every node_id this node knows about, sorted.
    /// Stable ordering lets tests compare cluster membership
    /// between nodes with a single `assert_eq!`.
    pub fn topology_ids(&self) -> Vec<u64> {
        let topo = self.topology.read().unwrap_or_else(|p| p.into_inner());
        let mut ids: Vec<u64> = topo.all_nodes().map(|n| n.node_id).collect();
        ids.sort_unstable();
        ids
    }

    pub async fn shutdown(self) {
        // Flip cooperative shutdown FIRST — every detached task
        // spawned inside `raft_loop::tick::do_tick` subscribes to
        // this watch and exits on the next poll, dropping its
        // `Arc<Mutex<MultiRaft>>` clone. This is what releases
        // the per-group redb log file locks so a subsequent
        // in-process restart can reopen them.
        self.raft_loop.begin_shutdown();
        // Then signal the external shutdown receivers that run()
        // and serve() are waiting on.
        let _ = self.shutdown_tx.send(true);
        // And abort the task handles so they unblock immediately
        // rather than finishing their current poll cycle.
        self.serve_handle.abort();
        self.run_handle.abort();
        let _ = self.serve_handle.await;
        let _ = self.run_handle.await;
        // `JoinHandle::abort() + .await` only guarantees the task
        // has finished — tokio may defer dropping the task's
        // Future (and therefore its captured `Arc<RaftLoop>`
        // clones) to the runtime's next cleanup cycle. Yield
        // repeatedly so that cycle runs before this function
        // returns and `self.raft_loop` drops its last strong
        // reference. Without this the catalog's redb file can
        // stay locked for longer than a subsequent in-process
        // restart is willing to wait.
        for _ in 0..32 {
            tokio::task::yield_now().await;
        }
    }
}

/// Poll a predicate up to `deadline`, sleeping `step` between
/// attempts. Panics with a descriptive message if the predicate
/// never holds.
pub async fn wait_for<F: FnMut() -> bool>(
    desc: &str,
    deadline: Duration,
    step: Duration,
    mut pred: F,
) {
    let start = std::time::Instant::now();
    while start.elapsed() < deadline {
        if pred() {
            return;
        }
        tokio::time::sleep(step).await;
    }
    panic!("timed out after {:?} waiting for: {}", deadline, desc);
}
