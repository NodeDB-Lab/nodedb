//! Integration test: 3 nodes, one bootstraps, two join over QUIC.
//!
//! Drives the production server-side code path: joining nodes send
//! `RaftRpc::JoinRequest` to the bootstrap leader, whose `RaftLoop` is
//! serving the transport. Asserts that all three nodes converge on a
//! 3-member topology within 10 seconds.

use std::sync::Arc;
use std::sync::RwLock;
use std::time::Duration;

use nodedb_cluster::{
    ClusterCatalog, ClusterConfig, ClusterTopology, NexarTransport, RaftLoop, start_cluster,
};
use nodedb_raft::message::LogEntry;
use tempfile::TempDir;
use tokio::sync::watch;

/// No-op `CommitApplier` — we care about the join path, not state machine application.
struct NoopApplier;

impl nodedb_cluster::CommitApplier for NoopApplier {
    fn apply_committed(&self, _group_id: u64, entries: &[LogEntry]) -> u64 {
        entries.last().map(|e| e.index).unwrap_or(0)
    }
}

/// Everything one cluster node needs to run in-process for a test.
struct TestNode {
    #[allow(dead_code)] // Held so the TempDir is dropped at end of scope.
    data_dir: TempDir,
    #[allow(dead_code)] // Held so the catalog file stays open for the lifetime of the node.
    catalog: Arc<ClusterCatalog>,
    transport: Arc<NexarTransport>,
    topology: Arc<RwLock<ClusterTopology>>,
    node_id: u64,
    shutdown_tx: watch::Sender<bool>,
    serve_handle: tokio::task::JoinHandle<()>,
    run_handle: tokio::task::JoinHandle<()>,
}

impl TestNode {
    /// Spawn a full RaftLoop + transport serve loop on loopback.
    ///
    /// `seed_nodes` determines the path:
    /// - if `listen_addr` is in `seed_nodes`, this node bootstraps (or joins
    ///   via another seed if one is already up — the production behavior)
    /// - otherwise it calls `join()` against the given seeds
    async fn spawn(
        node_id: u64,
        seed_nodes: Vec<std::net::SocketAddr>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let data_dir = tempfile::tempdir()?;
        let catalog = Arc::new(ClusterCatalog::open(&data_dir.path().join("cluster.redb"))?);
        let transport = Arc::new(NexarTransport::new(
            node_id,
            "127.0.0.1:0".parse().unwrap(),
        )?);
        let listen_addr = transport.local_addr();

        // If the caller passed no seeds, assume this is the bootstrap node and
        // list only our own address (so is_seed == true and should_bootstrap
        // has nothing to probe).
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
            data_dir: data_dir.path().to_path_buf(),
            force_bootstrap: false,
        };

        let state = start_cluster(&config, &catalog, &transport).await?;

        let topology = Arc::new(RwLock::new(state.topology));
        let raft_loop = Arc::new(RaftLoop::new(
            state.multi_raft,
            transport.clone(),
            topology.clone(),
            NoopApplier,
        ));

        // Start the transport serve loop (this is the production path —
        // `transport.serve(raft_loop)` routes incoming RaftRpcs through
        // `RaftLoop::handle_rpc`, which is exactly what we want to test).
        let (shutdown_tx, shutdown_rx_serve) = watch::channel(false);
        let shutdown_rx_run = shutdown_tx.subscribe();

        let transport_for_serve = transport.clone();
        let handler_for_serve = raft_loop.clone();
        let serve_handle = tokio::spawn(async move {
            let _ = transport_for_serve
                .serve(handler_for_serve, shutdown_rx_serve)
                .await;
        });

        // Start the Raft tick loop (drives elections and heartbeats).
        let run_handle = tokio::spawn(async move {
            raft_loop.run(shutdown_rx_run).await;
        });

        Ok(Self {
            data_dir,
            catalog,
            transport,
            topology,
            node_id,
            shutdown_tx,
            serve_handle,
            run_handle,
        })
    }

    fn listen_addr(&self) -> std::net::SocketAddr {
        self.transport.local_addr()
    }

    fn topology_size(&self) -> usize {
        self.topology
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .node_count()
    }

    async fn shutdown(self) {
        let _ = self.shutdown_tx.send(true);
        let _ = self.serve_handle.await;
        let _ = self.run_handle.await;
    }
}

/// Poll a predicate up to `deadline`, sleeping `step` between attempts.
/// Panics with a descriptive message if the predicate never holds.
async fn wait_for<F: FnMut() -> bool>(desc: &str, deadline: Duration, step: Duration, mut pred: F) {
    let start = std::time::Instant::now();
    while start.elapsed() < deadline {
        if pred() {
            return;
        }
        tokio::time::sleep(step).await;
    }
    panic!("timed out after {:?} waiting for: {}", deadline, desc);
}

// ── Tests ─────────────────────────────────────────────────────────────

/// Spawns 3 in-process cluster nodes on loopback. Node 1 bootstraps. Nodes
/// 2 and 3 join via node 1 using the production `RaftLoop` RPC handler.
/// Within 10 seconds every node's topology must contain all 3 members.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_node_join_over_quic() {
    // Node 1 bootstraps the cluster — empty seeds → `spawn` fills in own addr.
    let node1 = TestNode::spawn(1, vec![]).await.expect("node 1 bootstrap");

    // Give node 1's serve loop a moment to start accepting connections.
    tokio::time::sleep(Duration::from_millis(200)).await;

    let seeds = vec![node1.listen_addr()];

    // Node 2 joins via node 1's address.
    let node2 = TestNode::spawn(2, seeds.clone())
        .await
        .expect("node 2 join");

    // Node 3 joins via node 1.
    let node3 = TestNode::spawn(3, seeds).await.expect("node 3 join");

    // All three must converge on a 3-member topology within 10 seconds.
    // Slight jitter is expected while conf-changes commit across Raft groups.
    let nodes = [&node1, &node2, &node3];
    wait_for(
        "all 3 nodes report topology_size == 3",
        Duration::from_secs(10),
        Duration::from_millis(100),
        || nodes.iter().all(|n| n.topology_size() == 3),
    )
    .await;

    // Sanity: each node knows its own id.
    assert_eq!(node1.node_id, 1);
    assert_eq!(node2.node_id, 2);
    assert_eq!(node3.node_id, 3);

    // Clean shutdown so the transports release their sockets before the next
    // test in the same binary picks up.
    node3.shutdown().await;
    node2.shutdown().await;
    node1.shutdown().await;
}

/// Minimal smoke variant — a single node still bootstraps cleanly.
///
/// Guards against the "fix the join path and accidentally break the
/// single-node path" regression.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_node_bootstrap_still_works() {
    let node = TestNode::spawn(1, vec![])
        .await
        .expect("single-node bootstrap");
    assert_eq!(node.topology_size(), 1);
    node.shutdown().await;
}
