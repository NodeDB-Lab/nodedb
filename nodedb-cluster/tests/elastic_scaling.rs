//! Elastic add/remove — proves the end-to-end path from membership
//! change to rebalancer dispatch.
//!
//! - **Add-node**: 3 balanced nodes, 4th node joins with zero load →
//!   kick fires → sweep dispatches moves to the new node.
//! - **Remove-node**: covered by `decommission_flow.rs` — the
//!   decommission plan strips the node from all groups, and the
//!   rebalancer loop naturally re-evaluates on its next tick.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use async_trait::async_trait;

use nodedb_cluster::error::Result;
use nodedb_cluster::rebalance::PlannedMove;
use nodedb_cluster::rebalancer::{
    AlwaysReadyGate, ElectionGate, LoadMetrics, LoadMetricsProvider, MigrationDispatcher,
    RebalancerKickHook, RebalancerLoop, RebalancerLoopConfig,
};
use nodedb_cluster::routing::RoutingTable;
use nodedb_cluster::swim::MemberState;
use nodedb_cluster::swim::subscriber::MembershipSubscriber;
use nodedb_cluster::topology::{ClusterTopology, NodeInfo, NodeState};
use nodedb_types::NodeId;

struct DynamicProvider {
    metrics: Mutex<Vec<LoadMetrics>>,
}

impl DynamicProvider {
    fn new(initial: Vec<LoadMetrics>) -> Arc<Self> {
        Arc::new(Self {
            metrics: Mutex::new(initial),
        })
    }
    fn push(&self, m: LoadMetrics) {
        self.metrics.lock().unwrap().push(m);
    }
}

#[async_trait]
impl LoadMetricsProvider for DynamicProvider {
    async fn snapshot(&self) -> Result<Vec<LoadMetrics>> {
        Ok(self.metrics.lock().unwrap().clone())
    }
}

struct RecordingDispatcher {
    calls: Mutex<Vec<PlannedMove>>,
    fired: AtomicBool,
}

impl RecordingDispatcher {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            calls: Mutex::new(Vec::new()),
            fired: AtomicBool::new(false),
        })
    }
}

#[async_trait]
impl MigrationDispatcher for RecordingDispatcher {
    async fn dispatch(&self, mv: PlannedMove) -> Result<()> {
        self.calls.lock().unwrap().push(mv);
        self.fired.store(true, Ordering::SeqCst);
        Ok(())
    }
}

fn lm(id: u64, v: u32, bytes_mib: u64, w: f64, r: f64) -> LoadMetrics {
    LoadMetrics {
        node_id: id,
        vshards_led: v,
        bytes_stored: bytes_mib * 1_048_576,
        writes_per_sec: w,
        reads_per_sec: r,
        qps_recent: 0.0,
        p95_latency_us: 0,
        cpu_utilization: 0.0,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn add_node_triggers_rebalance_via_kick() {
    // --- Initial state: 3 balanced nodes, 6 groups.
    let mut topo = ClusterTopology::new();
    for (i, id) in [1u64, 2, 3].iter().enumerate() {
        let a: std::net::SocketAddr = format!("127.0.0.1:{}", 9000 + i).parse().unwrap();
        topo.add_node(NodeInfo::new(*id, a, NodeState::Active));
    }
    let topology = Arc::new(RwLock::new(topo));
    let mut rt = RoutingTable::uniform(6, &[1, 2, 3], 1);
    // Node 1 leads all 6 groups → hot.
    for gid in 0..6 {
        rt.set_leader(gid, 1);
    }
    let routing = Arc::new(RwLock::new(rt));

    // Metrics: node 1 hot, 2 and 3 moderate.
    let provider = DynamicProvider::new(vec![
        lm(1, 200, 2000, 200.0, 200.0),
        lm(2, 50, 500, 50.0, 50.0),
        lm(3, 50, 500, 50.0, 50.0),
    ]);

    let dispatcher = RecordingDispatcher::new();
    let gate: Arc<dyn ElectionGate> = Arc::new(AlwaysReadyGate);

    // Use a long interval so the normal tick doesn't fire before the
    // kick does — the kick is the signal we're testing.
    let rloop = Arc::new(RebalancerLoop::new(
        RebalancerLoopConfig {
            interval: Duration::from_secs(300),
            ..Default::default()
        },
        provider.clone() as Arc<dyn LoadMetricsProvider>,
        dispatcher.clone() as Arc<dyn MigrationDispatcher>,
        gate,
        routing.clone(),
        topology.clone(),
    ));

    // Wire the kick hook.
    let kick_hook = RebalancerKickHook::new(rloop.kick_handle());

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let handle = tokio::spawn({
        let d = Arc::clone(&rloop);
        async move { d.run(shutdown_rx).await }
    });

    // --- Simulate node 4 joining.
    {
        let mut t = topology.write().unwrap();
        let a: std::net::SocketAddr = "127.0.0.1:9003".parse().unwrap();
        t.add_node(NodeInfo::new(4, a, NodeState::Active));
    }
    // Add node 4's zero-load metrics so the planner sees it as cold.
    provider.push(lm(4, 0, 0, 0.0, 0.0));

    // Fire the SWIM membership hook — this should kick the loop.
    kick_hook.on_state_change(&NodeId::new("node-4"), None, MemberState::Alive);

    // Wait for the dispatcher to fire.
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    while std::time::Instant::now() < deadline {
        if dispatcher.fired.load(Ordering::SeqCst) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        dispatcher.fired.load(Ordering::SeqCst),
        "kick did not trigger a rebalancer dispatch"
    );

    // At least one move should target node 4 (the cold newcomer).
    let calls = dispatcher.calls.lock().unwrap().clone();
    assert!(!calls.is_empty());
    let to_4 = calls.iter().filter(|m| m.target_node == 4).count();
    assert!(
        to_4 > 0,
        "expected at least one move targeting node 4, got {to_4}"
    );

    let _ = shutdown_tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(1), handle).await;
}
