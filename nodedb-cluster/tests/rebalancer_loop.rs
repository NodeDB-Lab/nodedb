//! End-to-end rebalancer driver loop.
//!
//! Wires every piece of the rebalancer together without standing up
//! the real `MigrationExecutor`:
//!
//! - A shared `Arc<RwLock<RoutingTable>>` + `Arc<RwLock<ClusterTopology>>`.
//! - A `StaticProvider` returning a canned set of `LoadMetrics` so
//!   node 1 is massively hotter than nodes 2 and 3.
//! - A `DirectDispatcher` that simulates instantaneous migration
//!   completion by reassigning the vshard's group leader in the
//!   live routing table and recording the call for assertions.
//! - An `AlwaysReadyGate` — no election gating in this synthetic
//!   scenario.
//!
//! The test spawns the loop, advances through one sweep, asserts
//! the dispatcher observed moves exclusively from node 1 as source,
//! and asserts the routing table was actually mutated — proving the
//! full plan → dispatch → apply chain.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::watch;

use nodedb_cluster::error::Result;
use nodedb_cluster::rebalance::PlannedMove;
use nodedb_cluster::rebalancer::{
    AlwaysReadyGate, ElectionGate, LoadMetrics, LoadMetricsProvider, MigrationDispatcher,
    RebalancerLoop, RebalancerLoopConfig,
};
use nodedb_cluster::routing::RoutingTable;
use nodedb_cluster::topology::{ClusterTopology, NodeInfo, NodeState};

struct StaticProvider(Vec<LoadMetrics>);

#[async_trait]
impl LoadMetricsProvider for StaticProvider {
    async fn snapshot(&self) -> Result<Vec<LoadMetrics>> {
        Ok(self.0.clone())
    }
}

struct DirectDispatcher {
    routing: Arc<RwLock<RoutingTable>>,
    calls: Mutex<Vec<PlannedMove>>,
    fired: AtomicBool,
}

impl DirectDispatcher {
    fn new(routing: Arc<RwLock<RoutingTable>>) -> Arc<Self> {
        Arc::new(Self {
            routing,
            calls: Mutex::new(Vec::new()),
            fired: AtomicBool::new(false),
        })
    }
    fn calls(&self) -> Vec<PlannedMove> {
        self.calls.lock().unwrap().clone()
    }
    fn fired(&self) -> bool {
        self.fired.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl MigrationDispatcher for DirectDispatcher {
    async fn dispatch(&self, mv: PlannedMove) -> Result<()> {
        // Simulate a completed migration by flipping the group
        // leader to the target node.
        {
            let mut rt = self.routing.write().unwrap_or_else(|p| p.into_inner());
            rt.set_leader(mv.source_group, mv.target_node);
        }
        self.calls.lock().unwrap().push(mv);
        self.fired.store(true, Ordering::SeqCst);
        Ok(())
    }
}

fn topo(nodes: &[u64]) -> Arc<RwLock<ClusterTopology>> {
    let mut t = ClusterTopology::new();
    for (i, id) in nodes.iter().enumerate() {
        let a: std::net::SocketAddr = format!("127.0.0.1:{}", 9000 + i).parse().unwrap();
        t.add_node(NodeInfo::new(*id, a, NodeState::Active));
    }
    Arc::new(RwLock::new(t))
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
async fn rebalancer_loop_dispatches_and_mutates_routing() {
    // --- 3 active nodes, 6 groups, node 1 leads all of them (hot).
    let topology = topo(&[1, 2, 3]);
    let mut r = RoutingTable::uniform(6, &[1, 2, 3], 1);
    for gid in 0..6 {
        r.set_leader(gid, 1);
    }
    let routing = Arc::new(RwLock::new(r));

    // --- Hot node 1, cold 2 and 3.
    let metrics: Arc<dyn LoadMetricsProvider> = Arc::new(StaticProvider(vec![
        lm(1, 500, 5000, 200.0, 200.0),
        lm(2, 5, 5, 5.0, 5.0),
        lm(3, 5, 5, 5.0, 5.0),
    ]));

    let dispatcher = DirectDispatcher::new(routing.clone());
    let gate: Arc<dyn ElectionGate> = Arc::new(AlwaysReadyGate);

    let rloop = Arc::new(RebalancerLoop::new(
        RebalancerLoopConfig {
            interval: Duration::from_millis(50),
            ..Default::default()
        },
        metrics,
        dispatcher.clone() as Arc<dyn MigrationDispatcher>,
        gate,
        routing.clone(),
        topology.clone(),
    ));

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let handle = tokio::spawn({
        let d = Arc::clone(&rloop);
        async move { d.run(shutdown_rx).await }
    });

    // Wall-clock wait — the loop uses real time, so just give it a
    // couple of intervals to sweep + spawn + dispatch.
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    while std::time::Instant::now() < deadline {
        if dispatcher.fired() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        dispatcher.fired(),
        "rebalancer loop never dispatched a move"
    );

    // Every move must have node 1 as source.
    let calls = dispatcher.calls();
    assert!(!calls.is_empty());
    for c in &calls {
        assert_eq!(c.source_node, 1, "source must be the hot node");
        assert_ne!(c.target_node, 1, "target must differ from source");
    }

    // Routing mutation: at least one group previously led by 1 now
    // has a non-1 leader.
    {
        let rt = routing.read().unwrap();
        let still_on_1 = (0..6)
            .filter(|gid| rt.group_info(*gid).unwrap().leader == 1)
            .count();
        assert!(
            still_on_1 < 6,
            "at least one group should have moved off node 1"
        );
    }

    let _ = shutdown_tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(1), handle).await;
}
