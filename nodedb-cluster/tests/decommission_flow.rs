//! End-to-end decommission flow.
//!
//! Wires every piece of the decommission subsystem together without
//! standing up a real metadata Raft group:
//!
//! - `CacheApplier::with_live_state` holds shared topology + routing.
//! - A direct in-memory `MetadataProposer` encodes each proposed
//!   entry, feeds it straight into the applier with a synthetic
//!   monotonically-increasing index, and returns the index — i.e. a
//!   "propose and wait for commit" that is instantaneous.
//! - `DecommissionCoordinator` walks a `plan_full_decommission`
//!   output through that proposer.
//! - `DecommissionObserver` watches the local topology for the
//!   target's state transition and fires its shutdown watch.
//!
//! The real metadata Raft path is already exercised by
//! `metadata_replication.rs`; this test focuses on the decommission
//! state machine end to end: plan → propose → apply → live state
//! → observer signal.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use async_trait::async_trait;

use nodedb_cluster::decommission::{
    DecommissionCoordinator, DecommissionObserver, MetadataProposer, plan_full_decommission,
};
use nodedb_cluster::error::Result;
use nodedb_cluster::metadata_group::{CacheApplier, MetadataApplier, MetadataCache, encode_entry};
use nodedb_cluster::routing::RoutingTable;
use nodedb_cluster::topology::{ClusterTopology, NodeInfo, NodeState};
use nodedb_cluster::{DecommissionRunResult, MetadataEntry};

/// In-memory proposer that encodes every entry and immediately feeds
/// it through an attached `CacheApplier`, returning a synthetic
/// monotonically-increasing index. This is the "one-node metadata
/// group" equivalent the test uses to drive the decommission
/// state machine end to end in a few hundred microseconds.
struct DirectProposer {
    applier: Arc<CacheApplier>,
    next_index: AtomicU64,
    proposed: Mutex<Vec<MetadataEntry>>,
}

impl DirectProposer {
    fn new(applier: Arc<CacheApplier>) -> Arc<Self> {
        Arc::new(Self {
            applier,
            next_index: AtomicU64::new(1),
            proposed: Mutex::new(Vec::new()),
        })
    }
}

#[async_trait]
impl MetadataProposer for DirectProposer {
    async fn propose_and_wait(&self, entry: MetadataEntry) -> Result<u64> {
        let idx = self.next_index.fetch_add(1, Ordering::SeqCst);
        let bytes = encode_entry(&entry).expect("encode metadata entry");
        self.applier.apply(&[(idx, bytes)]);
        self.proposed.lock().unwrap().push(entry);
        Ok(idx)
    }
}

#[tokio::test]
async fn end_to_end_decommission_drains_node_and_signals_shutdown() {
    // --- 3 active nodes, 4 groups, RF=3. Decommission node 3
    //     while RF=2 is the surviving quorum target.
    let mut topo = ClusterTopology::new();
    for (i, id) in [1u64, 2, 3].iter().enumerate() {
        let a: SocketAddr = format!("127.0.0.1:{}", 9000 + i).parse().unwrap();
        topo.add_node(NodeInfo::new(*id, a, NodeState::Active));
    }
    let topology = Arc::new(RwLock::new(topo));
    let mut rt = RoutingTable::uniform(4, &[1, 2, 3], 3);
    // Make node 3 the leader of at least one group so the plan
    // emits a LeadershipTransfer entry and the applier must handle
    // it live.
    rt.set_leader(0, 3);
    rt.set_leader(1, 1);
    rt.set_leader(2, 3);
    rt.set_leader(3, 2);
    let routing = Arc::new(RwLock::new(rt));

    // --- Applier with live topology + routing cascading.
    let cache = Arc::new(RwLock::new(MetadataCache::new()));
    let applier = Arc::new(
        CacheApplier::new(cache.clone()).with_live_state(topology.clone(), routing.clone()),
    );
    let proposer = DirectProposer::new(applier.clone());

    // --- Observer running on node 3 (the target).
    let (observer, mut shutdown_rx) =
        DecommissionObserver::new(topology.clone(), 3, Duration::from_millis(10));

    // --- Build the plan from a snapshot of the live state.
    let plan = {
        let t = topology.read().unwrap();
        let r = routing.read().unwrap();
        plan_full_decommission(3, &t, &r, 2).expect("plan")
    };
    let plan_len = plan.entries.len();

    // --- Drive the coordinator.
    let coordinator = DecommissionCoordinator::new(plan, proposer.clone());
    let result: DecommissionRunResult = coordinator.run().await.expect("coordinator run");
    assert_eq!(result.node_id, 3);
    assert_eq!(result.entries_committed, plan_len);

    // --- Assert live state now reflects the decommission outcome.
    //
    // Topology: node 3 is gone (final `Leave` entry removed it).
    {
        let t = topology.read().unwrap();
        assert!(
            t.get_node(3).is_none(),
            "node 3 should be removed from topology after Leave"
        );
        // Node 1 and 2 still present and unchanged.
        assert_eq!(t.get_node(1).unwrap().state, NodeState::Active);
        assert_eq!(t.get_node(2).unwrap().state, NodeState::Active);
    }

    // Routing: node 3 is no longer in any group's member set, and
    // the groups it used to lead have had their leader hints
    // updated via LeadershipTransfer.
    {
        let r = routing.read().unwrap();
        for (gid, info) in r.group_members() {
            assert!(
                !info.members.contains(&3),
                "group {gid} still contains node 3 after decommission"
            );
            assert!(
                !info.learners.contains(&3),
                "group {gid} still has node 3 as learner after decommission"
            );
        }
        // Group 0 was led by 3 → LeadershipTransfer emitted a new
        // non-3 leader; group 2 likewise.
        assert_ne!(r.group_info(0).unwrap().leader, 3);
        assert_ne!(r.group_info(2).unwrap().leader, 3);
    }

    // --- Observer must now fire its shutdown signal on the very
    //     next check — the topology change already landed.
    assert!(observer.check_once());
    assert!(*shutdown_rx.borrow_and_update());
}
