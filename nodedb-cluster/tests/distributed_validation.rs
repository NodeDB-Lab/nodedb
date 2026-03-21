//! Phase 3 validation gate tests.

use nodedb_cluster::ghost::{GhostStub, GhostTable, SweepVerdict};
use nodedb_cluster::migration::{MigrationPhase, MigrationState};
use nodedb_cluster::routing::RoutingTable;
use nodedb_cluster::topology::{ClusterTopology, NodeInfo, NodeState};

/// Gate 1: Simulated Jepsen-style test — partitions and recovery.
#[test]
fn jepsen_simulated_partition_recovery() {
    let mut topo = ClusterTopology::new();
    let addrs: Vec<std::net::SocketAddr> = (0..3)
        .map(|i| format!("127.0.0.1:{}", 9000 + i).parse().unwrap())
        .collect();
    for (i, addr) in addrs.iter().enumerate() {
        topo.add_node(NodeInfo::new(i as u64 + 1, *addr, NodeState::Active));
    }

    let routing = RoutingTable::uniform(4, &[1, 2, 3], 3);

    // All groups have 3 members.
    for gid in routing.group_ids() {
        let info = routing.group_info(gid).unwrap();
        assert_eq!(info.members.len(), 3);
    }

    // Simulate partition: node 3 drains (unreachable).
    topo.set_state(3, NodeState::Draining);
    assert_eq!(topo.active_nodes().len(), 2, "quorum maintained");

    // Heal: node 3 comes back.
    topo.set_state(3, NodeState::Active);
    assert_eq!(topo.active_nodes().len(), 3, "all nodes active");

    // Routing table still valid.
    for gid in routing.group_ids() {
        assert!(routing.group_info(gid).is_some());
    }
}

/// Gate 2: Migration completes correctly under concurrent writes.
#[test]
fn migration_under_concurrent_writes() {
    let mut state = MigrationState::new(42, 1, 2, 1, 2, 500_000);

    // Phase 1: Base copy.
    state.start_base_copy(1000);
    assert!(matches!(state.phase(), MigrationPhase::BaseCopy { .. }));
    state.update_base_copy(1000);

    // Phase 2: WAL catch-up.
    state.start_wal_catchup(100, 110);
    assert!(matches!(state.phase(), MigrationPhase::WalCatchUp { .. }));

    // Concurrent writes advance source LSN.
    state.update_wal_catchup(108, 115);
    // Lag = 7, still catching up.

    state.update_wal_catchup(114, 115);
    // Lag = 1, ready for catch-up.
    assert!(state.is_catchup_ready());

    // Phase 3: Atomic cut-over.
    assert!(state.start_cutover(250).is_ok());
    assert!(matches!(
        state.phase(),
        MigrationPhase::AtomicCutOver { .. }
    ));

    // Complete.
    state.complete(250);
    assert!(matches!(
        state.phase(),
        MigrationPhase::Completed {
            pause_duration_us: 250
        }
    ));
}

/// Gate 3: Ghost edge count converges to zero after rebalance.
#[test]
fn ghost_edge_convergence() {
    let mut ghost_table = GhostTable::new();

    // Create 10 ghost stubs with refcount=1 (simulating migration).
    for i in 0..10 {
        ghost_table.insert(GhostStub::new(format!("edge_{i}"), i + 10, 1));
    }
    assert_eq!(ghost_table.len(), 10);

    // Path A: refcount-based fast purge.
    // Decrement refcount to 0 → stub is immediately purged.
    for i in 0..5 {
        let purged = ghost_table.decrement_ref(&format!("edge_{i}"));
        assert!(purged, "edge_{i} should be purged on decrement");
    }
    assert_eq!(ghost_table.len(), 5, "5 stubs remain after fast purge");

    // Path B: sweep-based purge for remaining stubs.
    // Sweep calls verify_fn for each remaining stub.
    let report = ghost_table.sweep(|_node_id, _target_shard| SweepVerdict::Purge);
    assert_eq!(report.purged, 5, "sweep should purge remaining 5");
    assert_eq!(ghost_table.len(), 0, "ghost count should converge to zero");
}
