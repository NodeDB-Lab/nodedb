//! Liveness drives routing invalidation.
//!
//! Three UDP-backed SWIM nodes form a full mesh. A shared
//! `RoutingTable` declares node B as the leader of group 0. A
//! `RoutingLivenessHook` subscribed to node A's detector is wired to
//! that routing table. When B is shut down, A's detector must observe
//! the Suspect→Dead transition and the hook must clear the leader
//! hint for group 0 within a few suspicion timeouts.

use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use nodedb_cluster::routing::RoutingTable;
use nodedb_cluster::routing_liveness::{NodeIdResolver, RoutingLivenessHook};
use nodedb_cluster::swim::Transport;
use nodedb_cluster::swim::bootstrap::spawn_with_subscribers;
use nodedb_cluster::{
    Incarnation, MembershipSubscriber, SwimConfig, SwimHandle, UdpTransport, spawn_swim,
};
use nodedb_types::NodeId;

fn fast_cfg() -> SwimConfig {
    SwimConfig {
        probe_interval: Duration::from_millis(50),
        probe_timeout: Duration::from_millis(20),
        indirect_probes: 2,
        suspicion_mult: 3,
        min_suspicion: Duration::from_millis(150),
        initial_incarnation: Incarnation::ZERO,
        max_piggyback: 6,
        fanout_lambda: 3,
    }
}

fn resolver_static() -> NodeIdResolver {
    Arc::new(|nid: &NodeId| match nid.as_str() {
        "a" => Some(1),
        "b" => Some(2),
        "c" => Some(3),
        _ => None,
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn swim_dead_leader_clears_routing_hint() {
    // --- Build three real UDP transports on ephemeral ports. ---
    let t_a = Arc::new(
        UdpTransport::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap(),
    );
    let t_b = Arc::new(
        UdpTransport::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap(),
    );
    let t_c = Arc::new(
        UdpTransport::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap(),
    );
    let addr_a = t_a.local_addr();
    let addr_b = t_b.local_addr();
    let addr_c = t_c.local_addr();

    // --- Shared routing table: 4 groups, leader = node b (id=2) for groups 0 and 2. ---
    let rt = Arc::new(RwLock::new(RoutingTable::uniform(4, &[1, 2, 3], 3)));
    {
        let mut guard = rt.write().unwrap();
        guard.set_leader(0, 2);
        guard.set_leader(1, 1);
        guard.set_leader(2, 2);
        guard.set_leader(3, 3);
    }

    // --- Hook node A to the routing table. ---
    let hook: Arc<dyn MembershipSubscriber> =
        Arc::new(RoutingLivenessHook::new(rt.clone(), resolver_static()));

    let h_a: SwimHandle = spawn_with_subscribers(
        fast_cfg(),
        NodeId::new("a"),
        addr_a,
        vec![addr_b, addr_c],
        t_a.clone() as Arc<dyn Transport>,
        vec![hook],
    )
    .await
    .expect("spawn a");
    let h_b: SwimHandle = spawn_swim(
        fast_cfg(),
        NodeId::new("b"),
        addr_b,
        vec![addr_a, addr_c],
        t_b.clone() as Arc<dyn Transport>,
    )
    .await
    .expect("spawn b");
    let h_c: SwimHandle = spawn_swim(
        fast_cfg(),
        NodeId::new("c"),
        addr_c,
        vec![addr_a, addr_b],
        t_c.clone() as Arc<dyn Transport>,
    )
    .await
    .expect("spawn c");

    // --- Wait for A to learn about B (real id, not placeholder). ---
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let seen = h_a.membership().get(&NodeId::new("b")).is_some();
        if seen || Instant::now() >= deadline {
            assert!(seen, "A never learned B's real NodeId");
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // --- Sanity: group 0 still led by node 2. ---
    {
        let guard = rt.read().unwrap();
        assert_eq!(guard.group_info(0).unwrap().leader, 2);
    }

    // --- Shut B down and wait for A to invalidate the leader hint. ---
    h_b.shutdown().await;

    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let cleared = {
            let guard = rt.read().unwrap();
            guard.group_info(0).unwrap().leader == 0 && guard.group_info(2).unwrap().leader == 0
        };
        if cleared {
            break;
        }
        if Instant::now() >= deadline {
            panic!("routing hook never cleared leader hints for groups led by B");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Groups led by A must be untouched — A is still alive and probing.
    // We do NOT assert on groups led by C: under real UDP races the
    // detector may transiently flag C as Suspect while B is being
    // demoted, which is the correct behaviour of the hook.
    {
        let guard = rt.read().unwrap();
        assert_eq!(
            guard.group_info(1).unwrap().leader,
            1,
            "group led by local node A must not be invalidated"
        );
    }

    h_a.shutdown().await;
    h_c.shutdown().await;
}
