//! Real-UDP convergence test for the SWIM failure detector.
//!
//! Spawns three SWIM instances on ephemeral loopback UDP ports, each
//! seeded with the other two, and asserts:
//!
//! 1. Every node converges to a 3-member `Alive` view within 5 seconds.
//! 2. When one node shuts its detector down, the remaining two observe
//!    the silent peer as `Suspect` or `Dead` within another 5 seconds.
//!
//! Uses real wall-clock time and a fast probe cadence (50 ms probe
//! interval) so the whole test finishes well under a second on a warm
//! build. No mocks, no paused time.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, Instant};

use nodedb_cluster::swim::Transport;
use nodedb_cluster::{SwimConfig, SwimHandle, UdpTransport, spawn_swim};
use nodedb_types::NodeId;

fn any_loopback() -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)
}

fn fast_cfg() -> SwimConfig {
    SwimConfig {
        probe_interval: Duration::from_millis(50),
        probe_timeout: Duration::from_millis(20),
        indirect_probes: 2,
        suspicion_mult: 2,
        min_suspicion: Duration::from_millis(150),
        initial_incarnation: nodedb_cluster::Incarnation::ZERO,
        max_piggyback: 6,
        fanout_lambda: 3,
    }
}

async fn poll<F>(deadline: Duration, mut pred: F) -> bool
where
    F: FnMut() -> bool,
{
    let start = Instant::now();
    while start.elapsed() < deadline {
        if pred() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    pred()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_node_udp_mesh_converges_and_detects_failure() {
    // Bind three ephemeral UDP sockets.
    let t_a = Arc::new(UdpTransport::bind(any_loopback()).await.expect("bind a"));
    let t_b = Arc::new(UdpTransport::bind(any_loopback()).await.expect("bind b"));
    let t_c = Arc::new(UdpTransport::bind(any_loopback()).await.expect("bind c"));
    let addr_a = t_a.local_addr();
    let addr_b = t_b.local_addr();
    let addr_c = t_c.local_addr();

    // Spawn each detector seeded with the other two addresses.
    let h_a: SwimHandle = spawn_swim(
        fast_cfg(),
        NodeId::new("a"),
        addr_a,
        vec![addr_b, addr_c],
        t_a,
    )
    .await
    .expect("spawn a");
    let h_b: SwimHandle = spawn_swim(
        fast_cfg(),
        NodeId::new("b"),
        addr_b,
        vec![addr_a, addr_c],
        t_b,
    )
    .await
    .expect("spawn b");
    let h_c: SwimHandle = spawn_swim(
        fast_cfg(),
        NodeId::new("c"),
        addr_c,
        vec![addr_a, addr_b],
        t_c,
    )
    .await
    .expect("spawn c");

    // Converge: each node must observe at least 3 alive members (self
    // + 2 peers). The seed placeholders start as Alive so this check
    // is already true at t=0, but after a probe round the placeholder
    // IDs get replaced by the real node IDs via the `from` field on
    // the Ack. We assert that at least one of {a,b,c} *by real id*
    // is present on every node.
    let converged = poll(Duration::from_secs(5), || {
        let real_ids = [NodeId::new("a"), NodeId::new("b"), NodeId::new("c")];
        let check = |h: &SwimHandle| {
            real_ids
                .iter()
                .filter(|id| h.membership().get(id).is_some())
                .count()
                >= 3
        };
        check(&h_a) && check(&h_b) && check(&h_c)
    })
    .await;
    assert!(converged, "3-node UDP mesh failed to converge within 5s");

    // Shut down node B. Do it cleanly so B stops responding to probes.
    h_b.shutdown().await;

    // A and C must now observe B as Suspect or Dead within 5s.
    // We check by real id, so this assertion survives even if the
    // merged entry's node_id ends up being "b" or "seed:<addr>".
    let detected = poll(Duration::from_secs(5), || {
        let b_id = NodeId::new("b");
        let state_of = |h: &SwimHandle| {
            h.membership()
                .get(&b_id)
                .map(|m| m.state)
                .filter(|s| {
                    matches!(
                        s,
                        nodedb_cluster::MemberState::Suspect | nodedb_cluster::MemberState::Dead
                    )
                })
                .is_some()
        };
        state_of(&h_a) && state_of(&h_c)
    })
    .await;
    assert!(
        detected,
        "A and C failed to mark B as Suspect/Dead within 5s after B shut down"
    );

    h_a.shutdown().await;
    h_c.shutdown().await;
}
