//! Parallel peer-cache warm-up.
//!
//! Dials every node in the live topology except `self_id`,
//! caps each dial at half the total deadline, and aggregates
//! the outcome into a [`PeerWarmReport`]. Failures are
//! non-fatal — operators see the per-peer failure in the
//! returned report and the startup phase advances regardless.

use std::collections::HashSet;
use std::time::{Duration, Instant};

use futures::future::join_all;
use nodedb_cluster::{ClusterTopology, NexarTransport};

use super::report::PeerWarmReport;

/// Pre-warm every peer in `topology` except `self_id`.
///
/// `total_deadline` bounds the entire operation. Each
/// individual peer dial is capped at `total_deadline / 2`
/// so a single slow peer cannot eat the whole budget. All
/// dials run in parallel, so the total wall-clock is
/// roughly `max(per-peer-latency, total_deadline)`.
///
/// Empty topology → returns [`PeerWarmReport::empty`].
/// Caller logs the returned report once; this function
/// only logs per-peer failures if verbose tracing is
/// enabled on the transport layer.
pub async fn warm_known_peers(
    transport: &NexarTransport,
    topology: &ClusterTopology,
    self_id: u64,
    total_deadline: Duration,
) -> PeerWarmReport {
    let start = Instant::now();
    let per_peer_deadline = total_deadline.checked_div(2).unwrap_or(total_deadline);

    let peers: Vec<(u64, std::net::SocketAddr)> = topology
        .all_nodes()
        .filter(|n| n.node_id != self_id)
        .filter_map(|n| n.socket_addr().map(|a| (n.node_id, a)))
        .collect();

    if peers.is_empty() {
        return PeerWarmReport::empty();
    }

    let attempted = peers.len();

    // Ensure the transport's address map has every peer.
    // `register_peer` is idempotent — safe to call again.
    for (id, addr) in &peers {
        transport.register_peer(*id, *addr);
    }

    let futs = peers.iter().map(|(id, _addr)| {
        let id = *id;
        async move {
            match tokio::time::timeout(per_peer_deadline, transport.warm_peer(id)).await {
                Ok(Ok(())) => (id, Ok(())),
                Ok(Err(e)) => (id, Err(format!("{e}"))),
                Err(_) => (id, Err(format!("dial timeout after {per_peer_deadline:?}"))),
            }
        }
    });

    // Outer deadline bounds the entire parallel batch. On
    // outer timeout, every still-pending future is dropped
    // (cancelling the in-flight dials) and the missing peers
    // are reported as deadline exceedances.
    let outcome = tokio::time::timeout(total_deadline, join_all(futs)).await;

    let mut succeeded: Vec<u64> = Vec::with_capacity(attempted);
    let mut failed: Vec<(u64, String)> = Vec::new();

    match outcome {
        Ok(results) => {
            for (id, outcome) in results {
                match outcome {
                    Ok(()) => succeeded.push(id),
                    Err(msg) => failed.push((id, msg)),
                }
            }
        }
        Err(_) => {
            // Outer deadline expired — we don't know which
            // futures completed vs which were in flight, so
            // mark every peer as a deadline exceedance. The
            // report is accurate about the worst case.
            for (id, _) in &peers {
                failed.push((
                    *id,
                    format!("outer warm deadline exceeded after {total_deadline:?}"),
                ));
            }
        }
    }

    // Sanity: account every attempted peer. If a future was
    // dropped between matching on the outer result and
    // collecting (shouldn't happen with join_all but cheap
    // to double-check), any missing ids become failures.
    let reported: HashSet<u64> = succeeded
        .iter()
        .copied()
        .chain(failed.iter().map(|(id, _)| *id))
        .collect();
    for (id, _) in &peers {
        if !reported.contains(id) {
            failed.push((*id, "unaccounted peer — internal bookkeeping bug".into()));
        }
    }

    PeerWarmReport {
        attempted,
        succeeded,
        failed,
        elapsed: start.elapsed(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_cluster::ClusterTopology;

    #[tokio::test]
    async fn empty_topology_returns_empty_report() {
        let transport = test_transport();
        let topo = ClusterTopology::new();
        let report = warm_known_peers(&transport, &topo, 1, Duration::from_secs(1)).await;
        assert_eq!(report.attempted, 0);
        assert!(report.is_complete());
        assert_eq!(report.elapsed, Duration::ZERO);
    }

    #[tokio::test]
    async fn self_only_topology_skips_self() {
        let transport = test_transport();
        let mut topo = ClusterTopology::new();
        topo.add_node(nodedb_cluster::NodeInfo::new(
            1,
            "127.0.0.1:65530".parse().unwrap(),
            nodedb_cluster::NodeState::Active,
        ));
        let report = warm_known_peers(&transport, &topo, 1, Duration::from_secs(1)).await;
        assert_eq!(report.attempted, 0);
    }

    #[tokio::test]
    async fn dead_peer_is_reported_as_failure() {
        let transport = test_transport();
        let mut topo = ClusterTopology::new();
        topo.add_node(nodedb_cluster::NodeInfo::new(
            1,
            "127.0.0.1:65530".parse().unwrap(),
            nodedb_cluster::NodeState::Active,
        ));
        // 127.0.0.1:1 — reserved port, guaranteed to fail.
        topo.add_node(nodedb_cluster::NodeInfo::new(
            2,
            "127.0.0.1:1".parse().unwrap(),
            nodedb_cluster::NodeState::Active,
        ));
        let report = warm_known_peers(&transport, &topo, 1, Duration::from_millis(500)).await;
        assert_eq!(report.attempted, 1);
        assert_eq!(report.succeeded.len(), 0);
        assert_eq!(report.failed.len(), 1);
        assert_eq!(report.failed[0].0, 2);
        assert!(report.elapsed <= Duration::from_millis(700));
    }

    fn test_transport() -> NexarTransport {
        NexarTransport::new(
            999,
            "127.0.0.1:0".parse().unwrap(),
            nodedb_cluster::TransportCredentials::Insecure,
        )
        .expect("test transport bind")
    }
}
