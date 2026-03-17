//! Cluster health monitoring — periodic pings, failure detection, topology broadcast.
//!
//! The [`HealthMonitor`] runs as a background task alongside the Raft loop:
//! - Periodically pings all known peers to detect failures
//! - Updates topology when peers fail or recover
//! - Broadcasts topology changes to all active peers
//! - Persists topology updates to the cluster catalog

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use tracing::{debug, info, warn};

use crate::catalog::ClusterCatalog;
use crate::rpc_codec::{
    JoinNodeInfo, PingRequest, PongResponse, RaftRpc, TopologyAck, TopologyUpdate,
};
use crate::topology::{ClusterTopology, NodeState};
use crate::transport::NexarTransport;

/// Default ping interval.
const DEFAULT_PING_INTERVAL: Duration = Duration::from_secs(5);

/// Default number of consecutive failures before marking a node as down.
const DEFAULT_FAILURE_THRESHOLD: u32 = 3;

/// Health monitor configuration.
#[derive(Debug, Clone)]
pub struct HealthConfig {
    pub ping_interval: Duration,
    pub failure_threshold: u32,
}

impl Default for HealthConfig {
    fn default() -> Self {
        Self {
            ping_interval: DEFAULT_PING_INTERVAL,
            failure_threshold: DEFAULT_FAILURE_THRESHOLD,
        }
    }
}

/// Cluster health monitor.
///
/// Runs as a background task. Pings all known peers, detects failures,
/// updates topology, and broadcasts changes.
pub struct HealthMonitor {
    node_id: u64,
    transport: Arc<NexarTransport>,
    topology: Arc<RwLock<ClusterTopology>>,
    catalog: Arc<ClusterCatalog>,
    config: HealthConfig,
    /// Per-peer consecutive ping failure count.
    ping_failures: Mutex<HashMap<u64, u32>>,
}

impl HealthMonitor {
    pub fn new(
        node_id: u64,
        transport: Arc<NexarTransport>,
        topology: Arc<RwLock<ClusterTopology>>,
        catalog: Arc<ClusterCatalog>,
        config: HealthConfig,
    ) -> Self {
        Self {
            node_id,
            transport,
            topology,
            catalog,
            config,
            ping_failures: Mutex::new(HashMap::new()),
        }
    }

    /// Run the health monitor until shutdown.
    pub async fn run(&self, mut shutdown: tokio::sync::watch::Receiver<bool>) {
        let mut interval = tokio::time::interval(self.config.ping_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        info!(node_id = self.node_id, "health monitor started");

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    self.ping_all_peers().await;
                }
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        debug!("health monitor shutting down");
                        break;
                    }
                }
            }
        }
    }

    /// Ping all known peers and update failure tracking.
    async fn ping_all_peers(&self) {
        let peers = self.collect_peers();
        if peers.is_empty() {
            return;
        }

        let topo_version = {
            let topo = self.topology.read().unwrap_or_else(|p| p.into_inner());
            topo.version()
        };

        let mut handles = Vec::new();
        for (peer_id, addr) in peers {
            let transport = self.transport.clone();
            let ping = RaftRpc::Ping(PingRequest {
                sender_id: self.node_id,
                topology_version: topo_version,
            });
            handles.push(tokio::spawn(async move {
                let result = transport.send_rpc(peer_id, ping).await;
                (peer_id, addr, result)
            }));
        }

        let mut topology_changed = false;
        for handle in handles {
            let (peer_id, _addr, result) = match handle.await {
                Ok(r) => r,
                Err(_) => continue, // JoinError — task panicked, skip.
            };

            match result {
                Ok(RaftRpc::Pong(pong)) => {
                    topology_changed |= self.handle_pong(peer_id, &pong);
                }
                Ok(_) => {
                    // Unexpected response type — count as failure.
                    topology_changed |= self.record_ping_failure(peer_id);
                }
                Err(_) => {
                    topology_changed |= self.record_ping_failure(peer_id);
                }
            }
        }

        if topology_changed {
            self.persist_and_broadcast().await;
        }
    }

    /// Handle a successful pong — reset failure count, mark node Active if needed.
    fn handle_pong(&self, peer_id: u64, _pong: &PongResponse) -> bool {
        // Reset failure count.
        {
            let mut failures = self.ping_failures.lock().unwrap_or_else(|p| p.into_inner());
            failures.remove(&peer_id);
        }

        // If node was not Active, mark it Active.
        let mut topo = self.topology.write().unwrap_or_else(|p| p.into_inner());
        if let Some(node) = topo.get_node(peer_id) {
            if node.state != NodeState::Active && node.state != NodeState::Decommissioned {
                info!(peer_id, "peer recovered, marking active");
                topo.set_state(peer_id, NodeState::Active);
                return true;
            }
        }
        false
    }

    /// Record a ping failure. Returns true if topology changed (node marked Draining).
    fn record_ping_failure(&self, peer_id: u64) -> bool {
        let count = {
            let mut failures = self.ping_failures.lock().unwrap_or_else(|p| p.into_inner());
            let count = failures.entry(peer_id).or_insert(0);
            *count += 1;
            *count
        };

        if count >= self.config.failure_threshold {
            let mut topo = self.topology.write().unwrap_or_else(|p| p.into_inner());
            if let Some(node) = topo.get_node(peer_id) {
                if node.state == NodeState::Active {
                    warn!(
                        peer_id,
                        failures = count,
                        "peer unreachable, marking draining"
                    );
                    topo.set_state(peer_id, NodeState::Draining);
                    return true;
                }
            }
        }
        false
    }

    /// Persist updated topology and broadcast to all active peers.
    async fn persist_and_broadcast(&self) {
        // Persist.
        let topo = self.topology.read().unwrap_or_else(|p| p.into_inner());
        if let Err(e) = self.catalog.save_topology(&topo) {
            warn!(error = %e, "failed to persist topology update");
        }

        // Build TopologyUpdate message.
        let update = RaftRpc::TopologyUpdate(TopologyUpdate {
            version: topo.version(),
            nodes: topo
                .all_nodes()
                .map(|n| JoinNodeInfo {
                    node_id: n.node_id,
                    addr: n.addr.clone(),
                    state: n.state.as_u8(),
                    raft_groups: n.raft_groups.clone(),
                })
                .collect(),
        });

        // Collect active peers to broadcast to.
        let active_peers: Vec<u64> = topo
            .active_nodes()
            .iter()
            .map(|n| n.node_id)
            .filter(|&id| id != self.node_id)
            .collect();
        drop(topo);

        // Broadcast (fire-and-forget).
        for peer_id in active_peers {
            let transport = self.transport.clone();
            let msg = update.clone();
            tokio::spawn(async move {
                if let Err(e) = transport.send_rpc(peer_id, msg).await {
                    debug!(peer_id, error = %e, "topology broadcast failed");
                }
            });
        }
    }

    /// Collect all non-self, non-decommissioned peers with their addresses.
    fn collect_peers(&self) -> Vec<(u64, SocketAddr)> {
        let topo = self.topology.read().unwrap_or_else(|p| p.into_inner());
        topo.all_nodes()
            .filter(|n| n.node_id != self.node_id && n.state != NodeState::Decommissioned)
            .filter_map(|n| n.socket_addr().map(|addr| (n.node_id, addr)))
            .collect()
    }
}

/// Handle an incoming Ping RPC — return a Pong with our topology version.
pub fn handle_ping(node_id: u64, topology_version: u64, _req: &PingRequest) -> RaftRpc {
    RaftRpc::Pong(PongResponse {
        responder_id: node_id,
        topology_version,
    })
}

/// Handle an incoming TopologyUpdate — adopt if newer version.
///
/// Returns true if topology was updated.
pub fn handle_topology_update(
    node_id: u64,
    topology: &RwLock<ClusterTopology>,
    update: &TopologyUpdate,
) -> (bool, RaftRpc) {
    let mut topo = topology.write().unwrap_or_else(|p| p.into_inner());

    let updated = if update.version > topo.version() {
        // Adopt the newer topology.
        let mut new_topo = ClusterTopology::new();
        for node in &update.nodes {
            let state = crate::topology::NodeState::from_u8(node.state)
                .unwrap_or(crate::topology::NodeState::Active);
            let info = crate::topology::NodeInfo {
                node_id: node.node_id,
                addr: node.addr.clone(),
                state,
                raft_groups: node.raft_groups.clone(),
            };
            new_topo.add_node(info);
        }
        *topo = new_topo;
        true
    } else {
        false
    };

    let ack = RaftRpc::TopologyAck(TopologyAck {
        responder_id: node_id,
        accepted_version: topo.version(),
    });

    (updated, ack)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topology::NodeInfo;

    #[test]
    fn handle_ping_returns_pong() {
        let req = PingRequest {
            sender_id: 2,
            topology_version: 5,
        };
        let resp = handle_ping(1, 7, &req);
        match resp {
            RaftRpc::Pong(pong) => {
                assert_eq!(pong.responder_id, 1);
                assert_eq!(pong.topology_version, 7);
            }
            other => panic!("expected Pong, got {other:?}"),
        }
    }

    #[test]
    fn topology_update_adopts_newer_version() {
        let topo = RwLock::new(ClusterTopology::new()); // version 0

        let update = TopologyUpdate {
            version: 3,
            nodes: vec![
                JoinNodeInfo {
                    node_id: 1,
                    addr: "10.0.0.1:9400".into(),
                    state: 1,
                    raft_groups: vec![],
                },
                JoinNodeInfo {
                    node_id: 2,
                    addr: "10.0.0.2:9400".into(),
                    state: 1,
                    raft_groups: vec![],
                },
            ],
        };

        let (updated, ack) = handle_topology_update(1, &topo, &update);
        assert!(updated);

        let t = topo.read().unwrap();
        assert_eq!(t.node_count(), 2);

        match ack {
            RaftRpc::TopologyAck(a) => assert_eq!(a.accepted_version, t.version()),
            other => panic!("expected TopologyAck, got {other:?}"),
        }
    }

    #[test]
    fn topology_update_ignores_stale_version() {
        let topo = RwLock::new(ClusterTopology::new());
        {
            let mut t = topo.write().unwrap();
            t.add_node(NodeInfo::new(
                1,
                "10.0.0.1:9400".parse().unwrap(),
                NodeState::Active,
            ));
            // version is now 1
        }

        let update = TopologyUpdate {
            version: 0, // Older than current.
            nodes: vec![],
        };

        let (updated, _) = handle_topology_update(1, &topo, &update);
        assert!(!updated);

        let t = topo.read().unwrap();
        assert_eq!(t.node_count(), 1); // Unchanged.
    }

    #[tokio::test]
    async fn failure_tracking_marks_draining() {
        // Test the core failure detection logic without networking.
        let topo = Arc::new(RwLock::new(ClusterTopology::new()));
        {
            let mut t = topo.write().unwrap();
            t.add_node(NodeInfo::new(
                1,
                "10.0.0.1:9400".parse().unwrap(),
                NodeState::Active,
            ));
            t.add_node(NodeInfo::new(
                2,
                "10.0.0.2:9400".parse().unwrap(),
                NodeState::Active,
            ));
        }

        let transport = Arc::new(NexarTransport::new(1, "127.0.0.1:0".parse().unwrap()).unwrap());
        let dir = tempfile::tempdir().unwrap();
        let catalog = Arc::new(ClusterCatalog::open(&dir.path().join("cluster.redb")).unwrap());

        let monitor = HealthMonitor::new(
            1,
            transport,
            topo.clone(),
            catalog,
            HealthConfig {
                ping_interval: Duration::from_secs(5),
                failure_threshold: 3,
            },
        );

        // Simulate 3 consecutive ping failures.
        assert!(!monitor.record_ping_failure(2)); // 1st
        assert!(!monitor.record_ping_failure(2)); // 2nd
        assert!(monitor.record_ping_failure(2)); // 3rd — triggers Draining

        let t = topo.read().unwrap();
        assert_eq!(t.get_node(2).unwrap().state, NodeState::Draining);
    }

    #[tokio::test]
    async fn pong_recovers_node() {
        let topo = Arc::new(RwLock::new(ClusterTopology::new()));
        {
            let mut t = topo.write().unwrap();
            t.add_node(NodeInfo::new(
                1,
                "10.0.0.1:9400".parse().unwrap(),
                NodeState::Active,
            ));
            t.add_node(NodeInfo::new(
                2,
                "10.0.0.2:9400".parse().unwrap(),
                NodeState::Draining, // Previously marked down.
            ));
        }

        let transport = Arc::new(NexarTransport::new(1, "127.0.0.1:0".parse().unwrap()).unwrap());
        let dir = tempfile::tempdir().unwrap();
        let catalog = Arc::new(ClusterCatalog::open(&dir.path().join("cluster.redb")).unwrap());

        let monitor =
            HealthMonitor::new(1, transport, topo.clone(), catalog, HealthConfig::default());

        let pong = PongResponse {
            responder_id: 2,
            topology_version: 1,
        };
        let changed = monitor.handle_pong(2, &pong);
        assert!(changed); // Should have transitioned to Active.

        let t = topo.read().unwrap();
        assert_eq!(t.get_node(2).unwrap().state, NodeState::Active);
    }
}
