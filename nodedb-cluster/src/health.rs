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
use std::time::{Duration, Instant};

use tracing::{debug, info, warn};

use crate::catalog::ClusterCatalog;
use crate::loop_metrics::LoopMetrics;
use crate::rpc_codec::{
    JoinNodeInfo, PingRequest, PongResponse, RaftRpc, TopologyAck, TopologyUpdate,
};
use crate::topology::{ClusterTopology, NodeState};
use crate::transport::NexarTransport;

/// Default ping interval.
///
/// Corresponds to `ClusterTransportTuning::health_ping_interval_secs`.
pub const DEFAULT_PING_INTERVAL: Duration = Duration::from_secs(5);

/// Default number of consecutive failures before marking a node as down.
///
/// Corresponds to `ClusterTransportTuning::health_failure_threshold`.
pub const DEFAULT_FAILURE_THRESHOLD: u32 = 3;

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
    loop_metrics: Arc<LoopMetrics>,
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
            loop_metrics: LoopMetrics::new("health_loop"),
        }
    }

    /// Shared handle to this loop's standardized metrics.
    pub fn loop_metrics(&self) -> Arc<LoopMetrics> {
        Arc::clone(&self.loop_metrics)
    }

    /// Snapshot of currently-suspect peers (non-zero consecutive
    /// ping-failure count). Used to render the labeled
    /// `health_loop_suspect_peers{peer_id}` gauge.
    pub fn suspect_peers(&self) -> HashMap<u64, u32> {
        self.ping_failures
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }

    /// Run the health monitor until shutdown.
    pub async fn run(&self, mut shutdown: tokio::sync::watch::Receiver<bool>) {
        let mut interval = tokio::time::interval(self.config.ping_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        info!(node_id = self.node_id, "health monitor started");
        self.loop_metrics.set_up(true);

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let started = Instant::now();
                    self.ping_all_peers().await;
                    self.loop_metrics.observe(started.elapsed());
                }
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        debug!("health monitor shutting down");
                        break;
                    }
                }
            }
        }
        self.loop_metrics.set_up(false);
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

    /// Handle a successful pong — reset failure count, mark node Active
    /// if needed, and push topology if the peer is behind.
    fn handle_pong(&self, peer_id: u64, pong: &PongResponse) -> bool {
        // Reset failure count.
        {
            let mut failures = self.ping_failures.lock().unwrap_or_else(|p| p.into_inner());
            failures.remove(&peer_id);
        }

        // Push topology to peers with a stale version. This closes
        // the convergence gap when the fire-and-forget broadcast
        // during the join flow is lost (e.g. peer QUIC server not
        // yet accepting at that instant).
        let our_version = {
            let topo = self.topology.read().unwrap_or_else(|p| p.into_inner());
            topo.version()
        };
        if pong.topology_version < our_version {
            debug!(
                peer_id,
                peer_version = pong.topology_version,
                our_version,
                "peer has stale topology, pushing update"
            );
            let transport = self.transport.clone();
            let topology = self.topology.clone();
            let self_id = self.node_id;
            tokio::spawn(async move {
                broadcast_topology_to_peer(self_id, peer_id, &topology, &transport).await;
            });
        }

        // If node was not Active, mark it Active.
        let mut topo = self.topology.write().unwrap_or_else(|p| p.into_inner());
        if let Some(node) = topo.get_node(peer_id)
            && node.state != NodeState::Active
            && node.state != NodeState::Decommissioned
        {
            info!(peer_id, "peer recovered, marking active");
            topo.set_state(peer_id, NodeState::Active);
            return true;
        }
        false
    }

    /// Record a ping failure. Returns true if topology changed (node marked Draining).
    fn record_ping_failure(&self, peer_id: u64) -> bool {
        self.loop_metrics.record_error("ping");
        let count = {
            let mut failures = self.ping_failures.lock().unwrap_or_else(|p| p.into_inner());
            let count = failures.entry(peer_id).or_insert(0);
            *count += 1;
            *count
        };

        if count >= self.config.failure_threshold {
            let mut topo = self.topology.write().unwrap_or_else(|p| p.into_inner());
            if let Some(node) = topo.get_node(peer_id)
                && node.state == NodeState::Active
            {
                warn!(
                    peer_id,
                    failures = count,
                    "peer unreachable, marking draining"
                );
                topo.set_state(peer_id, NodeState::Draining);
                return true;
            }
        }
        false
    }

    /// Persist updated topology and broadcast to all active peers.
    async fn persist_and_broadcast(&self) {
        let topo = self.topology.read().unwrap_or_else(|p| p.into_inner());
        if let Err(e) = self.catalog.save_topology(&topo) {
            warn!(error = %e, "failed to persist topology update");
        }
        drop(topo);
        broadcast_topology(self.node_id, &self.topology, &self.transport);
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

/// Broadcast the current topology to every active peer (fire-and-forget).
///
/// Shared by [`HealthMonitor`] and the cluster-join path
/// (`raft_loop::join`). Does not block — spawns one detached task per
/// peer and returns immediately. Uses a short-lived read guard to
/// snapshot the topology and the peer list under one lock acquisition.
pub fn broadcast_topology(
    self_node_id: u64,
    topology: &RwLock<ClusterTopology>,
    transport: &Arc<NexarTransport>,
) {
    let (update, active_peers) = {
        let topo = topology.read().unwrap_or_else(|p| p.into_inner());
        let update = RaftRpc::TopologyUpdate(TopologyUpdate {
            version: topo.version(),
            nodes: topo
                .all_nodes()
                .map(|n| JoinNodeInfo {
                    node_id: n.node_id,
                    addr: n.addr.clone(),
                    state: n.state.as_u8(),
                    raft_groups: n.raft_groups.clone(),
                    wire_version: n.wire_version,
                })
                .collect(),
        });
        let peers: Vec<u64> = topo
            .active_nodes()
            .iter()
            .map(|n| n.node_id)
            .filter(|&id| id != self_node_id)
            .collect();
        (update, peers)
    };

    for peer_id in active_peers {
        let transport = transport.clone();
        let msg = update.clone();
        tokio::spawn(async move {
            if let Err(e) = transport.send_rpc(peer_id, msg).await {
                debug!(peer_id, error = %e, "topology broadcast failed");
            }
        });
    }
}

/// Send a topology update to a single peer that has a stale version.
async fn broadcast_topology_to_peer(
    _self_node_id: u64,
    peer_id: u64,
    topology: &RwLock<ClusterTopology>,
    transport: &NexarTransport,
) {
    let update = {
        let topo = topology.read().unwrap_or_else(|p| p.into_inner());
        RaftRpc::TopologyUpdate(TopologyUpdate {
            version: topo.version(),
            nodes: topo
                .all_nodes()
                .map(|n| JoinNodeInfo {
                    node_id: n.node_id,
                    addr: n.addr.clone(),
                    state: n.state.as_u8(),
                    raft_groups: n.raft_groups.clone(),
                    wire_version: n.wire_version,
                })
                .collect(),
        })
    };
    if let Err(e) = transport.send_rpc(peer_id, update).await {
        debug!(peer_id, error = %e, "targeted topology push failed");
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
                wire_version: node.wire_version,
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
                    wire_version: crate::topology::CLUSTER_WIRE_FORMAT_VERSION,
                },
                JoinNodeInfo {
                    node_id: 2,
                    addr: "10.0.0.2:9400".into(),
                    state: 1,
                    raft_groups: vec![],
                    wire_version: crate::topology::CLUSTER_WIRE_FORMAT_VERSION,
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

        let transport = Arc::new(
            NexarTransport::new(
                1,
                "127.0.0.1:0".parse().unwrap(),
                crate::transport::credentials::TransportCredentials::Insecure,
            )
            .unwrap(),
        );
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

        let transport = Arc::new(
            NexarTransport::new(
                1,
                "127.0.0.1:0".parse().unwrap(),
                crate::transport::credentials::TransportCredentials::Insecure,
            )
            .unwrap(),
        );
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
