//! Raft event loop — drives MultiRaft ticks and dispatches messages over the transport.
//!
//! This module glues [`MultiRaft`] to [`NexarTransport`]:
//! - Periodic ticks drive elections and heartbeats
//! - `Ready` output is dispatched over QUIC (messages, vote requests)
//! - Incoming RPCs are routed to the correct Raft group
//! - Committed entries are applied via a [`CommitApplier`] callback
//! - RPC responses are fed back asynchronously (non-blocking tick loop)

use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use tracing::{debug, warn};

use nodedb_raft::message::LogEntry;
use nodedb_raft::transport::RaftTransport;

use crate::error::{ClusterError, Result};
use crate::forward::RequestForwarder;
use crate::health;
use crate::multi_raft::MultiRaft;
use crate::rpc_codec::RaftRpc;
use crate::topology::ClusterTopology;
use crate::transport::{NexarTransport, RaftRpcHandler};

/// Default tick interval (10ms — fast enough for sub-second elections).
const DEFAULT_TICK_INTERVAL: Duration = Duration::from_millis(10);

/// Callback for applying committed Raft log entries to the state machine.
///
/// Called synchronously during the tick loop. Implementations should be fast
/// (enqueue to SPSC, not perform I/O directly).
pub trait CommitApplier: Send + Sync + 'static {
    /// Apply committed entries for a Raft group.
    ///
    /// Returns the index of the last successfully applied entry.
    fn apply_committed(&self, group_id: u64, entries: &[LogEntry]) -> u64;
}

/// Raft event loop coordinator.
///
/// Owns the MultiRaft state (behind `Arc<Mutex>`) and drives it via periodic
/// ticks. Implements [`RaftRpcHandler`] so it can be passed directly to
/// [`NexarTransport::serve`] for incoming RPC dispatch.
pub struct RaftLoop<A: CommitApplier, F: RequestForwarder = crate::forward::NoopForwarder> {
    node_id: u64,
    multi_raft: Arc<Mutex<MultiRaft>>,
    transport: Arc<NexarTransport>,
    topology: Arc<RwLock<ClusterTopology>>,
    applier: A,
    forwarder: Arc<F>,
    tick_interval: Duration,
}

impl<A: CommitApplier> RaftLoop<A> {
    pub fn new(
        multi_raft: MultiRaft,
        transport: Arc<NexarTransport>,
        topology: Arc<RwLock<ClusterTopology>>,
        applier: A,
    ) -> Self {
        let node_id = multi_raft.node_id();
        Self {
            node_id,
            multi_raft: Arc::new(Mutex::new(multi_raft)),
            transport,
            topology,
            applier,
            forwarder: Arc::new(crate::forward::NoopForwarder),
            tick_interval: DEFAULT_TICK_INTERVAL,
        }
    }

}

impl<A: CommitApplier, F: RequestForwarder> RaftLoop<A, F> {
    /// Create a RaftLoop with a custom request forwarder (for cluster mode).
    pub fn with_forwarder(
        multi_raft: MultiRaft,
        transport: Arc<NexarTransport>,
        topology: Arc<RwLock<ClusterTopology>>,
        applier: A,
        forwarder: Arc<F>,
    ) -> Self {
        let node_id = multi_raft.node_id();
        Self {
            node_id,
            multi_raft: Arc::new(Mutex::new(multi_raft)),
            transport,
            topology,
            applier,
            forwarder,
            tick_interval: DEFAULT_TICK_INTERVAL,
        }
    }

    pub fn with_tick_interval(mut self, interval: Duration) -> Self {
        self.tick_interval = interval;
        self
    }

    /// Run the event loop until shutdown.
    ///
    /// This drives Raft elections, heartbeats, and message dispatch.
    /// Call [`NexarTransport::serve`] separately with `Arc<Self>` as the handler.
    pub async fn run(&self, mut shutdown: tokio::sync::watch::Receiver<bool>) {
        let mut interval = tokio::time::interval(self.tick_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    self.do_tick();
                }
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        debug!("raft loop shutting down");
                        break;
                    }
                }
            }
        }
    }

    /// Execute a single tick: drive Raft, dispatch outbound messages, apply commits.
    fn do_tick(&self) {
        // Phase 1: tick under lock, extract Ready.
        let ready = {
            let mut mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
            mr.tick()
        };

        if ready.is_empty() {
            return;
        }

        // Phase 2: process each group's Ready outside the lock.
        for (group_id, group_ready) in ready.groups {
            // Dispatch AppendEntries RPCs (fire-and-forget, responses handled async).
            for (peer, req) in group_ready.messages {
                let transport = self.transport.clone();
                let mr = self.multi_raft.clone();
                tokio::spawn(async move {
                    match transport.append_entries(peer, req).await {
                        Ok(resp) => {
                            let mut mr = mr.lock().unwrap_or_else(|p| p.into_inner());
                            if let Err(e) = mr.handle_append_entries_response(group_id, peer, &resp)
                            {
                                debug!(group_id, peer, error = %e, "handle ae response");
                            }
                        }
                        Err(e) => {
                            warn!(group_id, peer, error = %e, "append_entries RPC failed");
                        }
                    }
                });
            }

            // Dispatch RequestVote RPCs.
            for (peer, req) in group_ready.vote_requests {
                let transport = self.transport.clone();
                let mr = self.multi_raft.clone();
                tokio::spawn(async move {
                    match transport.request_vote(peer, req).await {
                        Ok(resp) => {
                            let mut mr = mr.lock().unwrap_or_else(|p| p.into_inner());
                            if let Err(e) = mr.handle_request_vote_response(group_id, peer, &resp) {
                                debug!(group_id, peer, error = %e, "handle vote response");
                            }
                        }
                        Err(e) => {
                            warn!(group_id, peer, error = %e, "request_vote RPC failed");
                        }
                    }
                });
            }

            // Apply committed entries synchronously.
            if !group_ready.committed_entries.is_empty() {
                let last_applied = self
                    .applier
                    .apply_committed(group_id, &group_ready.committed_entries);
                if last_applied > 0 {
                    let mut mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
                    if let Err(e) = mr.advance_applied(group_id, last_applied) {
                        warn!(group_id, error = %e, "failed to advance applied index");
                    }
                }
            }

            // Log snapshot needs (actual snapshot transfer is a later batch).
            for peer in group_ready.snapshots_needed {
                warn!(group_id, peer, "peer needs snapshot (not yet implemented)");
            }
        }
    }

    /// Propose a command to the Raft group owning the given vShard.
    pub fn propose(&self, vshard_id: u16, data: Vec<u8>) -> Result<u64> {
        let mut mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
        mr.propose(vshard_id, data)
    }
}

// ── Incoming RPC handler ────────────────────────────────────────────

impl<A: CommitApplier, F: RequestForwarder> RaftRpcHandler for RaftLoop<A, F> {
    async fn handle_rpc(&self, rpc: RaftRpc) -> Result<RaftRpc> {
        match rpc {
            // Raft consensus RPCs — lock MultiRaft (sync, never across await).
            RaftRpc::AppendEntriesRequest(req) => {
                let mut mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
                let resp = mr.handle_append_entries(&req)?;
                Ok(RaftRpc::AppendEntriesResponse(resp))
            }
            RaftRpc::RequestVoteRequest(req) => {
                let mut mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
                let resp = mr.handle_request_vote(&req)?;
                Ok(RaftRpc::RequestVoteResponse(resp))
            }
            RaftRpc::InstallSnapshotRequest(_req) => Err(ClusterError::Transport {
                detail: "InstallSnapshot not yet implemented".into(),
            }),
            // Health check.
            RaftRpc::Ping(req) => {
                let topo_version = {
                    let topo = self.topology.read().unwrap_or_else(|p| p.into_inner());
                    topo.version()
                };
                Ok(health::handle_ping(self.node_id, topo_version, &req))
            }
            // Topology broadcast.
            RaftRpc::TopologyUpdate(update) => {
                let (_updated, ack) =
                    health::handle_topology_update(self.node_id, &self.topology, &update);
                Ok(ack)
            }
            // Query forwarding — execute locally via the RequestForwarder.
            RaftRpc::ForwardRequest(req) => {
                let resp = self.forwarder.execute_forwarded(req).await;
                Ok(RaftRpc::ForwardResponse(resp))
            }
            other => Err(ClusterError::Transport {
                detail: format!("unexpected request type in RPC handler: {other:?}"),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routing::RoutingTable;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Instant;

    /// Test applier that counts applied entries.
    struct CountingApplier {
        applied: AtomicU64,
    }

    impl CountingApplier {
        fn new() -> Self {
            Self {
                applied: AtomicU64::new(0),
            }
        }

        fn count(&self) -> u64 {
            self.applied.load(Ordering::Relaxed)
        }
    }

    impl CommitApplier for CountingApplier {
        fn apply_committed(&self, _group_id: u64, entries: &[LogEntry]) -> u64 {
            self.applied
                .fetch_add(entries.len() as u64, Ordering::Relaxed);
            entries.last().map(|e| e.index).unwrap_or(0)
        }
    }

    /// Helper: create a transport on an ephemeral port.
    fn make_transport(node_id: u64) -> Arc<NexarTransport> {
        Arc::new(NexarTransport::new(node_id, "127.0.0.1:0".parse().unwrap()).unwrap())
    }

    #[tokio::test]
    async fn single_node_raft_loop_commits() {
        // Single-node cluster: elections and commits happen locally.
        let transport = make_transport(1);
        let rt = RoutingTable::uniform(1, &[1], 1);
        let mut mr = MultiRaft::new(1, rt);
        mr.add_group(0, vec![]);

        // Force election.
        for node in mr.groups_mut().values_mut() {
            node.election_deadline_override(Instant::now() - Duration::from_millis(1));
        }

        let applier = CountingApplier::new();
        let topo = Arc::new(RwLock::new(ClusterTopology::new()));
        let raft_loop = Arc::new(RaftLoop::new(mr, transport, topo, applier));

        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

        let rl = raft_loop.clone();
        let run_handle = tokio::spawn(async move {
            rl.run(shutdown_rx).await;
        });

        // Wait for election + no-op commit.
        tokio::time::sleep(Duration::from_millis(50)).await;

        // The no-op entry should have been committed and applied.
        assert!(
            raft_loop.applier.count() >= 1,
            "expected at least 1 applied entry (no-op), got {}",
            raft_loop.applier.count()
        );

        // Propose a command.
        let idx = raft_loop.propose(0, b"hello".to_vec()).unwrap();
        assert!(idx >= 2); // 1 = no-op, 2+ = our command

        // Wait for commit.
        tokio::time::sleep(Duration::from_millis(50)).await;

        assert!(
            raft_loop.applier.count() >= 2,
            "expected at least 2 applied entries, got {}",
            raft_loop.applier.count()
        );

        shutdown_tx.send(true).unwrap();
        run_handle.abort();
    }

    #[tokio::test]
    async fn three_node_election_over_quic() {
        // Three-node cluster: node 1 should win election via QUIC RPCs.
        let t1 = make_transport(1);
        let t2 = make_transport(2);
        let t3 = make_transport(3);

        // Register peers.
        t1.register_peer(2, t2.local_addr());
        t1.register_peer(3, t3.local_addr());
        t2.register_peer(1, t1.local_addr());
        t2.register_peer(3, t3.local_addr());
        t3.register_peer(1, t1.local_addr());
        t3.register_peer(2, t2.local_addr());

        let rt = RoutingTable::uniform(1, &[1, 2, 3], 3);

        // Node 1: force immediate election.
        let mut mr1 = MultiRaft::new(1, rt.clone());
        mr1.add_group(0, vec![2, 3]);
        for node in mr1.groups_mut().values_mut() {
            node.election_deadline_override(Instant::now() - Duration::from_millis(1));
        }

        // Nodes 2 and 3: normal timeouts (won't start election).
        let mut mr2 = MultiRaft::new(2, rt.clone())
            .with_election_timeout(Duration::from_secs(60), Duration::from_secs(120));
        mr2.add_group(0, vec![1, 3]);

        let mut mr3 = MultiRaft::new(3, rt.clone())
            .with_election_timeout(Duration::from_secs(60), Duration::from_secs(120));
        mr3.add_group(0, vec![1, 2]);

        let a1 = CountingApplier::new();
        let a2 = CountingApplier::new();
        let a3 = CountingApplier::new();

        let topo1 = Arc::new(RwLock::new(ClusterTopology::new()));
        let topo2 = Arc::new(RwLock::new(ClusterTopology::new()));
        let topo3 = Arc::new(RwLock::new(ClusterTopology::new()));

        let rl1 = Arc::new(RaftLoop::new(mr1, t1.clone(), topo1, a1));
        let rl2 = Arc::new(RaftLoop::new(mr2, t2.clone(), topo2, a2));
        let rl3 = Arc::new(RaftLoop::new(mr3, t3.clone(), topo3, a3));

        let (shutdown_tx, _) = tokio::sync::watch::channel(false);

        // Start serve loops (incoming RPCs).
        let rl2_h = rl2.clone();
        let sr2 = shutdown_tx.subscribe();
        tokio::spawn(async move { t2.serve(rl2_h, sr2).await });

        let rl3_h = rl3.clone();
        let sr3 = shutdown_tx.subscribe();
        tokio::spawn(async move { t3.serve(rl3_h, sr3).await });

        // Start tick loops.
        let rl1_r = rl1.clone();
        let sr1 = shutdown_tx.subscribe();
        tokio::spawn(async move { rl1_r.run(sr1).await });

        let rl2_r = rl2.clone();
        let sr2r = shutdown_tx.subscribe();
        tokio::spawn(async move { rl2_r.run(sr2r).await });

        let rl3_r = rl3.clone();
        let sr3r = shutdown_tx.subscribe();
        tokio::spawn(async move { rl3_r.run(sr3r).await });

        // Also start serve for node 1 (receives responses).
        let rl1_h = rl1.clone();
        let sr1h = shutdown_tx.subscribe();
        tokio::spawn(async move { t1.serve(rl1_h, sr1h).await });

        // Wait for election + replication.
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Node 1 should be leader and have committed the no-op.
        assert!(
            rl1.applier.count() >= 1,
            "node 1 should have committed at least the no-op, got {}",
            rl1.applier.count()
        );

        // Propose a command on node 1.
        let idx = rl1.propose(0, b"distributed-cmd".to_vec()).unwrap();
        assert!(idx >= 2);

        // Wait for replication.
        tokio::time::sleep(Duration::from_millis(200)).await;

        // All nodes should have applied the command (replicated via Raft).
        assert!(
            rl1.applier.count() >= 2,
            "node 1: expected >= 2 applied, got {}",
            rl1.applier.count()
        );

        // Followers apply committed entries when they receive AppendEntries
        // with updated leader_commit.
        assert!(
            rl2.applier.count() >= 1,
            "node 2: expected >= 1 applied, got {}",
            rl2.applier.count()
        );
        assert!(
            rl3.applier.count() >= 1,
            "node 3: expected >= 1 applied, got {}",
            rl3.applier.count()
        );

        shutdown_tx.send(true).unwrap();
    }

    #[tokio::test]
    async fn rpc_handler_routes_append_entries() {
        let transport = make_transport(1);
        let rt = RoutingTable::uniform(1, &[1], 1);
        let mut mr = MultiRaft::new(1, rt);
        mr.add_group(0, vec![]);

        // Make it a leader first.
        for node in mr.groups_mut().values_mut() {
            node.election_deadline_override(Instant::now() - Duration::from_millis(1));
        }

        let topo = Arc::new(RwLock::new(ClusterTopology::new()));
        let raft_loop = RaftLoop::new(mr, transport, topo, CountingApplier::new());

        // Tick to trigger election.
        raft_loop.do_tick();
        tokio::time::sleep(Duration::from_millis(20)).await;

        // Send an AppendEntries RPC via the handler.
        let req = RaftRpc::AppendEntriesRequest(nodedb_raft::AppendEntriesRequest {
            term: 99, // Higher term — will cause step-down.
            leader_id: 2,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
            group_id: 0,
        });

        let resp = raft_loop.handle_rpc(req).await.unwrap();
        match resp {
            RaftRpc::AppendEntriesResponse(r) => {
                assert!(r.success);
                assert_eq!(r.term, 99);
            }
            other => panic!("expected AppendEntriesResponse, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn rpc_handler_routes_request_vote() {
        let transport = make_transport(1);
        let rt = RoutingTable::uniform(1, &[1, 2, 3], 3);
        let mut mr = MultiRaft::new(1, rt);
        mr.add_group(0, vec![2, 3]);

        let topo = Arc::new(RwLock::new(ClusterTopology::new()));
        let raft_loop = RaftLoop::new(mr, transport, topo, CountingApplier::new());

        let req = RaftRpc::RequestVoteRequest(nodedb_raft::RequestVoteRequest {
            term: 1,
            candidate_id: 2,
            last_log_index: 0,
            last_log_term: 0,
            group_id: 0,
        });

        let resp = raft_loop.handle_rpc(req).await.unwrap();
        match resp {
            RaftRpc::RequestVoteResponse(r) => {
                assert!(r.vote_granted);
                assert_eq!(r.term, 1);
            }
            other => panic!("expected RequestVoteResponse, got {other:?}"),
        }
    }
}
