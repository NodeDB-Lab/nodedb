//! `RaftLoop` struct, constructors, top-level run loop, and thin wrappers
//! over `MultiRaft` proposal APIs. The tick body lives in
//! [`super::tick`]; the inbound-RPC handler lives in
//! [`super::handle_rpc`]; the async join orchestration lives in
//! [`super::join`].

use std::pin::Pin;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use tracing::debug;

use nodedb_raft::message::LogEntry;

use crate::catalog::ClusterCatalog;
use crate::conf_change::ConfChange;
use crate::error::Result;
use crate::forward::{NoopPlanExecutor, PlanExecutor};
use crate::loop_metrics::LoopMetrics;
use crate::metadata_group::applier::{MetadataApplier, NoopMetadataApplier};
use crate::multi_raft::MultiRaft;
use crate::topology::ClusterTopology;
use crate::transport::NexarTransport;

/// Default tick interval (10ms — fast enough for sub-second elections).
///
/// Matches `ClusterTransportTuning::raft_tick_interval_ms` default.
pub(super) const DEFAULT_TICK_INTERVAL: Duration = Duration::from_millis(10);

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

/// Type-erased async handler for incoming `VShardEnvelope` messages.
///
/// Receives raw envelope bytes, returns response bytes. Set by the main binary
/// to dispatch to the appropriate engine handler (Event Plane, timeseries, etc.).
pub type VShardEnvelopeHandler = Arc<
    dyn Fn(Vec<u8>) -> Pin<Box<dyn std::future::Future<Output = Result<Vec<u8>>> + Send>>
        + Send
        + Sync,
>;

/// Raft event loop coordinator.
///
/// Owns the MultiRaft state (behind `Arc<Mutex>`) and drives it via periodic
/// ticks. Implements [`crate::transport::RaftRpcHandler`] (in
/// [`super::handle_rpc`]) so it can be passed directly to
/// [`NexarTransport::serve`] for incoming RPC dispatch.
///
/// The `F: RequestForwarder` generic parameter was removed in C-δ.6 when the
/// SQL-string forwarding path was retired. Cross-node SQL routing now goes
/// through `gateway.execute / ExecuteRequest` (C-β path).
pub struct RaftLoop<A: CommitApplier, P: PlanExecutor = NoopPlanExecutor> {
    pub(super) node_id: u64,
    pub(super) multi_raft: Arc<Mutex<MultiRaft>>,
    pub(super) transport: Arc<NexarTransport>,
    pub(super) topology: Arc<RwLock<ClusterTopology>>,
    pub(super) applier: A,
    /// Applies committed entries from the metadata Raft group (group 0).
    pub(super) metadata_applier: Arc<dyn MetadataApplier>,
    /// Executes incoming `ExecuteRequest` RPCs without SQL re-planning.
    pub(super) plan_executor: Arc<P>,
    pub(super) tick_interval: Duration,
    /// Optional handler for incoming VShardEnvelope messages.
    /// Set when the Event Plane or other subsystems need cross-node messaging.
    pub(super) vshard_handler: Option<VShardEnvelopeHandler>,
    /// Optional catalog handle for persisting topology/routing updates
    /// from the join flow. When `None`, persistence is skipped — useful
    /// for unit tests that don't care about durability.
    pub(super) catalog: Option<Arc<ClusterCatalog>>,
    /// Cooperative shutdown signal observed by every detached
    /// `tokio::spawn` task in [`super::tick`]. `run()` flips it on
    /// its own shutdown, and [`Self::begin_shutdown`] provides a
    /// direct entry point for test harnesses that abort the run /
    /// serve handles and need the spawned tasks to drop their
    /// `Arc<Mutex<MultiRaft>>` clones immediately so the per-group
    /// redb log files can release their in-process locks.
    ///
    /// Using `watch::Sender` here rather than a raw `AtomicBool` +
    /// `Notify` pair gives us two properties at once: the latest
    /// value is visible to every newly-subscribed receiver (no
    /// missed-notification race when a new detached task is
    /// spawned just after `begin_shutdown`), and awaiting
    /// `receiver.changed()` is cancellable inside `tokio::select!`.
    pub(super) shutdown_watch: tokio::sync::watch::Sender<bool>,
    /// Standardized loop observations. Updated inside `run()` after
    /// every `do_tick`. Register via
    /// [`Self::loop_metrics`] with the cluster registry.
    pub(super) loop_metrics: Arc<LoopMetrics>,
    /// Boot-time readiness signal. Flipped to `true` by
    /// [`super::tick::do_tick`] after the first tick completes phase
    /// 4 (apply committed entries) — i.e. once Raft has driven at
    /// least one no-op or replayed entries past the persisted
    /// applied watermark on this node.
    ///
    /// The host crate's `start_raft` returns `subscribe_ready()` to
    /// `main.rs`, which awaits it before binding any client-facing
    /// listener. This guarantees the first SQL DDL the operator
    /// runs after process start cannot race against an
    /// uninitialized metadata raft group, which previously surfaced
    /// as `metadata propose: not leader` under fast restart loops.
    pub(super) ready_watch: tokio::sync::watch::Sender<bool>,
}

impl<A: CommitApplier> RaftLoop<A> {
    pub fn new(
        multi_raft: MultiRaft,
        transport: Arc<NexarTransport>,
        topology: Arc<RwLock<ClusterTopology>>,
        applier: A,
    ) -> Self {
        let node_id = multi_raft.node_id();
        let (shutdown_watch, _) = tokio::sync::watch::channel(false);
        let (ready_watch, _) = tokio::sync::watch::channel(false);
        Self {
            node_id,
            multi_raft: Arc::new(Mutex::new(multi_raft)),
            transport,
            topology,
            applier,
            metadata_applier: Arc::new(NoopMetadataApplier),
            plan_executor: Arc::new(NoopPlanExecutor),
            tick_interval: DEFAULT_TICK_INTERVAL,
            vshard_handler: None,
            catalog: None,
            shutdown_watch,
            ready_watch,
            loop_metrics: LoopMetrics::new("raft_tick_loop"),
        }
    }
}

impl<A: CommitApplier, P: PlanExecutor> RaftLoop<A, P> {
    /// Install a custom plan executor (for cluster mode — C-β path).
    pub fn with_plan_executor<P2: PlanExecutor>(self, executor: Arc<P2>) -> RaftLoop<A, P2> {
        RaftLoop {
            node_id: self.node_id,
            multi_raft: self.multi_raft,
            transport: self.transport,
            topology: self.topology,
            applier: self.applier,
            metadata_applier: self.metadata_applier,
            plan_executor: executor,
            tick_interval: self.tick_interval,
            vshard_handler: self.vshard_handler,
            catalog: self.catalog,
            shutdown_watch: self.shutdown_watch,
            ready_watch: self.ready_watch,
            loop_metrics: self.loop_metrics,
        }
    }

    /// Shared handle to this loop's standardized metrics.
    pub fn loop_metrics(&self) -> Arc<LoopMetrics> {
        Arc::clone(&self.loop_metrics)
    }

    /// Count of Raft groups currently mounted on this node — used to
    /// render the `raft_tick_loop_pending_groups` gauge.
    pub fn pending_groups(&self) -> usize {
        let mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
        mr.group_count()
    }

    /// Signal cooperative shutdown to every detached task spawned
    /// inside [`super::tick::do_tick`].
    ///
    /// This is the entry point for test harnesses that want to
    /// tear down a `RaftLoop` without waiting for the external
    /// `run()` shutdown watch channel to propagate. In production
    /// the same signal is emitted automatically by `run()` when
    /// its external shutdown receiver fires.
    ///
    /// Idempotent: calling this multiple times is a no-op after
    /// the first.
    pub fn begin_shutdown(&self) {
        let _ = self.shutdown_watch.send(true);
    }

    /// Subscribe to the boot-time readiness signal.
    ///
    /// The returned receiver starts at `false` and flips to `true`
    /// exactly once, after the first [`super::tick::do_tick`]
    /// completes phase 4 (apply committed entries). Used by the
    /// host crate to gate client-facing listener startup until the
    /// metadata raft group has produced its first applied entry.
    pub fn subscribe_ready(&self) -> tokio::sync::watch::Receiver<bool> {
        self.ready_watch.subscribe()
    }

    /// Set a handler for incoming VShardEnvelope messages.
    pub fn with_vshard_handler(mut self, handler: VShardEnvelopeHandler) -> Self {
        self.vshard_handler = Some(handler);
        self
    }

    /// Install the metadata applier used for group-0 commits.
    ///
    /// The host crate (nodedb) calls this with a production applier that
    /// wraps an in-memory `MetadataCache` and additionally persists to
    /// redb / broadcasts catalog change events. The default
    /// [`NoopMetadataApplier`] is kept only for tests that don't care.
    pub fn with_metadata_applier(mut self, applier: Arc<dyn MetadataApplier>) -> Self {
        self.metadata_applier = applier;
        self
    }

    pub fn with_tick_interval(mut self, interval: Duration) -> Self {
        self.tick_interval = interval;
        self
    }

    /// Attach a cluster catalog — used by the join flow to persist the
    /// updated topology + routing after a conf-change commits.
    pub fn with_catalog(mut self, catalog: Arc<ClusterCatalog>) -> Self {
        self.catalog = Some(catalog);
        self
    }

    /// This node's id (exposed for handlers and tests).
    pub fn node_id(&self) -> u64 {
        self.node_id
    }

    /// Run the event loop until shutdown.
    ///
    /// This drives Raft elections, heartbeats, and message dispatch.
    /// Call [`NexarTransport::serve`] separately with `Arc<Self>` as the handler.
    ///
    /// When the externally-supplied `shutdown` receiver fires,
    /// the loop also propagates the signal to the internal
    /// cooperative-shutdown channel so every detached task
    /// spawned inside `do_tick` exits promptly and drops its
    /// `Arc<Mutex<MultiRaft>>` clone.
    pub async fn run(&self, mut shutdown: tokio::sync::watch::Receiver<bool>) {
        let mut interval = tokio::time::interval(self.tick_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        self.loop_metrics.set_up(true);

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let started = Instant::now();
                    self.do_tick();
                    self.loop_metrics.observe(started.elapsed());
                }
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        debug!("raft loop shutting down");
                        self.begin_shutdown();
                        break;
                    }
                }
            }
        }
        self.loop_metrics.set_up(false);
    }

    /// Propose a command to the Raft group owning the given vShard.
    ///
    /// Returns `(group_id, log_index)` on success.
    pub fn propose(&self, vshard_id: u16, data: Vec<u8>) -> Result<(u64, u64)> {
        let mut mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
        mr.propose(vshard_id, data)
    }

    /// Propose a command directly to the metadata Raft group (group 0).
    ///
    /// Used by the host crate's metadata proposer and by integration
    /// tests that exercise the replicated-catalog path without a
    /// pgwire client. Fails with `ClusterError::GroupNotFound` if
    /// group 0 does not exist on this node, and with
    /// `ClusterError::Raft(NotLeader)` if this node is not the
    /// current leader of group 0.
    pub fn propose_to_metadata_group(&self, data: Vec<u8>) -> Result<u64> {
        let mut mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
        mr.propose_to_group(crate::metadata_group::METADATA_GROUP_ID, data)
    }

    /// Propose to the metadata Raft group, transparently forwarding
    /// to the current leader if this node is not it.
    ///
    /// Tries a local propose first. On
    /// `ClusterError::Raft(NotLeader { leader_hint })`, looks up the
    /// hinted leader's address in cluster topology and sends a
    /// [`crate::rpc_codec::MetadataProposeRequest`] over QUIC. The
    /// receiving leader applies the proposal locally and returns
    /// the log index.
    ///
    /// On `NotLeader { leader_hint: None }` (election in progress,
    /// no observed leader yet) the call returns the original
    /// `NotLeader` error so the caller can decide whether to retry.
    /// We deliberately do not implement a wait-and-retry loop here
    /// because the caller (the host-side proposer) may have a
    /// shorter deadline than any reasonable retry budget.
    ///
    /// The leader-side path through this function is identical to
    /// the bare `propose_to_metadata_group` — the only extra cost is
    /// an `is_leader_locally` check before the local propose.
    pub async fn propose_to_metadata_group_via_leader(&self, data: Vec<u8>) -> Result<u64> {
        // Phase 1: try local propose.
        match self.propose_to_metadata_group(data.clone()) {
            Ok(idx) => Ok(idx),
            Err(crate::error::ClusterError::Raft(nodedb_raft::RaftError::NotLeader {
                leader_hint,
            })) => {
                let Some(leader_id) = leader_hint else {
                    return Err(crate::error::ClusterError::Raft(
                        nodedb_raft::RaftError::NotLeader { leader_hint: None },
                    ));
                };
                if leader_id == self.node_id {
                    // Should not happen — local propose said we
                    // weren't leader but the hint points at us. Fall
                    // through to the original error so the caller
                    // sees the contradiction.
                    return Err(crate::error::ClusterError::Raft(
                        nodedb_raft::RaftError::NotLeader {
                            leader_hint: Some(leader_id),
                        },
                    ));
                }
                // Phase 2: forward to the hinted leader.
                self.forward_metadata_propose(leader_id, data).await
            }
            Err(other) => Err(other),
        }
    }

    /// Send a `MetadataProposeRequest` to `leader_id`. Looks up the
    /// leader's listen address via the local topology snapshot and
    /// dispatches through the existing peer transport.
    async fn forward_metadata_propose(&self, leader_id: u64, data: Vec<u8>) -> Result<u64> {
        // Resolve and register the leader's address with the
        // transport so `send_rpc` has a destination. Topology is
        // updated by the membership / health subsystem; if the
        // leader isn't in our local topology yet we fail loudly so
        // the caller can fall back to its own retry policy rather
        // than silently dropping the proposal.
        {
            let topo = self.topology.read().unwrap_or_else(|p| p.into_inner());
            let Some(node) = topo.get_node(leader_id) else {
                return Err(crate::error::ClusterError::Transport {
                    detail: format!(
                        "metadata propose forward: leader {leader_id} not in local topology"
                    ),
                });
            };
            let Some(addr) = node.socket_addr() else {
                return Err(crate::error::ClusterError::Transport {
                    detail: format!(
                        "metadata propose forward: leader {leader_id} has unparseable addr {:?}",
                        node.addr
                    ),
                });
            };
            // Idempotent: register_peer overwrites any prior mapping.
            self.transport.register_peer(leader_id, addr);
        }

        let req = crate::rpc_codec::RaftRpc::MetadataProposeRequest(
            crate::rpc_codec::MetadataProposeRequest { bytes: data },
        );
        let resp = self.transport.send_rpc(leader_id, req).await?;
        match resp {
            crate::rpc_codec::RaftRpc::MetadataProposeResponse(r) => {
                if r.success {
                    Ok(r.log_index)
                } else if let Some(hint) = r.leader_hint {
                    // The receiving node was also not the leader
                    // (rare: leader changed between our local check
                    // and the forwarded RPC). Surface as NotLeader
                    // so the caller's normal retry path runs.
                    Err(crate::error::ClusterError::Raft(
                        nodedb_raft::RaftError::NotLeader {
                            leader_hint: Some(hint),
                        },
                    ))
                } else {
                    Err(crate::error::ClusterError::Transport {
                        detail: format!("metadata propose forward failed: {}", r.error_message),
                    })
                }
            }
            other => Err(crate::error::ClusterError::Transport {
                detail: format!("metadata propose forward: unexpected response variant {other:?}"),
            }),
        }
    }

    /// Returns the inner multi-raft handle. Exposed for tests and for
    /// the host crate's metadata proposer so it can hold a second
    /// reference to the same underlying mutex without pulling the
    /// whole raft loop into the caller's lifetime.
    pub fn multi_raft_handle(&self) -> Arc<Mutex<crate::multi_raft::MultiRaft>> {
        self.multi_raft.clone()
    }

    /// Snapshot all Raft group states for observability (SHOW RAFT GROUPS).
    pub fn group_statuses(&self) -> Vec<crate::multi_raft::GroupStatus> {
        let mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
        mr.group_statuses()
    }

    /// Propose a configuration change to a Raft group.
    ///
    /// Returns `(group_id, log_index)` on success.
    pub fn propose_conf_change(&self, group_id: u64, change: &ConfChange) -> Result<(u64, u64)> {
        let mut mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
        mr.propose_conf_change(group_id, change)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routing::RoutingTable;
    use nodedb_types::config::tuning::ClusterTransportTuning;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Instant;

    /// Test applier that counts applied entries across both data and
    /// metadata groups. The metadata-group variant ([`CountingMetadataApplier`])
    /// increments the same counter so tests that propose against group 0
    /// (the metadata group) still see the count move.
    pub(crate) struct CountingApplier {
        applied: Arc<AtomicU64>,
    }

    impl CountingApplier {
        pub(crate) fn new() -> Self {
            Self {
                applied: Arc::new(AtomicU64::new(0)),
            }
        }

        pub(crate) fn count(&self) -> u64 {
            self.applied.load(Ordering::Relaxed)
        }

        pub(crate) fn metadata_applier(&self) -> Arc<CountingMetadataApplier> {
            Arc::new(CountingMetadataApplier {
                applied: self.applied.clone(),
            })
        }
    }

    impl CommitApplier for CountingApplier {
        fn apply_committed(&self, _group_id: u64, entries: &[LogEntry]) -> u64 {
            self.applied
                .fetch_add(entries.len() as u64, Ordering::Relaxed);
            entries.last().map(|e| e.index).unwrap_or(0)
        }
    }

    pub(crate) struct CountingMetadataApplier {
        applied: Arc<AtomicU64>,
    }

    impl MetadataApplier for CountingMetadataApplier {
        fn apply(&self, entries: &[(u64, Vec<u8>)]) -> u64 {
            self.applied
                .fetch_add(entries.len() as u64, Ordering::Relaxed);
            entries.last().map(|(idx, _)| *idx).unwrap_or(0)
        }
    }

    /// Helper: create a transport on an ephemeral port.
    fn make_transport(node_id: u64) -> Arc<NexarTransport> {
        Arc::new(
            NexarTransport::new(
                node_id,
                "127.0.0.1:0".parse().unwrap(),
                crate::transport::credentials::TransportCredentials::Insecure,
            )
            .unwrap(),
        )
    }

    #[tokio::test]
    async fn single_node_raft_loop_commits() {
        let dir = tempfile::tempdir().unwrap();
        let transport = make_transport(1);
        let rt = RoutingTable::uniform(1, &[1], 1);
        let mut mr = MultiRaft::new(1, rt, dir.path().to_path_buf());
        mr.add_group(0, vec![]).unwrap();

        for node in mr.groups_mut().values_mut() {
            node.election_deadline_override(Instant::now() - Duration::from_millis(1));
        }

        let applier = CountingApplier::new();
        let meta = applier.metadata_applier();
        let topo = Arc::new(RwLock::new(ClusterTopology::new()));
        let raft_loop =
            Arc::new(RaftLoop::new(mr, transport, topo, applier).with_metadata_applier(meta));

        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

        let rl = raft_loop.clone();
        let run_handle = tokio::spawn(async move {
            rl.run(shutdown_rx).await;
        });

        tokio::time::sleep(Duration::from_millis(50)).await;

        assert!(
            raft_loop.applier.count() >= 1,
            "expected at least 1 applied entry (no-op), got {}",
            raft_loop.applier.count()
        );

        let (_gid, idx) = raft_loop.propose(0, b"hello".to_vec()).unwrap();
        assert!(idx >= 2);

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
        let t1 = make_transport(1);
        let t2 = make_transport(2);
        let t3 = make_transport(3);

        t1.register_peer(2, t2.local_addr());
        t1.register_peer(3, t3.local_addr());
        t2.register_peer(1, t1.local_addr());
        t2.register_peer(3, t3.local_addr());
        t3.register_peer(1, t1.local_addr());
        t3.register_peer(2, t2.local_addr());

        let rt = RoutingTable::uniform(1, &[1, 2, 3], 3);

        let dir1 = tempfile::tempdir().unwrap();
        let mut mr1 = MultiRaft::new(1, rt.clone(), dir1.path().to_path_buf());
        mr1.add_group(0, vec![2, 3]).unwrap();
        for node in mr1.groups_mut().values_mut() {
            node.election_deadline_override(Instant::now() - Duration::from_millis(1));
        }

        let transport_tuning = ClusterTransportTuning::default();
        let election_timeout_min = Duration::from_secs(transport_tuning.election_timeout_min_secs);
        let election_timeout_max = Duration::from_secs(transport_tuning.election_timeout_max_secs);

        let dir2 = tempfile::tempdir().unwrap();
        let mut mr2 = MultiRaft::new(2, rt.clone(), dir2.path().to_path_buf())
            .with_election_timeout(election_timeout_min, election_timeout_max);
        mr2.add_group(0, vec![1, 3]).unwrap();

        let dir3 = tempfile::tempdir().unwrap();
        let mut mr3 = MultiRaft::new(3, rt.clone(), dir3.path().to_path_buf())
            .with_election_timeout(election_timeout_min, election_timeout_max);
        mr3.add_group(0, vec![1, 2]).unwrap();

        let a1 = CountingApplier::new();
        let m1 = a1.metadata_applier();
        let a2 = CountingApplier::new();
        let m2 = a2.metadata_applier();
        let a3 = CountingApplier::new();
        let m3 = a3.metadata_applier();

        let topo1 = Arc::new(RwLock::new(ClusterTopology::new()));
        let topo2 = Arc::new(RwLock::new(ClusterTopology::new()));
        let topo3 = Arc::new(RwLock::new(ClusterTopology::new()));

        let rl1 = Arc::new(RaftLoop::new(mr1, t1.clone(), topo1, a1).with_metadata_applier(m1));
        let rl2 = Arc::new(RaftLoop::new(mr2, t2.clone(), topo2, a2).with_metadata_applier(m2));
        let rl3 = Arc::new(RaftLoop::new(mr3, t3.clone(), topo3, a3).with_metadata_applier(m3));

        let (shutdown_tx, _) = tokio::sync::watch::channel(false);

        let rl2_h = rl2.clone();
        let sr2 = shutdown_tx.subscribe();
        tokio::spawn(async move { t2.serve(rl2_h, sr2).await });

        let rl3_h = rl3.clone();
        let sr3 = shutdown_tx.subscribe();
        tokio::spawn(async move { t3.serve(rl3_h, sr3).await });

        let rl1_r = rl1.clone();
        let sr1 = shutdown_tx.subscribe();
        tokio::spawn(async move { rl1_r.run(sr1).await });

        let rl2_r = rl2.clone();
        let sr2r = shutdown_tx.subscribe();
        tokio::spawn(async move { rl2_r.run(sr2r).await });

        let rl3_r = rl3.clone();
        let sr3r = shutdown_tx.subscribe();
        tokio::spawn(async move { rl3_r.run(sr3r).await });

        let rl1_h = rl1.clone();
        let sr1h = shutdown_tx.subscribe();
        tokio::spawn(async move { t1.serve(rl1_h, sr1h).await });

        // Poll until node 1 commits at least the no-op (election done).
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            if rl1.applier.count() >= 1 {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "node 1 should have committed at least the no-op, got {}",
                rl1.applier.count()
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        let (_gid, idx) = rl1.propose(0, b"distributed-cmd".to_vec()).unwrap();
        assert!(idx >= 2);

        // Poll until all nodes replicate the proposed command.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            if rl1.applier.count() >= 2 && rl2.applier.count() >= 1 && rl3.applier.count() >= 1 {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "replication timed out: n1={}, n2={}, n3={}",
                rl1.applier.count(),
                rl2.applier.count(),
                rl3.applier.count()
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        shutdown_tx.send(true).unwrap();
    }
}
