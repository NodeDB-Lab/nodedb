// SPDX-License-Identifier: BUSL-1.1

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

use crate::applied_watcher::GroupAppliedWatchers;
use crate::catalog::ClusterCatalog;
use crate::error::Result;
use crate::forward::{NoopPlanExecutor, PlanExecutor};
use crate::loop_metrics::LoopMetrics;
use crate::metadata_group::applier::{MetadataApplier, NoopMetadataApplier};
use crate::metadata_group::cache::MetadataCache;
use crate::multi_raft::MultiRaft;
use crate::topology::ClusterTopology;
use crate::transport::NexarTransport;

use super::hooks::{
    AssignRemoteSurrogate, CalvinSubmit, CalvinSubmitInbox, ReleaseReservation, ReserveRead,
    ShuffleAggregator, ShuffleConsumer, ShuffleProducer, ShuffleReceiver, SnapshotApplier,
    SnapshotBuilder, SnapshotQuarantineHook,
};

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
    /// Per-Raft-group apply watermark watchers. Bumped from
    /// [`super::tick::do_tick`] after applies and from
    /// [`super::handle_rpc`] after snapshot installs. The host crate
    /// shares this Arc with `SharedState` so proposers, lease
    /// renewals, and consistent reads can wait on the apply
    /// watermark of the *specific* group whose proposal they made.
    pub(super) group_watchers: Arc<GroupAppliedWatchers>,
    /// Tracks whether this node was the metadata-group leader on the
    /// previous tick. Used to detect false→true edges so the cluster
    /// epoch (see [`crate::cluster_epoch`]) can be bumped exactly once
    /// per leadership acquisition. `AtomicBool` because [`super::tick::do_tick`]
    /// runs against `&self`.
    pub(super) prev_metadata_leader: std::sync::atomic::AtomicBool,
    /// This node's cluster-epoch state, shared with the transport's
    /// `AuthContext` so outbound frames stamp the generation this node has
    /// applied. Advanced by the applier when a `ClusterEpochBump` commits.
    pub(crate) cluster_epoch: std::sync::Arc<crate::cluster_epoch::ClusterEpochState>,

    /// Optional quarantine hook for the snapshot receive path.
    ///
    /// When set (by the `nodedb` binary via `with_snapshot_quarantine_hook`),
    /// the `InstallSnapshotRequest` handler checks whether the incoming chunk
    /// is already quarantined, records successes to clear transient strikes, and
    /// records consecutive failures to quarantine persistently.
    ///
    /// Cluster-only tests leave this as `None`, which disables all quarantine
    /// accounting for snapshot chunks.
    pub(super) snapshot_quarantine_hook: Option<Arc<dyn SnapshotQuarantineHook>>,

    /// Optional cross-node streaming-shuffle receiver (E1).
    ///
    /// When set (by the `nodedb` binary via `with_shuffle_receiver`), the
    /// `ShufflePush` transport read-loop deposits chunks and records barrier
    /// completion through it. Cluster-only tests leave this `None`, which makes
    /// any incoming `ShufflePush` stream return a typed "not configured" error.
    pub(super) shuffle_receiver: Option<Arc<dyn ShuffleReceiver>>,

    /// Optional cross-node shuffle PRODUCER (E4a).
    ///
    /// When set (by the `nodedb` binary via `with_shuffle_producer`), an incoming
    /// `ShuffleProduceRequest` runs the local scan + hash-partition fan-out
    /// through it. Cluster-only tests leave this `None`, which makes any incoming
    /// `ShuffleProduce` request return a typed "not configured" error.
    pub(super) shuffle_producer: Option<Arc<dyn ShuffleProducer>>,

    /// Optional cross-node shuffle CONSUMER (E4b).
    ///
    /// When set (by the `nodedb` binary via `with_shuffle_consumer`), an incoming
    /// `ShuffleConsumeRequest` runs the node-local grace join over the part's
    /// staged sides through it. Cluster-only tests leave this `None`, which makes
    /// any incoming `ShuffleConsume` request return a typed "not configured"
    /// error.
    pub(super) shuffle_consumer: Option<Arc<dyn ShuffleConsumer>>,

    /// Optional cross-node distributed GROUP BY shuffle CONSUMER (E5b).
    ///
    /// SINGLE-SIDED aggregate sibling of [`shuffle_consumer`](Self::shuffle_consumer).
    /// When set (by the `nodedb` binary via `with_shuffle_aggregator`), an
    /// incoming `ShuffleAggregateConsumeRequest` runs the node-local partial-state
    /// merge + finalize over the part's single staged side through it.
    /// Cluster-only tests leave this `None`, which makes any incoming
    /// `ShuffleAggregateConsume` request return a typed "not configured" error.
    pub(super) shuffle_aggregator: Option<Arc<dyn ShuffleAggregator>>,

    /// Optional routed-surrogate-exchange assigner (F1b).
    ///
    /// When set (by the `nodedb` binary via `with_assign_remote_surrogate`), an
    /// incoming `AssignSurrogateRequest` runs a LOCAL `SurrogateAssigner::assign`
    /// for the endpoint key through it (this node IS the home vShard leader, so a
    /// local assign yields the authoritative value). Cluster-only tests leave
    /// this `None`, which makes any incoming `AssignSurrogate` request return a
    /// typed "not configured" error.
    pub(super) assign_remote_surrogate: Option<Arc<dyn AssignRemoteSurrogate>>,

    /// Optional routed Calvin-submit hook (Cv1).
    ///
    /// When set (by the `nodedb` binary via `with_calvin_submit`), an incoming
    /// `SubmitCalvinTxnRequest` submits the carried `TxClass` to THIS node's
    /// Calvin sequencer inbox and awaits completion through it (this node IS the
    /// sequencer-group leader, so the submit is actually sequenced and acked
    /// here). Cluster-only tests leave this `None`, which makes any incoming
    /// `SubmitCalvinTxn` request return a typed "not configured" error.
    pub(super) calvin_submit: Option<Arc<dyn CalvinSubmit>>,

    /// Optional routed Calvin-INBOX submit hook (Cv1).
    ///
    /// OLLP dependent sibling of [`calvin_submit`](Self::calvin_submit). When set
    /// (by the `nodedb` binary via `with_calvin_submit_inbox`), an incoming
    /// `SubmitCalvinInboxRequest` submits the carried `TxClass` to THIS node's
    /// Calvin sequencer inbox and awaits only the ASSIGNMENT (not completion)
    /// through it. Cluster-only tests leave this `None`, which makes any incoming
    /// `SubmitCalvinInbox` request return a typed "not configured" error.
    pub(super) calvin_submit_inbox: Option<Arc<dyn CalvinSubmitInbox>>,

    /// Optional routed reserve-read hook (Calvin OLLP).
    ///
    /// When set (by the `nodedb` binary via `with_reserve_read`), an incoming
    /// `ReserveReadRequest` assign-only reserves the read lock for the carried
    /// `LockKey` through it (this node IS the sequencer-group leader, so the
    /// reserve is enforced against the authoritative lock table). Cluster-only
    /// tests leave this `None`, which makes any incoming `ReserveRead` request
    /// return a typed "not configured" error.
    pub(super) reserve_read: Option<Arc<dyn ReserveRead>>,

    /// Optional routed release-reservation hook (Calvin OLLP).
    ///
    /// Ack-only sibling of [`reserve_read`](Self::reserve_read). When set (by
    /// the `nodedb` binary via `with_release_reservation`), an incoming
    /// `ReleaseReservationRequest` releases the reservation held by the
    /// carried owner through it. Cluster-only tests leave this `None`, which
    /// makes any incoming `ReleaseReservation` request return a typed "not
    /// configured" error.
    pub(super) release_reservation: Option<Arc<dyn ReleaseReservation>>,

    /// Optional per-group snapshot builder for the SEND path.
    ///
    /// When set (by the `nodedb` binary via `with_snapshot_builder`), the
    /// install-snapshot dispatch in [`super::tick`] calls it to produce the real
    /// serialized engine state for the lagging follower's group vshards before
    /// framing the chunked `InstallSnapshot` RPC. Cluster-only tests leave this
    /// `None`, which makes the sender fall back to the stub (empty) chunk.
    pub(super) snapshot_builder: Option<Arc<dyn SnapshotBuilder>>,

    /// Optional per-group snapshot applier for the RECEIVE path.
    ///
    /// When set (by the `nodedb` binary via `with_snapshot_applier`), the
    /// install-snapshot finalize path applies the received per-group snapshot to
    /// the local Data-Plane state machine AFTER the atomic `.partial`→`.snap`
    /// rename and BEFORE advancing Raft. Cluster-only tests leave this `None`,
    /// which makes the follower advance Raft without restoring engine state
    /// (correct for the empty bootstrap stub shipped by those tests).
    pub(super) snapshot_applier: Option<Arc<dyn SnapshotApplier>>,

    /// In-progress partial snapshot receives, keyed by `group_id`.
    ///
    /// Each entry tracks the `.partial` file, running CRC, and expected next
    /// byte offset for a follower that is currently receiving a chunked snapshot.
    /// Entries are created on `offset == 0`, updated on each chunk, and removed
    /// on finalization or offset regression.
    pub(super) partial_snapshots: Arc<crate::install_snapshot::PartialSnapshotMap>,

    /// Data directory for persistent partial-snapshot files and the
    /// `recv_snapshots/` subdirectory. `None` in tests that don't exercise
    /// the disk path.
    pub(super) data_dir: Option<std::path::PathBuf>,

    /// Snapshot chunk size for the sender path (bytes).
    pub(super) snapshot_chunk_bytes: u64,

    /// Orphan partial-snapshot max age for the GC sweeper (seconds).
    pub(super) orphan_partial_max_age_secs: u64,

    /// Cluster replication factor (target voters per group), loaded once from
    /// `ClusterSettings` at startup; immutable for the loop's lifetime. Used
    /// to cap voter promotion at min(RF, N). Defaults to 1 (single-node, no
    /// extra replicas) when not overridden via `with_replication_factor`.
    pub(super) replication_factor: u32,

    /// Monotonic tick counter; throttles periodic maintenance (placement
    /// reconcile) to a coarse cadence off the 10ms tick. `AtomicU64` because
    /// [`super::tick::do_tick`] runs against `&self`.
    pub(super) tick_count: std::sync::atomic::AtomicU64,

    /// Notification channel for kicking placement reconcile immediately
    /// when a node joins, instead of waiting up to ~1 s for the throttled
    /// tick. Fired from [`super::join`] on the success path; consumed by
    /// an extra `select!` arm in [`Self::run`].
    pub(super) reconcile_notify: tokio::sync::Notify,

    /// Optional handle to the metadata group's `MetadataCache` (the same
    /// `Arc` the `CacheApplier` writes into). When set, the leader's
    /// periodic lease-GC sweep can read committed lease state directly.
    /// `None` in cluster-only tests that don't wire it.
    pub(super) metadata_cache: Option<Arc<RwLock<MetadataCache>>>,

    /// Optional lease-holder liveness fed by SWIM verdicts. When set, the
    /// periodic lease-GC sweep releases a holder's leases as soon as SWIM
    /// marks it Dead/Left, instead of waiting for topology removal or lease
    /// expiry. `None` in cluster-only tests that don't wire it.
    pub(super) lease_liveness: Option<Arc<crate::lease_liveness::LeaseHolderLiveness>>,
}

impl<A: CommitApplier> RaftLoop<A> {
    pub fn new(
        multi_raft: MultiRaft,
        transport: Arc<NexarTransport>,
        topology: Arc<RwLock<ClusterTopology>>,
        applier: A,
    ) -> Self {
        let node_id = multi_raft.node_id();
        // Share the transport's epoch state rather than making a second one:
        // the generation this node applies and the generation it stamps must
        // be the same fact.
        let cluster_epoch = transport.cluster_epoch();
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
            group_watchers: Arc::new(GroupAppliedWatchers::new()),
            prev_metadata_leader: std::sync::atomic::AtomicBool::new(false),
            cluster_epoch,
            snapshot_quarantine_hook: None,
            shuffle_receiver: None,
            shuffle_producer: None,
            shuffle_consumer: None,
            shuffle_aggregator: None,
            assign_remote_surrogate: None,
            calvin_submit: None,
            calvin_submit_inbox: None,
            reserve_read: None,
            release_reservation: None,
            snapshot_builder: None,
            snapshot_applier: None,
            partial_snapshots: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
            data_dir: None,
            snapshot_chunk_bytes: 4 * 1024 * 1024,
            orphan_partial_max_age_secs: 300,
            replication_factor: 1,
            tick_count: std::sync::atomic::AtomicU64::new(0),
            reconcile_notify: tokio::sync::Notify::new(),
            metadata_cache: None,
            lease_liveness: None,
        }
    }
}

impl<A: CommitApplier, P: PlanExecutor> RaftLoop<A, P> {
    /// Install the snapshot quarantine hook (mutable setter variant).
    ///
    /// Prefer `with_snapshot_quarantine_hook` on the builder chain unless you
    /// need to set the hook after construction.
    pub fn set_snapshot_quarantine_hook(&mut self, hook: Arc<dyn SnapshotQuarantineHook>) {
        self.snapshot_quarantine_hook = Some(hook);
    }

    /// Shared handle to the per-group apply watcher registry.
    pub fn group_watchers(&self) -> Arc<GroupAppliedWatchers> {
        Arc::clone(&self.group_watchers)
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

    /// This node's id (exposed for handlers and tests).
    pub fn node_id(&self) -> u64 {
        self.node_id
    }

    /// Target replication factor for this cluster (voters per group).
    ///
    /// Loaded once from `ClusterSettings` at startup and immutable for the
    /// loop's lifetime. Returns `1` when no override was set (single-node
    /// default).
    pub fn replication_factor(&self) -> u32 {
        self.replication_factor
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

        // Startup GC sweep: remove orphaned partial-snapshot files from
        // previous runs that did not complete.
        if let Some(ref dir) = self.data_dir {
            match crate::install_snapshot::gc::sweep_orphans(dir, self.orphan_partial_max_age_secs)
            {
                Ok((removed, errs)) => {
                    if removed > 0 {
                        tracing::info!(removed, "startup: removed orphaned partial snapshot files");
                    }
                    for e in errs {
                        tracing::warn!(error = %e, "startup: partial snapshot GC error");
                    }
                }
                Err(e) => {
                    tracing::warn!(error = %e, "startup: failed to sweep partial snapshot directory");
                }
            }
        }

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let started = Instant::now();
                    self.do_tick();
                    self.loop_metrics.observe(started.elapsed());
                }
                _ = self.reconcile_notify.notified() => {
                    if *shutdown.borrow() {
                        return;
                    }
                    self.reconcile_placement();
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

    /// Verify that `with_replication_factor` stores the supplied value and that
    /// the default (no builder call) is 1.
    #[tokio::test]
    async fn replication_factor_accessor() {
        let dir = tempfile::tempdir().unwrap();
        let transport = make_transport(1);
        let rt = RoutingTable::uniform(1, &[1], 1);
        let mut mr = MultiRaft::new(1, rt, dir.path().to_path_buf());
        mr.add_group(0, vec![]).unwrap();
        mr.add_group(1, vec![]).unwrap();

        let topo = Arc::new(RwLock::new(ClusterTopology::new()));

        // Default: 1 (single-node sentinel, no builder call).
        let loop_default = RaftLoop::new(
            MultiRaft::new(
                1,
                RoutingTable::uniform(1, &[1], 1),
                tempfile::tempdir().unwrap().path().to_path_buf(),
            ),
            make_transport(1),
            topo.clone(),
            CountingApplier::new(),
        );
        assert_eq!(loop_default.replication_factor(), 1);

        // Explicit override via builder.
        let loop_rf3 =
            RaftLoop::new(mr, transport, topo, CountingApplier::new()).with_replication_factor(3);
        assert_eq!(loop_rf3.replication_factor(), 3);
    }

    #[tokio::test]
    async fn single_node_raft_loop_commits() {
        let dir = tempfile::tempdir().unwrap();
        let transport = make_transport(1);
        // uniform(1, ...) creates metadata group 0 + data group 1.
        let rt = RoutingTable::uniform(1, &[1], 1);
        let mut mr = MultiRaft::new(1, rt, dir.path().to_path_buf());
        // Add both the metadata group (0) and the data group (1).
        mr.add_group(0, vec![]).unwrap();
        mr.add_group(1, vec![]).unwrap();

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

        // vshard 0 maps to data group 1 (not metadata group 0).
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

        // uniform(1, ...) creates metadata group 0 + data group 1.
        // Both are added to every MultiRaft so vshard proposals (group 1)
        // and metadata proposals (group 0) both work.
        let rt = RoutingTable::uniform(1, &[1, 2, 3], 3);

        let dir1 = tempfile::tempdir().unwrap();
        let mut mr1 = MultiRaft::new(1, rt.clone(), dir1.path().to_path_buf());
        mr1.add_group(0, vec![2, 3]).unwrap();
        mr1.add_group(1, vec![2, 3]).unwrap();
        for node in mr1.groups_mut().values_mut() {
            node.election_deadline_override(Instant::now() - Duration::from_millis(1));
        }

        let transport_tuning = ClusterTransportTuning::default();
        let election_timeout_min =
            Duration::from_millis(transport_tuning.effective_election_timeout_min_ms());
        let election_timeout_max =
            Duration::from_millis(transport_tuning.effective_election_timeout_max_ms());

        let dir2 = tempfile::tempdir().unwrap();
        let mut mr2 = MultiRaft::new(2, rt.clone(), dir2.path().to_path_buf())
            .with_election_timeout(election_timeout_min, election_timeout_max);
        mr2.add_group(0, vec![1, 3]).unwrap();
        mr2.add_group(1, vec![1, 3]).unwrap();

        let dir3 = tempfile::tempdir().unwrap();
        let mut mr3 = MultiRaft::new(3, rt.clone(), dir3.path().to_path_buf())
            .with_election_timeout(election_timeout_min, election_timeout_max);
        mr3.add_group(0, vec![1, 2]).unwrap();
        mr3.add_group(1, vec![1, 2]).unwrap();

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
