// SPDX-License-Identifier: BUSL-1.1

//! Builder-chain methods for [`RaftLoop`].
//!
//! Every `with_*` method is a consumed-self builder that sets one optional
//! field and returns `Self` (or a new `RaftLoop` with a different generic when
//! the type changes, as with `with_plan_executor`).  They live here rather
//! than in [`super::loop_core`] so that the struct definition, constructor,
//! and runtime methods stay under the file-size limit.

use std::sync::{Arc, RwLock};
use std::time::Duration;

use crate::applied_watcher::GroupAppliedWatchers;
use crate::catalog::ClusterCatalog;
use crate::forward::PlanExecutor;
use crate::metadata_group::applier::MetadataApplier;
use crate::metadata_group::cache::MetadataCache;

use super::hooks::{
    AssignRemoteSurrogate, CalvinSubmit, CalvinSubmitInbox, ReleaseReservation, ReserveRead,
    ShuffleAggregator, ShuffleConsumer, ShuffleProducer, ShuffleReceiver, SnapshotApplier,
    SnapshotBuilder, SnapshotQuarantineHook,
};
use super::loop_core::{CommitApplier, RaftLoop, VShardEnvelopeHandler};

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
            group_watchers: self.group_watchers,
            prev_metadata_leader: self.prev_metadata_leader,
            cluster_epoch: self.cluster_epoch,
            snapshot_quarantine_hook: self.snapshot_quarantine_hook,
            shuffle_receiver: self.shuffle_receiver,
            shuffle_producer: self.shuffle_producer,
            shuffle_consumer: self.shuffle_consumer,
            shuffle_aggregator: self.shuffle_aggregator,
            assign_remote_surrogate: self.assign_remote_surrogate,
            calvin_submit: self.calvin_submit,
            calvin_submit_inbox: self.calvin_submit_inbox,
            reserve_read: self.reserve_read,
            release_reservation: self.release_reservation,
            snapshot_builder: self.snapshot_builder,
            snapshot_applier: self.snapshot_applier,
            partial_snapshots: self.partial_snapshots,
            data_dir: self.data_dir,
            snapshot_chunk_bytes: self.snapshot_chunk_bytes,
            orphan_partial_max_age_secs: self.orphan_partial_max_age_secs,
            replication_factor: self.replication_factor,
            // `with_plan_executor` is a construction-time builder, called before
            // `run()` ever ticks — `tick_count` is still 0, so reset is exact.
            tick_count: std::sync::atomic::AtomicU64::new(0),
            // Construction-time builder, before `run()` and any join kick — a
            // fresh `Notify` has no pending permit, so this loses nothing.
            reconcile_notify: tokio::sync::Notify::new(),
            metadata_cache: self.metadata_cache,
            lease_liveness: self.lease_liveness,
        }
    }

    /// Replace the per-group apply watcher registry.
    ///
    /// The host crate calls this with the same `Arc` it stores on
    /// `SharedState` so proposers and consistent-read paths share
    /// one registry with the tick loop's bump points. Defaults to a
    /// fresh empty registry when not set.
    pub fn with_group_watchers(mut self, watchers: Arc<GroupAppliedWatchers>) -> Self {
        self.group_watchers = watchers;
        self
    }

    /// Attach the snapshot quarantine hook (builder chain variant).
    ///
    /// The supplied implementation is called by the `InstallSnapshotRequest`
    /// handler to check for, record, and short-circuit quarantined chunks.
    pub fn with_snapshot_quarantine_hook(mut self, hook: Arc<dyn SnapshotQuarantineHook>) -> Self {
        self.snapshot_quarantine_hook = Some(hook);
        self
    }

    /// Attach the cross-node streaming-shuffle receiver (E1, builder chain).
    ///
    /// The supplied implementation (backed by `nodedb`'s
    /// `ShuffleReceiverRegistry`) is called by the `ShufflePush` transport
    /// read-loop to create inboxes, deposit chunks, and record the per-part
    /// build barrier.
    pub fn with_shuffle_receiver(mut self, receiver: Arc<dyn ShuffleReceiver>) -> Self {
        self.shuffle_receiver = Some(receiver);
        self
    }

    /// Attach the cross-node shuffle PRODUCER (E4a, builder chain).
    ///
    /// The supplied implementation (backed by `nodedb`'s local streaming executor
    /// and receiver registry) is called by the `ShuffleProduce` transport handler
    /// to run the local scan, hash-partition its rows, and fan them out to the
    /// part-owners.
    pub fn with_shuffle_producer(mut self, producer: Arc<dyn ShuffleProducer>) -> Self {
        self.shuffle_producer = Some(producer);
        self
    }

    /// Attach the cross-node shuffle CONSUMER (E4b, builder chain).
    ///
    /// The supplied implementation (backed by `nodedb`'s shuffle registry +
    /// local executor) is called by the `ShuffleConsume` transport handler to
    /// wait for the part's staged sides to finalize and run the node-local grace
    /// join.
    pub fn with_shuffle_consumer(mut self, consumer: Arc<dyn ShuffleConsumer>) -> Self {
        self.shuffle_consumer = Some(consumer);
        self
    }

    /// Attach the cross-node distributed GROUP BY shuffle CONSUMER (E5b, builder
    /// chain).
    ///
    /// SINGLE-SIDED aggregate sibling of [`with_shuffle_consumer`](Self::with_shuffle_consumer).
    /// The supplied implementation (backed by `nodedb`'s shuffle registry + local
    /// executor) is called by the `ShuffleAggregateConsume` transport handler to
    /// wait for the part's single staged side to finalize and run the node-local
    /// partial-state merge + finalize.
    pub fn with_shuffle_aggregator(mut self, aggregator: Arc<dyn ShuffleAggregator>) -> Self {
        self.shuffle_aggregator = Some(aggregator);
        self
    }

    /// Attach the routed-surrogate-exchange assigner (F1b, builder chain).
    ///
    /// The supplied implementation (backed by `nodedb`'s `SurrogateAssigner`) is
    /// called by the `AssignSurrogate` transport handler when this node is the
    /// home vShard's leader: it runs a LOCAL assign for the endpoint key, which
    /// yields the authoritative (first-wins, idempotent) surrogate the coordinator
    /// must use.
    pub fn with_assign_remote_surrogate(
        mut self,
        assigner: Arc<dyn AssignRemoteSurrogate>,
    ) -> Self {
        self.assign_remote_surrogate = Some(assigner);
        self
    }

    /// Attach the routed Calvin-submit hook (Cv1, builder chain).
    ///
    /// The supplied implementation (backed by `nodedb`'s Calvin sequencer inbox
    /// and `CalvinCompletionRegistry`) is called by the `SubmitCalvinTxn`
    /// transport handler when this node is the sequencer-group leader: it submits
    /// the carried `TxClass` to the local inbox and awaits completion, so a
    /// cross-shard write routed here from a non-leader coordinator actually
    /// commits.
    pub fn with_calvin_submit(mut self, submit: Arc<dyn CalvinSubmit>) -> Self {
        self.calvin_submit = Some(submit);
        self
    }

    /// Attach the routed Calvin-INBOX submit hook (Cv1, builder chain).
    ///
    /// OLLP dependent sibling of [`with_calvin_submit`](Self::with_calvin_submit).
    /// The supplied implementation (backed by `nodedb`'s Calvin sequencer inbox
    /// and `CalvinCompletionRegistry`) is called by the `SubmitCalvinInbox`
    /// transport handler when this node is the sequencer-group leader: it submits
    /// the carried dependent `TxClass` to the local inbox and awaits only the
    /// assignment, so a cross-shard write routed here from a non-leader
    /// coordinator gets its assignment back immediately (the OLLP coordinator
    /// drives it to completion itself).
    pub fn with_calvin_submit_inbox(mut self, submit: Arc<dyn CalvinSubmitInbox>) -> Self {
        self.calvin_submit_inbox = Some(submit);
        self
    }

    /// Attach the routed reserve-read hook (Calvin OLLP, builder chain).
    ///
    /// The supplied implementation (backed by `nodedb`'s Calvin sequencer
    /// scheduler) is called by the `ReserveRead` transport handler when this
    /// node is the sequencer-group leader: it decodes the carried `LockKey`
    /// and assign-only reserves the read lock against the local lock table.
    pub fn with_reserve_read(mut self, hook: Arc<dyn ReserveRead>) -> Self {
        self.reserve_read = Some(hook);
        self
    }

    /// Attach the routed release-reservation hook (Calvin OLLP, builder
    /// chain).
    ///
    /// Ack-only sibling of [`with_reserve_read`](Self::with_reserve_read). The
    /// supplied implementation (backed by `nodedb`'s Calvin sequencer
    /// scheduler) is called by the `ReleaseReservation` transport handler when
    /// this node is the sequencer-group leader: it decodes the carried owner
    /// and release reason and releases the reservation.
    pub fn with_release_reservation(mut self, hook: Arc<dyn ReleaseReservation>) -> Self {
        self.release_reservation = Some(hook);
        self
    }

    /// Attach the per-group snapshot builder for the SEND path (builder chain).
    ///
    /// The supplied implementation (backed by `nodedb`'s Data Plane snapshot
    /// dispatch) is called by the install-snapshot tick step to produce the real
    /// serialized engine state for a lagging follower's group vshards. When not
    /// set, the sender falls back to the stub (empty) chunk.
    pub fn with_snapshot_builder(mut self, builder: Arc<dyn SnapshotBuilder>) -> Self {
        self.snapshot_builder = Some(builder);
        self
    }

    /// Attach the per-group snapshot applier for the RECEIVE path (builder chain).
    ///
    /// The supplied implementation (backed by `nodedb`'s Data Plane restore
    /// dispatch) is called by the install-snapshot finalize path to apply a
    /// received per-group snapshot to the local state machine after the atomic
    /// `.partial`→`.snap` rename and before advancing Raft. When not set, the
    /// follower advances Raft without restoring engine state.
    pub fn with_snapshot_applier(mut self, applier: Arc<dyn SnapshotApplier>) -> Self {
        self.snapshot_applier = Some(applier);
        self
    }

    /// Set the data directory for partial-snapshot persistence and GC.
    ///
    /// When set, the `InstallSnapshotRequest` handler writes chunks to
    /// `<data_dir>/recv_snapshots/<group_id>.partial` and the GC sweeper
    /// removes stale partials on startup. When `None` (the default, used by
    /// unit tests), disk writes are skipped — the receiver operates in-memory
    /// only with empty chunk data.
    pub fn with_data_dir(mut self, data_dir: std::path::PathBuf) -> Self {
        self.data_dir = Some(data_dir);
        self
    }

    /// Override the snapshot chunk byte size (default: 4 MiB).
    pub fn with_snapshot_chunk_bytes(mut self, chunk_bytes: u64) -> Self {
        self.snapshot_chunk_bytes = chunk_bytes;
        self
    }

    /// Override the orphan-partial max age for the GC sweeper (default: 300 s).
    pub fn with_orphan_partial_max_age_secs(mut self, secs: u64) -> Self {
        self.orphan_partial_max_age_secs = secs;
        self
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

    /// Wire the metadata cache used by the periodic lease-GC sweep. The
    /// host passes the same `Arc` the production metadata applier holds.
    pub fn with_metadata_cache(mut self, cache: Arc<RwLock<MetadataCache>>) -> Self {
        self.metadata_cache = Some(cache);
        self
    }

    /// Wire the lease-holder liveness set fed by SWIM verdicts. The host
    /// passes the same `Arc` the `LeaseHolderLivenessHook` subscriber writes
    /// into, so the periodic lease-GC sweep releases a holder's leases as
    /// soon as its verdict is Dead/Left.
    pub fn with_lease_liveness(
        mut self,
        liveness: Arc<crate::lease_liveness::LeaseHolderLiveness>,
    ) -> Self {
        self.lease_liveness = Some(liveness);
        self
    }

    pub fn with_tick_interval(mut self, interval: Duration) -> Self {
        self.tick_interval = interval;
        self
    }

    /// Set the cluster replication factor (target voters per group).
    ///
    /// Pass the value loaded from `ClusterSettings::replication_factor` at
    /// startup. The field is immutable after the loop is wrapped in `Arc` —
    /// call this on the builder chain before that wrap.
    pub fn with_replication_factor(mut self, rf: u32) -> Self {
        self.replication_factor = rf;
        self
    }

    /// Attach a cluster catalog — used by the join flow to persist the
    /// updated topology + routing after a conf-change commits.
    pub fn with_catalog(mut self, catalog: Arc<ClusterCatalog>) -> Self {
        // Seed the local cluster-epoch high-water mark from the catalog
        // on attach, so the value emitted in the very first outbound
        // RPC reflects whatever the previous incarnation persisted. A
        // failure to load is treated as a fresh catalog (epoch 0); a
        // genuine catalog read error would have surfaced from earlier
        // catalog operations on this same handle.
        match crate::cluster_epoch::load_persisted_epoch(&catalog) {
            Ok(epoch) => self.cluster_epoch.advance_applied(epoch),
            Err(e) => {
                tracing::warn!(error = %e, "could not load the persisted cluster epoch; starting at 0")
            }
        }
        self.catalog = Some(catalog);
        self
    }
}
