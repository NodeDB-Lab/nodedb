// SPDX-License-Identifier: BUSL-1.1

//! Phase 1 of `start_raft`: bootstrap the sequencer Raft group, build the
//! propose tracker + distributed applier + Calvin state, the metadata
//! applier, the plan/array executors and vshard handler, and load the
//! snapshot-transfer / replication-factor config that must be read before
//! `pending_subsystems` is consumed.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::mpsc::{self, Sender};

use nodedb_cluster::calvin::{
    CalvinCompletionRegistry, SEQUENCER_GROUP_ID, SequencerStateMachine, TxnId, VerdictOutcome,
};

use crate::control::cluster::array_executor::DataPlaneArrayExecutor;
use crate::control::cluster::calvin::ReadResultEvent;
use crate::control::cluster::handle::ClusterHandle;
use crate::control::cluster::metadata_applier::MetadataCommitApplier;
use crate::control::cluster::spsc_applier::SpscCommitApplier;
use crate::control::cluster::start_raft_helpers::build_vshard_handler;
use crate::control::distributed_applier::{ApplyBatch, ProposeTracker, create_distributed_applier};
use crate::control::state::SharedState;

/// Everything phase 1 produces that later phases (loop construction,
/// proposer wiring, observability) need.
pub(super) struct GroupSetup {
    pub(super) tracker: Arc<ProposeTracker>,
    pub(super) data_applier: SpscCommitApplier,
    pub(super) apply_rx: mpsc::Receiver<ApplyBatch>,
    pub(super) calvin_completion_registry: Arc<CalvinCompletionRegistry>,
    /// Receiver for verdict signals emitted by `calvin_completion_registry`.
    /// Handed to the `SequencerService` (built in the loop-build phase) so the
    /// leader can propose `SequencerEntry::Verdict` once a cross-shard txn's
    /// vote tally completes.
    pub(super) calvin_verdict_rx: mpsc::Receiver<(TxnId, VerdictOutcome)>,
    pub(super) sequencer_state_machine: Arc<Mutex<SequencerStateMachine>>,
    pub(super) calvin_read_result_senders: Arc<Mutex<BTreeMap<u32, Sender<ReadResultEvent>>>>,
    pub(super) metadata_applier: Arc<dyn nodedb_cluster::MetadataApplier>,
    pub(super) token_state: nodedb_cluster::SharedTokenStateMirror,
    pub(super) plan_executor: Arc<crate::control::LocalPlanExecutor>,
    pub(super) vshard_handler: nodedb_cluster::VShardEnvelopeHandler,
    pub(super) tick_interval: Duration,
    pub(super) snapshot_chunk_bytes: u64,
    pub(super) orphan_partial_max_age_secs: u64,
    pub(super) replication_factor: u32,
}

/// Move the `MultiRaft` out of `handle.multi_raft`, add the sequencer Raft
/// group if this is a bootstrap/restart node, and build every phase-1
/// dependency. Returns the extracted `MultiRaft` (which the loop-build phase
/// moves into `RaftLoop::new`) alongside the rest of the setup.
pub(super) fn build_group_setup(
    handle: &ClusterHandle,
    shared: &Arc<SharedState>,
    data_dir: &std::path::Path,
    transport_tuning: &nodedb_types::config::tuning::ClusterTransportTuning,
) -> crate::Result<(nodedb_cluster::multi_raft::MultiRaft, GroupSetup)> {
    // Move the MultiRaft constructed by `start_cluster` into this
    // function. Rebuilding it here from the routing table would lose
    // learner membership for joining nodes and would double-open
    // per-group redb log files.
    let mut multi_raft = handle
        .multi_raft
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .take()
        .ok_or_else(|| crate::Error::Config {
            detail: "start_raft called twice: cluster multi_raft already consumed".into(),
        })?;

    // Bootstrap/restart nodes create the sequencer group here. A fresh joiner
    // already reconstructed it as a learner from JoinResponse; replacing that
    // group with a topology-derived voter set would fork the Raft membership.
    if !multi_raft.contains_group(SEQUENCER_GROUP_ID) {
        let sequencer_peers: Vec<u64> = {
            let topo = handle.topology.read().unwrap_or_else(|p| p.into_inner());
            topo.all_nodes()
                .filter(|node| node.node_id != handle.node_id && node.state.receives_log())
                .map(|node| node.node_id)
                .collect()
        };
        multi_raft
            .add_group(SEQUENCER_GROUP_ID, sequencer_peers)
            .map_err(|e| crate::Error::Config {
                detail: format!("sequencer raft group add: {e}"),
            })?;
    }

    // Build the propose tracker and distributed applier.
    //
    // The tracker is wired with the per-group apply watermark registry. The
    // apply loop bumps it through the tracker once every entry of a group up
    // to an index finished, so proposers and cross-node visibility waits read
    // one in-order "data applied on this node" signal.
    let tracker =
        Arc::new(ProposeTracker::new().with_group_watchers(handle.group_watchers.clone()));
    let (dist_applier, apply_rx) = create_distributed_applier(tracker.clone());
    let dist_applier = Arc::new(dist_applier);
    // Verdict signal channel: the registry emits `(txn, commit)` on a complete
    // vote tally; the sequencer service's leader-guarded arm proposes the
    // `Verdict`. Bounded to match the scheduler completion channel capacity.
    let (calvin_verdict_tx, calvin_verdict_rx) = mpsc::channel(512);
    let calvin_completion_registry = CalvinCompletionRegistry::new(calvin_verdict_tx);
    // Escalation for an unrecoverable sequencer epoch regression: a NEW
    // committed entry re-minted an epoch this replica already consumed, so the
    // state machine halts rather than alias committed transaction identities.
    //
    // The escalation is scoped to what was actually lost. Sequencing stops —
    // the service sheds queued submissions so Calvin writers fail fast instead
    // of hanging — while reads, metadata, and every non-Calvin write path keep
    // serving. Stopping the process instead would turn a subsystem fault into a
    // full outage (on a single-node deployment, total unavailability), which is
    // a strictly worse failure than the one being escalated, and it destroys the
    // running node an operator needs in order to diagnose the divergence. The
    // marker makes the degradation visible on the health surfaces so this can
    // never pass for a healthy node.
    //
    // Held weakly — `SharedState` reaches this state machine through the
    // compactor closure, and a strong capture here would close that into a
    // reference cycle that pins `SharedState` forever.
    let shared_for_halt = Arc::downgrade(shared);
    let node_id_for_halt = handle.node_id;
    let unrecoverable_hook: nodedb_cluster::calvin::UnrecoverableEpochHook =
        Arc::new(move |halt: nodedb_cluster::calvin::SequencerHalt| {
            tracing::error!(
                node_id = node_id_for_halt,
                expected_epoch = halt.expected_epoch,
                found_epoch = halt.found_epoch,
                txns_in_batch = halt.txns_in_batch,
                raft_index = halt.raft_index,
                "sequencer epoch regression is unrecoverable; this node has stopped sequencing \
                 and is reporting itself degraded. Every non-Calvin path keeps serving; \
                 operator intervention is required to resume sequencing."
            );
            if let Some(shared) = shared_for_halt.upgrade() {
                shared.sequencer_halt.record(halt);
            }
        });
    let sequencer_state_machine = Arc::new(Mutex::new(
        SequencerStateMachine::new(
            std::collections::HashMap::new(),
            Arc::clone(&calvin_completion_registry),
        )
        .with_unrecoverable_hook(unrecoverable_hook),
    ));
    let calvin_read_result_senders =
        Arc::new(Mutex::new(BTreeMap::<u32, Sender<ReadResultEvent>>::new()));

    // Install the propose tracker so CP dispatch paths can await commit.
    if shared.propose_tracker.set(tracker.clone()).is_err() {
        tracing::warn!("propose_tracker already set — start_raft appears to have run twice");
    }

    let data_applier = SpscCommitApplier::new(
        shared.clone(),
        dist_applier,
        Arc::clone(&sequencer_state_machine),
    );

    // Production metadata applier: writes to the shared cache,
    // writes back to the `SystemCatalog` redb so every non-cache
    // reader observes the change, bumps the applied-index watcher,
    // broadcasts `CatalogChangeEvent`, and spawns Data Plane
    // `Register` dispatches on committed `CollectionDdl::Create`.
    let token_state: nodedb_cluster::SharedTokenStateMirror = Arc::new(Mutex::new(
        shared
            .credentials
            .catalog()
            .list_join_token_states()?
            .into_iter()
            .map(|state| (state.token_hash, state))
            .collect(),
    ));
    let metadata_applier_concrete = Arc::new(MetadataCommitApplier::new(
        handle.metadata_cache.clone(),
        shared.catalog_change_tx.clone(),
        shared.credentials.clone(),
        Arc::clone(&token_state),
    ));
    metadata_applier_concrete.install_transport(Arc::clone(&handle.transport));
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(u64::MAX);
    for (spki, expires_at_ms) in shared
        .credentials
        .catalog()
        .list_enrollment_preauthorizations(now_ms)?
    {
        let ttl = std::time::Duration::from_millis(expires_at_ms - now_ms);
        if !handle.transport.preauthorize_peer_identity(spki, ttl) {
            // Keep startup live and admission fail-closed if durable state from
            // an older/corrupt deployment exceeds the current bounded cache.
            tracing::error!(
                ?spki,
                "enrollment preauthorization capacity exhausted during rehydration; identity not admitted"
            );
        }
    }
    // Install the Weak<SharedState> before the raft loop starts
    // ticking so no commit can reach the applier without it.
    metadata_applier_concrete.install_shared(Arc::downgrade(shared));
    let metadata_applier: Arc<dyn nodedb_cluster::MetadataApplier> =
        metadata_applier_concrete.clone();

    // LocalPlanExecutor is the C-β physical-plan execution path (C-δ.6: sole execution path).
    let plan_executor = Arc::new(crate::control::LocalPlanExecutor::new(shared.clone()));

    // Build the real ArrayLocalExecutor that bridges incoming array shard RPCs
    // into the local Data Plane via the SPSC bridge, then the vshard handler
    // that wraps it.
    let array_executor: Arc<dyn nodedb_cluster::distributed_array::ArrayLocalExecutor> =
        Arc::new(DataPlaneArrayExecutor::new(shared.clone()));

    // Receive side for Event-Plane envelopes. Its dependencies are constructed
    // here rather than read from `SharedState`: this is the one site that runs
    // only in cluster mode, where both are unconditionally available, so the
    // handler holds concrete values and has no absent-dependency case to
    // resolve at message time — an inbound NOTIFY can never be silently
    // dropped for want of a store it does not use.
    //
    // The HWM store roots under this node's `data_dir` ARGUMENT, not
    // `shared.data_dir`: the same path `build_hooks` and `build_raft_loop`
    // root under. `SharedState`'s field is not that path everywhere a node is
    // constructed, and rooting a redb file at the wrong one makes two nodes in
    // a single process open the same file — the second fails to acquire its
    // lock and the whole node fails to start.
    let cross_shard_receiver = Arc::new(crate::event::cross_shard::CrossShardReceiver::new(
        Arc::new(crate::event::cross_shard::HwmStore::open(data_dir)?),
        Arc::clone(shared),
        Arc::new(crate::event::cross_shard::CrossShardMetrics::new()),
        handle.node_id,
    ));
    let vshard_handler = build_vshard_handler(array_executor, cross_shard_receiver);

    let tick_interval = Duration::from_millis(transport_tuning.raft_tick_interval_ms);

    // Read snapshot-transfer config from the pending subsystem config before
    // the raft_loop is constructed (pending is consumed after the loop).
    let (snapshot_chunk_bytes, orphan_partial_max_age_secs) = {
        let guard = handle
            .pending_subsystems
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        let cfg = guard.as_ref().ok_or_else(|| crate::Error::Config {
            detail: "start_raft called twice: pending_subsystems already consumed".into(),
        })?;
        (
            cfg.config.install_snapshot_chunk_bytes,
            cfg.config.orphan_partial_max_age_secs,
        )
    };

    // Load the replication factor from persisted cluster settings. This
    // function is always called after bootstrap has written those settings —
    // `None` here indicates the node was never bootstrapped, which is an
    // invariant violation (not a recoverable condition).
    let replication_factor = match handle.catalog.load_cluster_settings().map_err(|e| {
        crate::Error::Config {
            detail: format!("start_raft: failed to load cluster settings: {e}"),
        }
    })? {
        Some(s) => s.replication_factor,
        None => {
            // Settings not yet persisted on this path — fall back to the
            // in-memory config RF (the same value bootstrap would persist).
            // Error only if neither source is available.
            handle
                .pending_subsystems
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .as_ref()
                .map(|p| p.config.replication_factor as u32)
                .ok_or_else(|| crate::Error::Config {
                    detail: "start_raft: no replication factor available (catalog and config both absent)".to_string(),
                })?
        }
    };

    let setup = GroupSetup {
        tracker,
        data_applier,
        apply_rx,
        calvin_completion_registry,
        calvin_verdict_rx,
        sequencer_state_machine,
        calvin_read_result_senders,
        metadata_applier,
        token_state,
        plan_executor,
        vshard_handler,
        tick_interval,
        snapshot_chunk_bytes,
        orphan_partial_max_age_secs,
        replication_factor,
    };

    Ok((multi_raft, setup))
}
