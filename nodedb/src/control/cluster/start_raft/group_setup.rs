// SPDX-License-Identifier: BUSL-1.1

//! Phase 1 of `start_raft`: bootstrap the sequencer Raft group, build the
//! propose tracker + distributed applier + Calvin state, the metadata
//! applier, the plan/array executors and vshard handler, and load the
//! snapshot-transfer / replication-factor config that must be read before
//! `pending_subsystems` is consumed.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::mpsc;

use nodedb_cluster::calvin::{
    CalvinCompletionRegistry, SEQUENCER_GROUP_ID, SequencerStateMachine, TxnId, VerdictOutcome,
};

use crate::control::cluster::array_executor::DataPlaneArrayExecutor;
use crate::control::cluster::handle::ClusterHandle;
use crate::control::cluster::metadata_applier::{MetadataCommitApplier, seed_metadata_cache};
use crate::control::cluster::spsc_applier::SpscCommitApplier;
use crate::control::cluster::vshard_envelope_handler::build_vshard_handler;
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
    let multi_raft = take_multi_raft(handle)?;

    // Build the propose tracker and distributed applier.
    //
    // The tracker is wired with the per-group apply watermark registry. The
    // apply loop bumps it through the tracker once every entry of a group up
    // to an index finished, so proposers and cross-node visibility waits read
    // one in-order "data applied on this node" signal.
    // Its waiter window is the statement deadline: no waiter outlives it, so
    // stored results and committed keys older than it serve nobody.
    let tracker = Arc::new(
        ProposeTracker::new()
            .with_group_watchers(handle.group_watchers.clone())
            .with_waiter_window(std::time::Duration::from_secs(
                shared.tuning.network.default_deadline_secs,
            )),
    );
    let (dist_applier, apply_rx) = create_distributed_applier(tracker.clone());
    let dist_applier = Arc::new(dist_applier);
    let calvin = build_calvin_state(handle, shared);

    // Install the propose tracker so CP dispatch paths can await commit.
    if shared.propose_tracker.set(tracker.clone()).is_err() {
        tracing::warn!("propose_tracker already set — start_raft appears to have run twice");
    }

    let data_applier = SpscCommitApplier::new(
        shared.clone(),
        dist_applier,
        Arc::clone(&calvin.sequencer_state_machine),
    );

    let (metadata_applier, token_state) = build_metadata_applier(handle, shared)?;

    // LocalPlanExecutor is the sole physical-plan execution path.
    let plan_executor = Arc::new(crate::control::LocalPlanExecutor::new(shared.clone()));

    let vshard_handler = build_envelope_handler(handle, shared, data_dir)?;

    let tick_interval = Duration::from_millis(transport_tuning.raft_tick_interval_ms);
    let (snapshot_chunk_bytes, orphan_partial_max_age_secs) =
        load_snapshot_transfer_config(handle)?;
    let replication_factor = load_replication_factor(handle)?;

    let setup = GroupSetup {
        tracker,
        data_applier,
        apply_rx,
        calvin_completion_registry: calvin.completion_registry,
        calvin_verdict_rx: calvin.verdict_rx,
        sequencer_state_machine: calvin.sequencer_state_machine,
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

/// Move the `MultiRaft` constructed by `start_cluster` out of `handle`, and
/// add the sequencer group on a bootstrap or restart node.
///
/// Rebuilding the `MultiRaft` here from the routing table will lose
/// learner membership for joining nodes and will double-open per-group
/// redb log files.
fn take_multi_raft(handle: &ClusterHandle) -> crate::Result<nodedb_cluster::multi_raft::MultiRaft> {
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
    // group with a topology-derived voter set will fork the Raft membership.
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
    Ok(multi_raft)
}

/// The Calvin state phase 1 builds.
struct CalvinState {
    completion_registry: Arc<CalvinCompletionRegistry>,
    verdict_rx: mpsc::Receiver<(TxnId, VerdictOutcome)>,
    sequencer_state_machine: Arc<Mutex<SequencerStateMachine>>,
}

/// Build the Calvin completion registry, its verdict channel, and the
/// sequencer state machine.
fn build_calvin_state(handle: &ClusterHandle, shared: &Arc<SharedState>) -> CalvinState {
    // Verdict signal channel: the registry emits `(txn, commit)` on a complete
    // vote tally; the sequencer service's leader-guarded arm proposes the
    // `Verdict`. Bounded to match the scheduler completion channel capacity.
    let (calvin_verdict_tx, calvin_verdict_rx) = mpsc::channel(512);
    let calvin_completion_registry = CalvinCompletionRegistry::new(calvin_verdict_tx);
    // A coordinator registers its completion waiter within its statement
    // deadline, so a terminal entry still waiterless after two deadlines
    // never gains one, and is evicted with its ack results.
    // A coordinator drains its applied response within the same deadline, so
    // a sidecar deposit still undrained after two deadlines never is.
    let completion_ttl = std::time::Duration::from_secs(
        shared
            .tuning
            .network
            .default_deadline_secs
            .saturating_mul(2),
    );
    calvin_completion_registry.set_waiterless_ttl(completion_ttl);
    shared.calvin.apply_results.set_ttl(completion_ttl);
    // Escalation for an unrecoverable sequencer epoch regression: a NEW
    // committed entry re-minted an epoch this replica already consumed, so the
    // state machine halts rather than alias committed transaction identities.
    //
    // The escalation is scoped to what was actually lost. Sequencing stops —
    // the service sheds queued submissions so Calvin writers fail fast instead
    // of hanging — while reads, metadata, and every non-Calvin write path keep
    // serving. Stopping the process instead will turn a subsystem fault into a
    // full outage (on a single-node deployment, total unavailability), which is
    // a strictly worse failure than the one being escalated, and it destroys the
    // running node an operator needs in order to diagnose the divergence. The
    // marker makes the degradation visible on the health surfaces so this can
    // never pass for a healthy node.
    //
    // Held weakly — `SharedState` reaches this state machine through the
    // compactor closure, and a strong capture here will close that into a
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
        .with_unrecoverable_hook(unrecoverable_hook)
        .with_restore_point_hook(crate::control::pitr::restore_point::sequencer_hook(
            Arc::downgrade(shared),
        ))
        .with_cut_instant_hook(crate::control::state::cut_instant_hook(Arc::downgrade(
            shared,
        ))),
    ));
    CalvinState {
        completion_registry: calvin_completion_registry,
        verdict_rx: calvin_verdict_rx,
        sequencer_state_machine,
    }
}

/// Build the production metadata applier and the join-token mirror it
/// shares, and re-admit every unexpired enrollment preauthorization.
fn build_metadata_applier(
    handle: &ClusterHandle,
    shared: &Arc<SharedState>,
) -> crate::Result<(
    Arc<dyn nodedb_cluster::MetadataApplier>,
    nodedb_cluster::SharedTokenStateMirror,
)> {
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
    // Leases and the cluster version load from their rows before any entry
    // applies. `SharedState::open` already seeded drains, pending DDL records,
    // and the DDL preparation owner.
    seed_metadata_cache(&handle.metadata_cache, shared.credentials.catalog())?;
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
    let metadata_applier: Arc<dyn nodedb_cluster::MetadataApplier> = metadata_applier_concrete;
    Ok((metadata_applier, token_state))
}

/// Build the vshard envelope handler: array shard RPCs and Event-Plane
/// envelopes.
fn build_envelope_handler(
    handle: &ClusterHandle,
    shared: &Arc<SharedState>,
    data_dir: &std::path::Path,
) -> crate::Result<nodedb_cluster::VShardEnvelopeHandler> {
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
    // The dedup store roots under this node's `data_dir` ARGUMENT, not
    // `shared.data_dir`: the same path `build_hooks` and `build_raft_loop`
    // root under. `SharedState`'s field is not that path everywhere a node is
    // constructed, and rooting a redb file at the wrong one makes two nodes in
    // a single process open the same file — the second fails to acquire its
    // lock and the whole node fails to start.
    //
    // The same store is installed on `SharedState`: every committed redo that
    // carries a cross-shard key records it there as it applies. Keys the WAL
    // holds but the store missed (a crash between the two) are restored here,
    // before this node applies or serves anything.
    let cross_shard_dedup = Arc::new(crate::event::cross_shard::CrossShardDedup::open(data_dir)?);
    cross_shard_dedup.restore_from_wal(&shared.wal)?;
    shared
        .cross_shard_dedup
        .set(Arc::clone(&cross_shard_dedup))
        .map_err(|_| crate::Error::Internal {
            detail: "the cross-shard dedup store was already installed".into(),
        })?;
    let cross_shard_receiver = Arc::new(crate::event::cross_shard::CrossShardReceiver::new(
        cross_shard_dedup,
        Arc::clone(shared),
        Arc::new(crate::event::cross_shard::CrossShardMetrics::new()),
        handle.node_id,
    ));
    Ok(build_vshard_handler(array_executor, cross_shard_receiver))
}

/// Read the snapshot-transfer config from the pending subsystem config.
/// It is read before the raft loop is constructed: pending is consumed
/// after the loop.
fn load_snapshot_transfer_config(handle: &ClusterHandle) -> crate::Result<(u64, u64)> {
    let guard = handle
        .pending_subsystems
        .lock()
        .unwrap_or_else(|p| p.into_inner());
    let cfg = guard.as_ref().ok_or_else(|| crate::Error::Config {
        detail: "start_raft called twice: pending_subsystems already consumed".into(),
    })?;
    Ok((
        cfg.config.install_snapshot_chunk_bytes,
        cfg.config.orphan_partial_max_age_secs,
    ))
}

/// Load the replication factor from persisted cluster settings. This
/// function is always called after bootstrap has written those settings —
/// `None` here indicates the node was never bootstrapped, which is an
/// invariant violation (not a recoverable condition).
fn load_replication_factor(handle: &ClusterHandle) -> crate::Result<u32> {
    let replication_factor = match handle.catalog.load_cluster_settings().map_err(|e| {
        crate::Error::Config {
            detail: format!("start_raft: failed to load cluster settings: {e}"),
        }
    })? {
        Some(s) => s.replication_factor,
        None => {
            // Settings not yet persisted on this path — fall back to the
            // in-memory config RF (the same value bootstrap will persist).
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
    Ok(replication_factor)
}
