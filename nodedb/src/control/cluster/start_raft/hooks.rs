// SPDX-License-Identifier: BUSL-1.1

//! Phase 2 of `start_raft`: build the snapshot quarantine hook, the
//! per-group snapshot builder/applier (including boot completion of any
//! interrupted snapshot install), and the cross-node shuffle / surrogate /
//! Calvin routing hooks that bridge `RaftLoop` callbacks to `SharedState`.

use std::sync::Arc;

use tracing::info;

use crate::control::cluster::handle::ClusterHandle;
use crate::control::state::SharedState;

/// Everything phase 2 produces: the hook trait objects `RaftLoop::new`'s
/// builder methods install.
pub(super) struct Hooks {
    pub(super) quarantine_hook:
        Arc<crate::control::cluster::snapshot_hook::RaftSnapshotQuarantineHook>,
    pub(super) snapshot_builder: Arc<dyn nodedb_cluster::SnapshotBuilder>,
    pub(super) snapshot_applier: Arc<dyn nodedb_cluster::SnapshotApplier>,
    pub(super) shuffle_receiver: Arc<dyn nodedb_cluster::ShuffleReceiver>,
    pub(super) shuffle_producer: Arc<dyn nodedb_cluster::ShuffleProducer>,
    pub(super) shuffle_consumer: Arc<dyn nodedb_cluster::ShuffleConsumer>,
    pub(super) shuffle_aggregator: Arc<dyn nodedb_cluster::ShuffleAggregator>,
    pub(super) assign_remote_surrogate: Arc<dyn nodedb_cluster::AssignRemoteSurrogate>,
    pub(super) calvin_submit: Arc<dyn nodedb_cluster::CalvinSubmit>,
    pub(super) calvin_submit_inbox: Arc<dyn nodedb_cluster::CalvinSubmitInbox>,
    pub(super) reserve_read: Arc<dyn nodedb_cluster::ReserveRead>,
    pub(super) release_reservation: Arc<dyn nodedb_cluster::ReleaseReservation>,
    /// The sequencer group's kept snapshot, which its own compaction writes.
    pub(super) sequencer_snapshots:
        Arc<crate::control::cluster::sequencer_snapshot::SequencerSnapshotStore>,
}

/// Build every cross-plane hook `RaftLoop` needs, including the boot
/// completion of interrupted snapshot installs, which must run before
/// `run_apply_loop` is spawned. The recovery is awaited, so the boot runs
/// on any runtime flavor.
pub(super) async fn build_hooks(
    handle: &ClusterHandle,
    shared: &Arc<SharedState>,
    data_dir: &std::path::Path,
    multi_raft: &mut nodedb_cluster::multi_raft::MultiRaft,
    token_state: &nodedb_cluster::SharedTokenStateMirror,
    sequencer_state_machine: &Arc<std::sync::Mutex<nodedb_cluster::calvin::SequencerStateMachine>>,
) -> crate::Result<Hooks> {
    let quarantine_hook = Arc::new(
        crate::control::cluster::snapshot_hook::RaftSnapshotQuarantineHook {
            registry: Arc::clone(&shared.quarantine_registry),
        },
    );

    // The sequencer group's snapshot: its state machine, captured on the
    // send path and kept durably on the receive path. A node whose sequencer
    // log starts right after an installed snapshot restores the state
    // machine from it before any entry applies. A log that starts above its
    // first entry with no usable snapshot marks the history unknown.
    let sequencer_snapshots = Arc::new(
        crate::control::cluster::sequencer_snapshot::SequencerSnapshotStore::new(
            Arc::clone(sequencer_state_machine),
            data_dir,
        ),
    );
    if sequencer_snapshots.restore_at_boot(multi_raft)? {
        info!(
            node_id = handle.node_id,
            "restored the sequencer state machine from its installed snapshot"
        );
    }

    // A data group mounted here takes log entries only once its vShards'
    // Calvin state reaches the sequencer log. Until then its leader sends a
    // snapshot, which carries the Calvin cut its storage holds.
    crate::control::cluster::calvin_snapshot::install_snapshot_requirement(
        shared,
        Arc::clone(&handle.routing),
        multi_raft,
    )?;

    // Per-group snapshot builder for the SEND path: on the leader, build the
    // real serialized engine state for a lagging follower's group vshards
    // (replacing the prior empty stub bytes).
    let snapshot_builder: Arc<dyn nodedb_cluster::SnapshotBuilder> = Arc::new(
        crate::control::cluster::snapshot_builder::DataPlaneSnapshotBuilder::new(shared.clone())
            .with_sequencer(Arc::clone(&sequencer_snapshots)),
    );

    // Per-group snapshot applier for the RECEIVE path: on the follower, apply a
    // received per-group snapshot to the local Data-Plane state machine (via the
    // existing restore handler with replace_mode = true) before Raft advances.
    let snapshot_applier: Arc<dyn nodedb_cluster::SnapshotApplier> = Arc::new(
        crate::control::cluster::snapshot_applier::DataPlaneSnapshotApplier::new(shared.clone())
            .with_metadata(
                Arc::clone(&handle.catalog),
                crate::control::cluster::metadata_image::RaftOwnedState {
                    token_state: Arc::clone(token_state),
                    transport: Some(Arc::clone(&handle.transport)),
                },
            )
            .with_sequencer(Arc::clone(&sequencer_snapshots)),
    );

    // Complete every snapshot install a crash interrupted, before the apply
    // loop starts: an install whose Raft boundary never moved is applied
    // again, then adopted. A finished install is durable on its own and is
    // never applied at boot.
    let completed = nodedb_cluster::install_snapshot::recover_staged_installs(
        data_dir,
        multi_raft,
        Some(snapshot_applier.as_ref()),
    )
    .await
    .map_err(|e| crate::Error::Internal {
        detail: format!("boot recovery of staged snapshot installs: {e}"),
    })?;
    if completed > 0 {
        info!(
            node_id = handle.node_id,
            completed, "completed interrupted snapshot installs"
        );
    }

    // Cross-node streaming-shuffle receiver (E1): bridge the cluster
    // `ShufflePush` read-loop to the in-process registry on `SharedState`.
    let shuffle_receiver: Arc<dyn nodedb_cluster::ShuffleReceiver> = Arc::new(
        crate::control::server::shuffle::RegistryShuffleReceiver::new(Arc::clone(
            &shared.shuffle_registry,
        )),
    );

    // Cross-node shuffle PRODUCER (E4a): runs a local scan through the streaming
    // executor + fan-out sink when a `ShuffleProduce` trigger arrives.
    let shuffle_producer: Arc<dyn nodedb_cluster::ShuffleProducer> =
        Arc::new(crate::control::server::shuffle::RegistryShuffleProducer::new(shared.clone()));

    // Cross-node shuffle CONSUMER (E4b): runs the node-local grace join over the
    // part's staged sides when a `ShuffleConsume` trigger arrives.
    let shuffle_consumer: Arc<dyn nodedb_cluster::ShuffleConsumer> =
        Arc::new(crate::control::server::shuffle::RegistryShuffleConsumer::new(shared.clone()));

    // Cross-node distributed GROUP BY shuffle CONSUMER (E5b): SINGLE-SIDED
    // aggregate sibling of the consumer — merges + finalizes the part's single
    // staged producer side when a `ShuffleAggregateConsume` trigger arrives.
    let shuffle_aggregator: Arc<dyn nodedb_cluster::ShuffleAggregator> =
        Arc::new(crate::control::server::shuffle::RegistryShuffleAggregator::new(shared.clone()));

    // Routed-surrogate-exchange (F1b): when this node is the home vShard's leader
    // for a `(collection, pk)` endpoint key, assign-or-return the authoritative
    // surrogate via a LOCAL `SurrogateAssigner::assign`.
    let assign_remote_surrogate: Arc<dyn nodedb_cluster::AssignRemoteSurrogate> = Arc::new(
        crate::control::server::surrogate_exchange::RegistryAssignRemoteSurrogate::new(
            shared.clone(),
        ),
    );

    // Routed Calvin-submit (Cv1): when this node is the sequencer-group leader,
    // submit a forwarded `TxClass` to the local Calvin sequencer inbox and await
    // its completion. Lets a cross-shard write submitted on a NON-leader
    // coordinator route here and actually commit.
    let calvin_submit: Arc<dyn nodedb_cluster::CalvinSubmit> =
        Arc::new(crate::control::server::calvin_submit::RegistryCalvinSubmit::new(shared.clone()));

    // Routed Calvin-INBOX submit (Cv1): the OLLP dependent sibling of the
    // submit-and-await hook above. When this node is the sequencer-group leader,
    // submit a forwarded dependent `TxClass` to the local Calvin sequencer inbox
    // and return its ASSIGNMENT immediately (without awaiting completion) so a
    // non-leader OLLP coordinator can drive the dependent transaction itself.
    let calvin_submit_inbox: Arc<dyn nodedb_cluster::CalvinSubmitInbox> = Arc::new(
        crate::control::server::calvin_submit::RegistryCalvinSubmitInbox::new(shared.clone()),
    );

    // Routed reserve-read (reservation admission): when this node is the
    // sequencer-group leader, submit a forwarded reserve-read to the local
    // reservation inbox and reply with the assigned owner.
    let reserve_read: Arc<dyn nodedb_cluster::ReserveRead> =
        Arc::new(crate::control::server::reservation::RegistryReserveRead::new(shared.clone()));

    // Routed release-reservation: when this node is the sequencer-group
    // leader, submit a forwarded release to the local reservation inbox.
    let release_reservation: Arc<dyn nodedb_cluster::ReleaseReservation> = Arc::new(
        crate::control::server::reservation::RegistryReleaseReservation::new(shared.clone()),
    );

    Ok(Hooks {
        quarantine_hook,
        snapshot_builder,
        snapshot_applier,
        shuffle_receiver,
        shuffle_producer,
        shuffle_consumer,
        shuffle_aggregator,
        assign_remote_surrogate,
        calvin_submit,
        calvin_submit_inbox,
        reserve_read,
        release_reservation,
        sequencer_snapshots,
    })
}
