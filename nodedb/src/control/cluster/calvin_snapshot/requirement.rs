// SPDX-License-Identifier: BUSL-1.1

//! Which data-group replicas need a snapshot for their Calvin state, and
//! which vShard schedulers can start.
//!
//! A replica's state of a vShard is its data group's log plus every Calvin
//! input the sequencer sequenced for the vShard. Calvin writes barely grow a
//! data group's log, so the group can never compact, and a new replica will
//! catch up by log replay alone. Its scheduler then catches up from the
//! first index the sequencer log still holds, and every input below that
//! index is lost on the replica. So:
//!
//! - A data group mounted here whose vShards have no base that reaches the
//!   sequencer log refuses log entries, and its leader sends a snapshot
//!   instead (see [`install_snapshot_requirement`]). The snapshot carries
//!   the Calvin cut its state holds.
//! - A vShard's scheduler starts only from a base that reaches the
//!   sequencer log, and then keeps the base (see [`may_start`]).
//! - The sequencer log keeps the range a waiting vShard will replay (see
//!   [`crate::control::state::CalvinBases::replay_floor`]).
//!
//! A data group whose chunked redo streams this node dropped when it left
//! the group requires a snapshot too, until one installs (see
//! `wal_replication::transaction_redo::chunks::owed`).

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex, RwLock};

use nodedb_cluster::multi_raft::MultiRaft;

use crate::control::security::catalog::calvin_base::CalvinBase;
use crate::control::state::SharedState;

/// Load this node's Calvin bases and owed redo snapshots, and install the
/// data-group snapshot requirement on `multi_raft`, before its data groups
/// take entries.
pub fn install_snapshot_requirement(
    shared: &Arc<SharedState>,
    routing: Arc<RwLock<nodedb_cluster::RoutingTable>>,
    multi_raft: &mut MultiRaft,
) -> crate::Result<()> {
    let catalog = shared.credentials.catalog();
    shared.calvin.bases.load(
        catalog.load_calvin_bases()?,
        catalog.load_calvin_sequencer_install()?,
    );
    shared
        .redo_chunks
        .load_owed(catalog.load_redo_snapshot_owed()?);
    let weak = Arc::downgrade(shared);
    multi_raft.set_snapshot_requirement(Arc::new(move |group_id, sequencer_first| {
        let Some(shared) = weak.upgrade() else {
            return false;
        };
        let vshards = routing
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .vshards_for_group(group_id);
        // Evaluated first and always: it registers the vShards as waiting.
        let reaches = shared.calvin.bases.group_reaches(&vshards, sequencer_first);
        !reaches || shared.redo_chunks.owes_snapshot(group_id)
    }));
    Ok(())
}

/// Whether the scheduler of `vshard_id` can start, with the sequencer log
/// holding entries from `sequencer_start`. A vShard whose base does not
/// reach the log needs a snapshot: its group's replica here refuses entries
/// until one installs.
///
/// `None` is a sequencer log with no known start: it holds no entry and no
/// snapshot boundary. Its leader can still send a snapshot that skips the
/// inputs a scheduler will wait for, so no scheduler starts yet. Nor does
/// one start while a sequencer snapshot installs here.
pub fn may_start(
    shared: &SharedState,
    multi_raft: &Mutex<MultiRaft>,
    vshard_id: u32,
    group_id: Option<u64>,
    sequencer_start: Option<u64>,
) -> bool {
    let Some(sequencer_first) = sequencer_start else {
        return false;
    };
    if shared
        .calvin
        .bases
        .sequencer_install_pending(sequencer_first)
    {
        return false;
    }
    let base = shared.calvin.bases.base(vshard_id);
    if CalvinBase::reaches(base, sequencer_first) {
        return true;
    }
    if let Some(group_id) = group_id {
        let mut multi_raft = multi_raft.lock().unwrap_or_else(|p| p.into_inner());
        multi_raft.set_snapshot_required(group_id, true);
        // Without a scheduler here the vShard casts no vote, so a leader here
        // blocks every Calvin transaction on it. A replica whose state is
        // whole takes the leadership. When none qualifies, the transactions
        // end at their deadline with an error.
        let sole_replica = multi_raft
            .group_membership(group_id)
            .is_some_and(|m| m.voters.len() + m.learners.len() <= 1);
        if sole_replica && shared.calvin.bases.first_stopped_report(vshard_id) {
            tracing::error!(
                vshard_id,
                group_id,
                sequencer_first,
                ?base,
                "calvin: vShard {vshard_id} lost its Calvin base on group {group_id}, where \
                 this node is the only replica; Calvin stays stopped for the vShard"
            );
        }
        if let Some(target) = multi_raft.hand_off_leadership(group_id) {
            tracing::info!(
                vshard_id,
                group_id,
                target,
                "calvin: the vShard's scheduler cannot start here; the data-group \
                 leadership moves to a replica that runs one"
            );
        }
    }
    tracing::debug!(
        vshard_id,
        ?group_id,
        sequencer_first,
        ?base,
        "calvin: the vShard's Calvin state does not reach the sequencer log; its scheduler \
         waits for a data-group snapshot"
    );
    false
}

/// Record that the schedulers of `vshards`, each `(vshard_id, from)`,
/// started from bases that reach the sequencer log and caught up from
/// index `from`. They keep them whole from here.
pub fn note_kept(shared: &SharedState, vshards: &[(u32, u64)]) {
    if let Err(error) = shared
        .calvin
        .bases
        .record_kept(shared.credentials.catalog(), vshards)
    {
        tracing::error!(
            ?vshards,
            %error,
            "calvin: the vShards' kept bases did not persist; a restart takes a snapshot for \
             them"
        );
    }
}

/// The served vShards whose base a snapshot install replaced since their
/// scheduler started, or a sequencer snapshot install ended. Their
/// scheduler stops and starts again from the state a snapshot installs.
pub fn rebased(shared: &SharedState, served: &[u32]) -> Vec<u32> {
    served
        .iter()
        .copied()
        .filter(|vshard_id| !CalvinBase::is_kept(shared.calvin.bases.base(*vshard_id)))
        .collect()
}

/// Forget the Calvin state of `vshards`: this node left their groups. A
/// later return catches up from nothing, so neither their bases nor their
/// applied positions can survive.
pub fn forget_left(shared: &SharedState, vshards: &[u32]) {
    if vshards.is_empty() {
        return;
    }
    let catalog = shared.credentials.catalog();
    let resets: Vec<crate::control::security::catalog::calvin_applied::StoredCalvinApplied> =
        vshards
            .iter()
            .map(|&vshard_id| {
                crate::control::security::catalog::calvin_applied::StoredCalvinApplied {
                    vshard_id,
                    fully_applied_epoch:
                        crate::control::cluster::calvin::scheduler::NOT_YET_APPLIED_EPOCH,
                    tail: BTreeSet::new(),
                }
            })
            .collect();
    for &vshard_id in vshards {
        shared.calvin.applied.remove(vshard_id);
    }
    let forgotten = catalog
        .replace_calvin_applied(&resets)
        .and_then(|()| shared.calvin.bases.forget(catalog, vshards));
    if let Err(error) = forgotten {
        tracing::error!(
            ?vshards,
            %error,
            "calvin: the left vShards' Calvin state was not forgotten"
        );
    }
}

/// Keep the sequencer log range only for vShards of the data groups this
/// node mounts.
pub fn retain_mounted(
    shared: &SharedState,
    multi_raft: &Mutex<MultiRaft>,
    routing: &RwLock<nodedb_cluster::RoutingTable>,
) {
    let groups = multi_raft
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .group_ids();
    let routing = routing.read().unwrap_or_else(|p| p.into_inner());
    let mounted: BTreeSet<u32> = groups
        .into_iter()
        .filter(|group_id| {
            *group_id != nodedb_cluster::METADATA_GROUP_ID
                && *group_id != nodedb_cluster::calvin::SEQUENCER_GROUP_ID
        })
        .flat_map(|group_id| routing.vshards_for_group(group_id))
        .collect();
    shared.calvin.bases.retain_waiting(&mounted);
}
