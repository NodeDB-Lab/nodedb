// SPDX-License-Identifier: BUSL-1.1

//! The task that places one ordered cut's barrier in every data group this
//! node leads.
//!
//! A node starts the driver when it learns of the cut: its coordinator takes
//! the cut here, or a scheduler here receives the cut's marker. The driver
//! runs until every data group this node hosts applied the cut's barrier
//! here, or the cut's window closes. While this node leads a group, the
//! driver proposes the barrier once every scheduler of the group's vShards
//! on this node passed the cut's marker: every transaction sequenced before
//! the marker then installed here, so its redo is in the log before the
//! barrier. A proposal a later term overwrote is proposed again.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use nodedb_cluster::calvin::SEQUENCER_GROUP_ID;
use nodedb_cluster::{METADATA_GROUP_ID, MultiRaft};

use crate::control::security::auth_fence::cluster::{hosts_group, routed_groups};
use crate::control::state::SharedState;
use crate::control::wal_replication::ReplicatedEntry;
use crate::types::DatabaseId;

use super::ordered_cut::{CutKey, CutWindow, OrderedCut};
use super::registry::Proposed;

/// How often a driver looks again when no change woke it. A leadership move
/// and a group applying past a lost proposal wake nothing.
const DRIVE_POLL: Duration = Duration::from_millis(20);

/// The tenant a barrier entry is framed under. A barrier orders every entry
/// of its group, whatever tenant writes it. No step reads the tenant of a
/// barrier: the apply's barrier arm ignores it, and a barrier raises no
/// tenant mark.
const BARRIER_FRAME_TENANT: u64 = 0;

/// Every data group this node hosts.
pub(crate) fn hosted_data_groups(state: &SharedState) -> Vec<u64> {
    routed_groups(state)
        .into_iter()
        .filter(|group_id| {
            *group_id != METADATA_GROUP_ID
                && *group_id != SEQUENCER_GROUP_ID
                && hosts_group(state, *group_id)
        })
        .collect()
}

/// Every vShard `group_id` homes, from this node's routing table.
fn group_vshards(state: &SharedState, group_id: u64) -> Vec<u32> {
    state
        .cluster_routing
        .as_ref()
        .map_or_else(Vec::new, |routing| {
            routing
                .read()
                .unwrap_or_else(|p| p.into_inner())
                .vshards_for_group(group_id)
        })
}

/// Start the driver of `cut` on this node, unless one runs for it.
pub(crate) fn ensure_driver(state: &Arc<SharedState>, cut: &OrderedCut) {
    if !state.calvin.cut_barriers.claim_driver(cut) {
        return;
    }
    let key = cut.key();
    let state = Arc::clone(state);
    tokio::spawn(async move {
        drive(&state, key).await;
        state.calvin.cut_barriers.driver_stopped(key);
    });
}

/// Place `key`'s barrier in every group this node leads, until every group
/// this node hosts applied it here or the cut's window closes.
async fn drive(state: &Arc<SharedState>, key: CutKey) {
    let Some(cut) = state.calvin.cut_barriers.cut(key) else {
        return;
    };
    let window = CutWindow::on(state, cut.hlc);
    let mut shutdown = state.shutdown.subscribe();
    loop {
        // Registered before the checks, so a change between a check and the
        // wait still wakes it.
        let changed = state.calvin.cut_barriers.notified();
        tokio::pin!(changed);
        changed.as_mut().enable();
        let open: Vec<u64> = hosted_data_groups(state)
            .into_iter()
            .filter(|group_id| state.calvin.cut_barriers.applied(key, *group_id).is_none())
            .collect();
        if open.is_empty() {
            return;
        }
        // no-determinism: the window bounds only when this leader proposes.
        // The barrier's place in the log is the one every replica applies.
        if state.hlc_clock.now().wall_ns > window.propose_until {
            for group_id in open {
                report_unplaced(state, &cut, group_id);
            }
            return;
        }
        for group_id in open {
            if let Err(error) = propose_if_due(state, &cut, group_id) {
                tracing::error!(
                    group_id,
                    hlc = cut.hlc,
                    %error,
                    "backup cut: the cut's barrier entry cannot be built; the cut fails"
                );
                report_unplaced(state, &cut, group_id);
                return;
            }
        }
        tokio::select! {
            () = shutdown.wait_cancelled() => return,
            () = &mut changed => {}
            () = tokio::time::sleep(DRIVE_POLL) => {}
        }
    }
}

/// This node's leader term of `group_id`, while it leads the group.
fn leader_term(multi_raft: &Mutex<MultiRaft>, group_id: u64) -> Option<u64> {
    multi_raft
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .leader_term(group_id)
}

/// Propose `cut`'s barrier into `group_id` when this node leads the group,
/// every scheduler of the group's vShards here passed the cut's marker, and
/// no proposal of the current term is still in the log. Fails only when the
/// barrier entry cannot be built.
fn propose_if_due(state: &SharedState, cut: &OrderedCut, group_id: u64) -> crate::Result<()> {
    let Some(multi_raft) = state.multi_raft.get() else {
        return Ok(());
    };
    let key = cut.key();
    let Some(term) = leader_term(multi_raft, group_id) else {
        return Ok(());
    };
    if let Some(proposed) = state.calvin.cut_barriers.proposal(key, group_id) {
        // An entry of an earlier term that the group applied past with no
        // barrier of the cut is gone: a later term overwrote it.
        let lost = proposed.term != term
            || state.applied_index_watcher(group_id).current() >= proposed.log_index;
        if !lost {
            return Ok(());
        }
        state
            .calvin
            .cut_barriers
            .forget_proposal(key, group_id, proposed);
        if state.calvin.cut_barriers.applied(key, group_id).is_some() {
            return Ok(());
        }
    }
    let vshards = group_vshards(state, group_id);
    if !state
        .calvin
        .cuts
        .lagging_among(cut.hlc, &vshards)
        .is_empty()
    {
        return Ok(());
    }
    let Some(&vshard_id) = vshards.first() else {
        return Ok(());
    };
    let bytes = ReplicatedEntry::new(
        BARRIER_FRAME_TENANT,
        DatabaseId::DEFAULT.as_u64(),
        vshard_id,
        cut.barrier_write(),
    )
    .encode()?;
    let landed = {
        let mut mr = multi_raft.lock().unwrap_or_else(|p| p.into_inner());
        // Leadership can move between the checks and the proposal.
        if mr.leader_term(group_id) != Some(term) {
            return Ok(());
        }
        mr.propose(vshard_id, bytes)
    };
    match landed {
        Ok((landed_group, log_index)) => {
            state
                .calvin
                .cut_barriers
                .note_proposed(key, landed_group, Proposed { term, log_index })
        }
        Err(error) => tracing::debug!(
            group_id,
            hlc = cut.hlc,
            %error,
            "backup cut: the group refused the cut's barrier; the driver proposes it again"
        ),
    }
    Ok(())
}

/// Report that the cut's window closed before `group_id` applied its
/// barrier here.
fn report_unplaced(state: &SharedState, cut: &OrderedCut, group_id: u64) {
    let led_here = state
        .multi_raft
        .get()
        .is_some_and(|multi_raft| leader_term(multi_raft, group_id).is_some());
    let lagging = state
        .calvin
        .cuts
        .lagging_among(cut.hlc, &group_vshards(state, group_id));
    tracing::warn!(
        group_id,
        hlc = cut.hlc,
        restore_point = cut.restore_point,
        led_here,
        ?lagging,
        "backup cut: the cut's window closed before its barrier applied in this group; \
         the backup or restore point that took the cut failed"
    );
    crate::diag::cut_barrier_not_placed(group_id, cut.hlc, cut.restore_point, led_here, lagging);
}
