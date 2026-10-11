// SPDX-License-Identifier: BUSL-1.1

//! Each node's part of a cluster restore point.
//!
//! The metadata entry that creates a point applies on every node in log
//! order. Each node then records the metadata group's place, and cuts every
//! data group it hosts, plus the Calvin sequencer, at the point's watermark.
//! Every replica of a group applies the cut barrier at the same log index and
//! records it, so the group's place at the point is that index on every node.
//! The group's leader places the barrier (see
//! `crate::control::backup::cut_order`). A group can still hold more than one
//! barrier for a point: a leader proposes it again when a later term
//! overwrote its proposal, and the overwritten copy can commit too. The
//! lowest index is the group's place.

use std::sync::{Arc, Weak};

use nodedb_cluster::METADATA_GROUP_ID;
use nodedb_cluster::calvin::{RestorePointHook, SEQUENCER_GROUP_ID, SequencerRestorePoint};
use nodedb_types::Hlc;
use nodedb_wal::record::RestorePointPayload;
use tracing::{error, info, warn};

use crate::control::state::SharedState;
use crate::wal::manager::NO_APPLY_KEY;

/// Record one group's place at a restore point in this node's WAL, durably,
/// and move this node's clock past the point's watermark. A data group's
/// record lists the vShards it homes now. A failure is logged: a cluster
/// restore to the point then starts this node's replica of the group with no
/// log.
pub fn record_group_point(state: &SharedState, mut point: RestorePointPayload) {
    state
        .hlc_clock
        .update(Hlc::new(point.hlc.saturating_add(1), 0));
    if point.group_id != METADATA_GROUP_ID
        && point.group_id != SEQUENCER_GROUP_ID
        && let Some(routing) = &state.cluster_routing
    {
        point.vshards = routing
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .vshards_for_group(point.group_id);
    }
    let recorded = state
        .wal
        .appender(NO_APPLY_KEY)
        .append_restore_point(&point)
        .and_then(|_| state.wal.sync());
    match recorded {
        Ok(()) => info!(
            restore_point = point.id,
            group_id = point.group_id,
            applied_index = point.applied_index,
            "restore point recorded for raft group"
        ),
        Err(e) => error!(
            restore_point = point.id,
            group_id = point.group_id,
            error = %e,
            "restore point not recorded in the WAL; a cluster restore to it starts this \
             group with no log on this node"
        ),
    }
}

/// The hook the sequencer state machine calls when it applies a restore
/// point's cut marker. The WAL append runs on a blocking thread: the hook
/// runs on the Raft tick thread, which must not do I/O.
pub fn sequencer_hook(shared: Weak<SharedState>) -> RestorePointHook {
    Arc::new(move |point: SequencerRestorePoint| {
        let Some(state) = shared.upgrade() else {
            return;
        };
        let payload = RestorePointPayload {
            id: point.id,
            hlc: point.hlc,
            group_id: SEQUENCER_GROUP_ID,
            applied_index: point.index,
            term: 0,
            next_epoch: point.next_epoch,
            epoch_system_ms: point
                .epoch_system_ms
                .and_then(|ms| u64::try_from(ms).ok())
                .unwrap_or(0),
            vshards: Vec::new(),
        };
        let record = move || {
            record_group_point(&state, payload);
            seal_point_segment(&state, point.id);
        };
        match tokio::runtime::Handle::try_current() {
            Ok(runtime) => {
                runtime.spawn_blocking(record);
            }
            Err(_) => record(),
        }
    })
}

/// Cut every data group this node hosts, and the Calvin sequencer, at the
/// point's watermark. Runs in the background: the metadata apply that
/// starts it must not wait on other groups. The WAL segment holding the
/// point's records is then sealed, so the archiver uploads it.
pub fn spawn_node_cut(state: Arc<SharedState>, id: u64, hlc: u64) {
    tokio::spawn(async move {
        match crate::control::backup::cut::cut_at_point(&state, hlc, id).await {
            Ok(()) => info!(restore_point = id, "restore point cut taken on this node"),
            Err(e) => warn!(
                restore_point = id,
                error = %e,
                "restore point cut did not finish on this node; a cluster restore to it \
                 starts every group this node did not record with no log"
            ),
        }
        let sealing = Arc::clone(&state);
        if let Err(e) = tokio::task::spawn_blocking(move || seal_point_segment(&sealing, id)).await
        {
            warn!(restore_point = id, error = %e, "restore point segment seal did not run");
        }
    });
}

/// Seal the active WAL segment after the records of restore point `id`.
fn seal_point_segment(state: &SharedState, id: u64) {
    if let Err(e) = state.wal.seal_active_segment() {
        warn!(
            restore_point = id,
            error = %e,
            "WAL segment not sealed after the restore point; the archiver uploads its \
             records once the segment fills"
        );
    }
}
