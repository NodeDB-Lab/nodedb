// SPDX-License-Identifier: BUSL-1.1

//! The Calvin cut a data-group snapshot build takes on the leader.
//!
//! The build first places a cut marker in the sequencer log and waits until
//! the scheduler of every group vShard here passed it (see
//! [`await_calvin_cut`]). Every input sequenced below the marker then
//! finished here. The build then fences the group: the apply gate it holds
//! exclusive also stops every Calvin install of the group's vShards, since
//! a scheduler installs only under a shared hold of that gate. The applied
//! positions it reads under the fence (see [`capture_calvin_cut`]) are
//! exactly the ones the captured storage holds.

use std::collections::{BTreeSet, HashSet};
use std::time::Duration;

use nodedb_cluster::calvin::SequencerEntry;

use crate::Error;
use crate::control::cluster::calvin::scheduler::barrier_store;
use crate::control::security::catalog::calvin_base::CalvinBase;
use crate::control::state::SharedState;
use crate::types::{GroupCalvinCut, VShardCalvinState};

/// How long the build waits for the group's schedulers to pass its marker.
/// A failed build is retried on the next heartbeat.
const CUT_WAIT: Duration = Duration::from_secs(5);

/// How long one marker proposal waits before it is proposed again. A leader
/// change can drop a proposed marker.
const MARKER_RETRY: Duration = Duration::from_secs(1);

/// Place a cut marker and wait until the scheduler of every group vShard
/// here passed it. Returns the marker's sequencer log index.
///
/// Fails when this node holds no whole Calvin state of a group vShard: its
/// scheduler did not start from a base that reaches the sequencer log, so
/// its storage can lack Calvin transactions. Another replica sends the
/// snapshot, or this one once its own snapshot installed.
pub async fn await_calvin_cut(
    shared: &SharedState,
    group_id: u64,
    group_vshards: &HashSet<u32>,
) -> Result<u64, Error> {
    if let Some(vshard_id) = group_vshards
        .iter()
        .copied()
        .find(|vshard_id| !CalvinBase::is_kept(shared.calvin.bases.base(*vshard_id)))
    {
        return Err(Error::Internal {
            detail: format!(
                "snapshot build: group {group_id}: this node holds no whole Calvin state of \
                 vShard {vshard_id}; the build retries on the next heartbeat"
            ),
        });
    }
    let proposer = shared
        .calvin
        .sequencer_proposer
        .get()
        .ok_or_else(|| Error::Internal {
            detail: format!(
                "snapshot build: group {group_id}: no sequencer proposer is set on this node, \
                 so the build cannot place its Calvin cut marker; the build retries on the \
                 next heartbeat"
            ),
        })?;
    let hlc = shared.hlc_clock.now().wall_ns;
    let marker = zerompk::to_msgpack_vec(&SequencerEntry::CutMarker {
        hlc,
        restore_point: 0,
        barrier: None,
    })
    .map_err(|error| Error::Internal {
        detail: format!("snapshot build: group {group_id}: encode the Calvin cut marker: {error}"),
    })?;
    let vshards: BTreeSet<u32> = group_vshards.iter().copied().collect();
    let deadline = tokio::time::Instant::now() + CUT_WAIT;
    let mut last_refusal = None;
    loop {
        if let Err(error) = proposer.propose(marker.clone()) {
            last_refusal = Some(error.to_string());
        }
        let attempt = deadline.min(tokio::time::Instant::now() + MARKER_RETRY);
        if let Some(index) = shared
            .calvin
            .cuts
            .await_vshards_passed(hlc, &vshards, attempt)
            .await
        {
            return Ok(index);
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(Error::Internal {
                detail: format!(
                    "snapshot build: group {group_id}: its Calvin schedulers did not pass the \
                     cut marker in time (last marker refusal: {}); the build retries on the \
                     next heartbeat",
                    last_refusal.as_deref().unwrap_or("none")
                ),
            });
        }
    }
}

/// The group's Calvin cut through `through`: the applied positions of every
/// group vShard here, and the stored barrier logs of their unfinished txns.
/// The caller holds the group's apply gate exclusive, so no Calvin install
/// of these vShards runs and no barrier entry of the group applies.
///
/// Fails when the barrier logs cannot be read whole (see
/// [`barrier_store::capture_group`]). The build retries on the next
/// heartbeat.
pub fn capture_calvin_cut(
    shared: &SharedState,
    group_vshards: &HashSet<u32>,
    through: u64,
) -> Result<GroupCalvinCut, Error> {
    let ledgers = &shared.calvin.applied;
    let ids: BTreeSet<u32> = group_vshards.iter().copied().collect();
    let vshards = ids
        .iter()
        .filter_map(|&vshard_id| {
            let (fully_applied_epoch, tail) = ledgers.get(vshard_id)?.snapshot();
            Some(VShardCalvinState {
                vshard_id,
                fully_applied_epoch,
                tail: tail.into_iter().collect(),
            })
        })
        .collect();
    let barrier_logs = barrier_store::capture_group(shared, &ids)?;
    Ok(GroupCalvinCut {
        through,
        vshards,
        barrier_logs,
    })
}
