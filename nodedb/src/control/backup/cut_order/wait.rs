// SPDX-License-Identifier: BUSL-1.1

//! A cut's wait on this node for the barrier of each data group it hosts.

use std::time::Duration;

use crate::Error;
use crate::control::security::auth_fence::cluster::{hosts_group, wait_applied};
use crate::control::state::SharedState;

use super::ordered_cut::{CutKey, OrderedCut};

/// How often a wait looks again when no change woke it. A group this node
/// leaves wakes nothing.
const WAIT_POLL: Duration = Duration::from_millis(50);

/// Wait until every group of `groups` applied `cut`'s barrier here, and
/// every entry before the barrier applied here, or `deadline`. A group this
/// node left needs no wait: the source node that snapshots it takes its own
/// cut.
pub(crate) async fn await_group_barriers(
    state: &SharedState,
    cut: &OrderedCut,
    groups: &[u64],
    deadline: tokio::time::Instant,
) -> Result<(), Error> {
    let waits = futures::future::join_all(
        groups
            .iter()
            .map(|group_id| await_group_barrier(state, cut, *group_id, deadline)),
    )
    .await;
    waits.into_iter().collect()
}

/// Wait until every group of `groups` applied the barrier of `key` here, or
/// `until`. A group this node left counts as placed. Returns whether every
/// group's barrier applied.
pub(crate) async fn await_placed(
    state: &SharedState,
    key: CutKey,
    groups: &[u64],
    until: tokio::time::Instant,
) -> bool {
    loop {
        let changed = state.calvin.cut_barriers.notified();
        tokio::pin!(changed);
        changed.as_mut().enable();
        let placed = groups.iter().all(|group_id| {
            state.calvin.cut_barriers.applied(key, *group_id).is_some()
                || !hosts_group(state, *group_id)
        });
        if placed {
            return true;
        }
        if tokio::time::Instant::now() >= until {
            return false;
        }
        tokio::select! {
            () = &mut changed => {}
            () = tokio::time::sleep(WAIT_POLL) => {}
            () = tokio::time::sleep_until(until) => {}
        }
    }
}

async fn await_group_barrier(
    state: &SharedState,
    cut: &OrderedCut,
    group_id: u64,
    deadline: tokio::time::Instant,
) -> Result<(), Error> {
    let key = cut.key();
    loop {
        // Registered before the checks, so a change between a check and the
        // wait still wakes it.
        let changed = state.calvin.cut_barriers.notified();
        tokio::pin!(changed);
        changed.as_mut().enable();
        if let Some(barrier_index) = state.calvin.cut_barriers.applied(key, group_id) {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            return match wait_applied(state, group_id, barrier_index, remaining).await {
                Ok(()) => Ok(()),
                Err(error) if !hosts_group(state, group_id) => {
                    note_left(group_id, &error);
                    Ok(())
                }
                Err(error) => Err(Error::Internal {
                    detail: format!(
                        "backup: the consistent-cut barrier of raft group {group_id} at log \
                         index {barrier_index} did not apply on this node: {error}. Retry the \
                         backup"
                    ),
                }),
            };
        }
        if !hosts_group(state, group_id) {
            note_left(group_id, &"no barrier applied yet");
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(unplaced_error(state, cut, group_id));
        }
        tokio::select! {
            () = &mut changed => {}
            () = tokio::time::sleep(WAIT_POLL) => {}
            () = tokio::time::sleep_until(deadline) => {}
        }
    }
}

fn note_left(group_id: u64, why: &dyn std::fmt::Display) {
    tracing::info!(
        group_id,
        %why,
        "backup: this node left the group before its cut barrier applied here; the group \
         needs no cut on this node"
    );
}

/// The error of a group whose barrier did not apply here by the deadline.
fn unplaced_error(state: &SharedState, cut: &OrderedCut, group_id: u64) -> Error {
    let leads = state.multi_raft.get().is_some_and(|multi_raft| {
        multi_raft
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .leader_term(group_id)
            .is_some()
    });
    let place = if leads {
        let vshards = state
            .cluster_routing
            .as_ref()
            .map_or_else(Vec::new, |routing| {
                routing
                    .read()
                    .unwrap_or_else(|p| p.into_inner())
                    .vshards_for_group(group_id)
            });
        let lagging = state.calvin.cuts.lagging_among(cut.hlc, &vshards);
        format!(
            "This node leads the group. The Calvin schedulers of its vShards {lagging:?} here \
             have not passed the cut's marker"
        )
    } else {
        "Another node leads the group and places the barrier once its Calvin schedulers \
         passed the cut's marker"
            .to_owned()
    };
    Error::Internal {
        detail: format!(
            "backup: raft group {group_id} applied no consistent-cut barrier at watermark {} on \
             this node in time. {place}. Retry the backup",
            cut.hlc
        ),
    }
}
