// SPDX-License-Identifier: BUSL-1.1

//! What this node's authorization state covers, per Raft group.
//!
//! - **Metadata group:** roles, grants, RLS policies, scope grants and tree
//!   definitions. The metadata applier updates each store before it advances
//!   the applied index, and queues tree-definition changes, which this step
//!   moves into the permission cache. The applied index is covered as read.
//! - **Data groups:** the permission cache holds tree rows through the
//!   Event Plane's permission step. A group's applied index read before the
//!   emitted-event counters is covered once the step reaches those counters.
//! - **Sequencer group:** Calvin writes. Covered as [`super::calvin_acks`]
//!   settles, then through the permission step like a data group.
//! - **A group this node does not replicate** is reported at `u64::MAX`:
//!   planning here refuses any tenant whose tree rows live in it.
//!
//! Indexes are read first, then the counters, then the cache is checked. A
//! group's writes at or below the index read emitted their events before the
//! counters were read, so a cache that reached the counters holds them.

use std::time::{Duration, Instant};

use nodedb_cluster::calvin::SEQUENCER_GROUP_ID;
use nodedb_cluster::{GroupCoverage, METADATA_GROUP_ID};

use crate::control::security::auth_fence::cluster::{group_of_vshard, hosts_group, routed_groups};
use crate::control::security::auth_fence::view::apply_committed_tree_defs;
use crate::control::security::permission_tree::reload;
use crate::control::state::SharedState;

/// Raw indexes, read before the emitted-event counters.
struct RawCoverage {
    metadata: u64,
    /// Data groups and the sequencer group, which the permission step covers.
    event_backed: Vec<GroupCoverage>,
}

fn read_raw(state: &SharedState) -> RawCoverage {
    let metadata = state.applied_index_watcher(METADATA_GROUP_ID).current();
    let mut event_backed: Vec<GroupCoverage> = routed_groups(state)
        .into_iter()
        .filter(|group_id| *group_id != METADATA_GROUP_ID && *group_id != SEQUENCER_GROUP_ID)
        .map(|group_id| GroupCoverage {
            group_id,
            through: if hosts_group(state, group_id) {
                state.applied_index_watcher(group_id).current()
            } else {
                u64::MAX
            },
        })
        .collect();
    event_backed.push(GroupCoverage {
        group_id: SEQUENCER_GROUP_ID,
        through: sequencer_coverage(state),
    });
    RawCoverage {
        metadata,
        event_backed,
    }
}

/// The sequencer index this node's Calvin replicas cover.
fn sequencer_coverage(state: &SharedState) -> u64 {
    let hosts_sequencer = state.raft_status_fn.get().is_some_and(|status| {
        status()
            .iter()
            .any(|group| group.group_id == SEQUENCER_GROUP_ID)
    });
    let Some(registry) = state.calvin_completion_registry.get() else {
        return u64::MAX;
    };
    if !hosts_sequencer {
        // No sequencer replica runs here, so no local scheduler applies a
        // Calvin write and none can be planned against.
        return u64::MAX;
    }
    let applied = state.applied_index_watcher(SEQUENCER_GROUP_ID).current();
    let fence = &state.authorization_fence;
    fence
        .calvin_acks()
        .covered_through(registry, fence.calvin_mirrors(), applied, |vshard_id| {
            group_of_vshard(state, vshard_id).is_ok_and(|g| hosts_group(state, g))
        })
}

/// This node's coverage, confirmed through the permission step.
///
/// The metadata group is always current. The event-backed groups are taken
/// from this snapshot when the permission step reaches it within `wait`;
/// otherwise `previous` stands for them, which an earlier call confirmed.
pub async fn confirmed_coverage(
    state: &SharedState,
    previous: &[GroupCoverage],
    wait: Duration,
) -> crate::Result<Vec<GroupCoverage>> {
    let raw = read_raw(state);
    apply_committed_tree_defs(state).await;
    if state.authorization_fence.take_snapshot_installed() {
        // The snapshot rows emitted no events. A reload after the indexes
        // were read holds them.
        reload::reload_all(state, None).await?;
    }
    reload::reload_if_stale(state).await?;

    let caught_up = permission_step_reaches_now(state, wait).await?;
    let mut coverage = vec![GroupCoverage {
        group_id: METADATA_GROUP_ID,
        through: raw.metadata,
    }];
    if caught_up {
        coverage.extend(raw.event_backed);
    } else {
        coverage.extend(
            previous
                .iter()
                .filter(|report| report.group_id != METADATA_GROUP_ID)
                .copied(),
        );
    }
    Ok(coverage)
}

/// Whether the permission cache reflects every event the cores emitted
/// before this call, within `wait`. A core that lost an event is reloaded.
pub(crate) async fn permission_step_reaches_now(
    state: &SharedState,
    wait: Duration,
) -> crate::Result<bool> {
    let fence = &state.authorization_fence;
    let Some(targets) = fence.emitted_snapshot() else {
        // No permission step runs, so only a reload reflects the writes.
        reload::reload_all(state, None).await?;
        return Ok(true);
    };
    let until = Instant::now() + wait;
    loop {
        let notified = fence.permission_applied().notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        let needs_reload = {
            let cache = state.permission_cache.read().await;
            if cache.progress().caught_up(&targets) {
                return Ok(true);
            }
            cache.progress().needs_reload_for(&targets)
        };
        if needs_reload {
            reload::reload_all(state, Some(&targets)).await?;
            continue;
        }
        let now = Instant::now();
        if now >= until {
            return Ok(false);
        }
        let _ = tokio::time::timeout(until - now, notified).await;
    }
}

/// Whether the permission step covers every event the cores emitted before
/// this call, within `wait`. A writer holding its acknowledgement calls this.
///
/// It never reloads. A cache that only a reload can bring to the targets is
/// stale, and [`reload::reload_if_stale`] reloads it before the next
/// statement plans. That reload reads each core after the write applied, so
/// the write already binds every later plan. Before the Event Plane starts no
/// permission step counts writes, so the cache is marked for a reload.
pub(crate) async fn permission_step_covers_now(state: &SharedState, wait: Duration) -> bool {
    let fence = &state.authorization_fence;
    let Some(targets) = fence.emitted_snapshot() else {
        state
            .permission_cache
            .write()
            .await
            .progress_mut()
            .mark_reload_needed();
        return true;
    };
    let until = Instant::now() + wait;
    loop {
        let notified = fence.permission_applied().notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        {
            let cache = state.permission_cache.read().await;
            let progress = cache.progress();
            if progress.caught_up(&targets) || progress.needs_reload_for(&targets) {
                return true;
            }
        }
        let now = Instant::now();
        if now >= until {
            return false;
        }
        let _ = tokio::time::timeout(until - now, notified).await;
    }
}
