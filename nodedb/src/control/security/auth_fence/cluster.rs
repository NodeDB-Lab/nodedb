// SPDX-License-Identifier: BUSL-1.1

//! Cluster helpers shared by the planning view and the authorization lease:
//! hosting checks, confirmed read indexes and applied-index waits.

use std::time::{Duration, Instant};

use nodedb_cluster::WaitOutcome;

use crate::control::cluster::read_index::ReadIndexRefusal;
use crate::control::state::SharedState;

/// Refusal for a statement whose authorization state is not current.
pub(crate) fn behind(detail: impl Into<String>) -> crate::Error {
    crate::Error::AuthorizationStateBehind {
        detail: detail.into(),
    }
}

/// Whether this node replicates `group_id`, as a voter or a learner.
pub(crate) fn hosts_group(state: &SharedState, group_id: u64) -> bool {
    let Some(routing) = state.cluster_routing.as_ref() else {
        return false;
    };
    let routing = routing.read().unwrap_or_else(|p| p.into_inner());
    routing.group_info(group_id).is_some_and(|info| {
        info.members.contains(&state.node_id) || info.learners.contains(&state.node_id)
    })
}

/// The data group that homes `vshard_id`, from this node's routing table.
pub(crate) fn group_of_vshard(state: &SharedState, vshard_id: u32) -> crate::Result<u64> {
    let Some(routing) = state.cluster_routing.as_ref() else {
        return Err(behind("no routing table on this node"));
    };
    let routing = routing.read().unwrap_or_else(|p| p.into_inner());
    routing
        .group_for_vshard(vshard_id)
        .map_err(|e| behind(format!("raft group of vShard {vshard_id}: {e}")))
}

/// Every data group in this node's routing table.
pub(crate) fn routed_groups(state: &SharedState) -> Vec<u64> {
    let Some(routing) = state.cluster_routing.as_ref() else {
        return Vec::new();
    };
    let routing = routing.read().unwrap_or_else(|p| p.into_inner());
    routing.group_ids()
}

/// A read index of `group_id` confirmed by its leader against a quorum,
/// taken by a probe that started after this call.
pub(crate) async fn confirmed_read_index(
    state: &SharedState,
    group_id: u64,
    timeout: Duration,
) -> crate::Result<u64> {
    let Some(gate) = state.raft_read_gate.get() else {
        return Err(behind(format!(
            "no read index service for raft group {group_id} yet"
        )));
    };
    let deadline = Instant::now() + timeout;
    state
        .authorization_fence
        .read_index_coalescer(group_id)
        .read_index(deadline, || gate.read_index(group_id, timeout))
        .await
        .map_err(|refusal| match refusal {
            ReadIndexRefusal::NotLeader => {
                behind(format!("raft group {group_id} has no reachable leader"))
            }
            ReadIndexRefusal::Timeout { waited_ms } => behind(format!(
                "raft group {group_id} confirmed no read index within {waited_ms}ms"
            )),
        })
}

/// Wait until this node's applied index of `group_id` reaches `target`.
pub(crate) async fn wait_applied(
    state: &SharedState,
    group_id: u64,
    target: u64,
    timeout: Duration,
) -> crate::Result<()> {
    let watcher = state.applied_index_watcher(group_id);
    if watcher.current() >= target {
        return Ok(());
    }
    // The watcher parks its caller on a condition variable, so the wait runs
    // on the blocking pool.
    let outcome = tokio::task::spawn_blocking(move || watcher.wait_for(target, timeout))
        .await
        .map_err(|e| crate::Error::Internal {
            detail: format!("applied-index wait for raft group {group_id} did not finish: {e}"),
        })?;
    match outcome {
        WaitOutcome::Reached => Ok(()),
        WaitOutcome::TimedOut => Err(behind(format!(
            "raft group {group_id} was not applied through index {target} in time"
        ))),
        WaitOutcome::GroupGone => Err(behind(format!(
            "raft group {group_id} left this node while it caught up"
        ))),
    }
}
