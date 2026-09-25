// SPDX-License-Identifier: BUSL-1.1

//! The writer's side: hold an authorization change's acknowledgement until
//! no node can plan against the state before it.
//!
//! - **In a cluster** the metadata leader holds the barrier until every node
//!   with an unexpired lease covered the targets, or its lease expired. The
//!   writing node is a lease holder too, so its own Event Plane lag closes
//!   the same way.
//! - **On a single node** there is no lease. The barrier waits until the
//!   local permission cache reflects every event the cores emitted, which
//!   covers the change just applied.

use std::time::{Duration, Instant};

use nodedb_cluster::calvin::SEQUENCER_GROUP_ID;
use nodedb_cluster::{AuthBarrierOutcome, AuthBarrierRequest, GroupCoverage, RaftRpc};
use tokio::runtime::RuntimeFlavor;

use crate::control::security::auth_fence::cluster::behind;
use crate::control::state::SharedState;

use super::coverage::permission_step_covers_now;
use super::leadership::{metadata_leader, send_to_leader};

/// Hold the acknowledgement of a change until it binds every node.
///
/// `targets` name the change: per group, the index it committed at. The
/// change itself is committed; an error means only that the barrier did not
/// release within the request deadline.
///
/// Latency: in a cluster, every authorization-bearing write waits about one
/// renewal interval (the Raft heartbeat) before it is acknowledged. Each
/// holder reports its coverage only with its next renewal. This covers every
/// DDL that bears authorization, `CREATE COLLECTION` included. A holder that
/// cannot renew adds up to one lease duration (the election timeout), until
/// its lease expires. On a single node the wait is the permission step's
/// lag only.
pub async fn authorization_barrier(
    state: &SharedState,
    targets: Vec<GroupCoverage>,
) -> crate::Result<()> {
    let deadline_secs = state.tuning.network.default_deadline_secs;
    let deadline = Instant::now() + Duration::from_secs(deadline_secs);
    let Some(timing) = state.authorization_fence.timing() else {
        return await_local_coverage(state, deadline).await;
    };
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(committed_but_pending(format!(
                "the barrier did not release within {deadline_secs}s"
            )));
        }
        let request = AuthBarrierRequest {
            targets: targets.clone(),
            timeout_ms: u64::try_from(remaining.as_millis()).unwrap_or(u64::MAX),
        };
        let outcome = match metadata_leader(state).filter(|(leader, _)| *leader != 0) {
            None => None,
            Some((leader_id, _)) if leader_id == state.node_id => {
                match state.authorization_fence.leader() {
                    Some(service) => Some(service.hold_barrier(request).await.outcome),
                    None => None,
                }
            }
            Some((leader_id, _)) => match send_to_leader(
                state,
                leader_id,
                RaftRpc::AuthBarrierRequest(request),
                remaining + timing.lease,
            )
            .await
            {
                Ok(RaftRpc::AuthBarrierResponse(response)) => Some(response.outcome),
                Ok(other) => {
                    tracing::warn!(
                        leader_id,
                        "authorization barrier: unexpected reply {other:?}"
                    );
                    None
                }
                Err(error) => {
                    tracing::debug!(%error, "authorization barrier: not delivered");
                    None
                }
            },
        };
        match outcome {
            Some(AuthBarrierOutcome::Released) => return Ok(()),
            // No leader known, a leader change, or a lost message: ask the
            // leader again. A new leader holds the barrier to its own floors.
            Some(AuthBarrierOutcome::NotLeader { .. })
            | Some(AuthBarrierOutcome::Timeout { .. })
            | None => tokio::time::sleep(timing.renew_every).await,
        }
    }
}

/// Hold the acknowledgement of a Calvin transaction that wrote a
/// permission-tree source.
///
/// Its completion acks sit in the sequencer log, at or below the sequencer
/// group's commit index now. A node covers that index only once its own
/// replicas applied every acknowledged transaction below it.
pub async fn calvin_write_barrier(state: &SharedState) -> crate::Result<()> {
    if state.authorization_fence.timing().is_none() {
        let deadline =
            Instant::now() + Duration::from_secs(state.tuning.network.default_deadline_secs);
        return await_local_coverage(state, deadline).await;
    }
    let commit_index = state
        .raft_status_fn
        .get()
        .and_then(|status| {
            status()
                .into_iter()
                .find(|group| group.group_id == SEQUENCER_GROUP_ID)
                .map(|group| group.commit_index)
        })
        .ok_or_else(|| committed_but_pending("this node does not replicate the sequencer group"))?;
    authorization_barrier(
        state,
        vec![GroupCoverage {
            group_id: SEQUENCER_GROUP_ID,
            through: commit_index,
        }],
    )
    .await
}

/// Run [`authorization_barrier`] from synchronous code on a Tokio worker.
pub fn block_on_barrier(state: &SharedState, targets: Vec<GroupCoverage>) -> crate::Result<()> {
    let handle = tokio::runtime::Handle::try_current().map_err(|_| crate::Error::Internal {
        detail: "authorization barrier: called outside a Tokio runtime".into(),
    })?;
    if handle.runtime_flavor() != RuntimeFlavor::MultiThread {
        return Err(crate::Error::Internal {
            detail: "authorization barrier: synchronous callers need a multi-thread runtime".into(),
        });
    }
    tokio::task::block_in_place(|| handle.block_on(authorization_barrier(state, targets)))
}

/// Wait until the permission step covers every event the cores emitted
/// before this call, which includes the write just applied.
///
/// This is a writer's acknowledgement wait: it only waits, and never reloads
/// or dispatches. A cache that needs a reload is reloaded by the next
/// statement's planning, before it reads the cache.
pub async fn await_local_coverage(state: &SharedState, deadline: Instant) -> crate::Result<()> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if permission_step_covers_now(state, remaining).await {
        return Ok(());
    }
    Err(committed_but_pending(
        "the permission cache did not catch up with the change",
    ))
}

fn committed_but_pending(detail: impl std::fmt::Display) -> crate::Error {
    behind(format!(
        "the authorization change is committed, but {detail}; nodes still planning against \
         the previous state refuse until they catch up"
    ))
}
