// SPDX-License-Identifier: BUSL-1.1

//! The writer's side: hold an authorization change's acknowledgement until
//! no node can plan against the state before it.
//!
//! The metadata leader holds the barrier until every node with an unexpired
//! lease covered the targets, or its lease expired. The writing node is a
//! lease holder too, so its own Event Plane lag closes the same way. A
//! single-node cluster runs the same barrier against its one lease.

use nodedb_cluster::calvin::SEQUENCER_GROUP_ID;
use nodedb_cluster::{AuthBarrierOutcome, AuthBarrierRequest, GroupCoverage, RaftRpc};

use crate::control::security::auth_fence::cluster::behind;
use crate::control::state::SharedState;

use super::leadership::{metadata_leader, send_to_leader};

/// When an authorization barrier gives up, and the statement budget its
/// error reports.
#[derive(Debug, Clone, Copy)]
pub struct BarrierDeadline {
    /// The instant the barrier stops waiting.
    pub at: std::time::Instant,
    /// The whole budget of the statement the barrier runs in.
    pub budget: std::time::Duration,
}

/// Hold the acknowledgement of a change until it binds every node.
///
/// `targets` name the change: per group, the index it committed at. The
/// change itself is committed; an error means only that the barrier did not
/// release by `deadline`, the caller's statement deadline.
///
/// Latency: in a cluster, every authorization-bearing write waits about one
/// renewal interval (the Raft heartbeat) before it is acknowledged. Each
/// holder reports its coverage only with its next renewal. This covers every
/// DDL that bears authorization, `CREATE COLLECTION` included. A holder that
/// cannot renew adds up to one lease duration (the election timeout), until
/// its lease expires. A pinned holder, the leader that is the only voter of
/// the metadata group, has no expiry: the barrier waits for its next renewal
/// however late it runs.
pub async fn authorization_barrier(
    state: &SharedState,
    targets: Vec<GroupCoverage>,
    deadline: BarrierDeadline,
) -> crate::Result<()> {
    let remaining_until = || {
        deadline
            .at
            .saturating_duration_since(std::time::Instant::now())
    };
    let budget_ms = deadline.budget.as_millis();
    let timing = lease_timing(state)?;
    // A statement whose deadline passed before the barrier starts still asks
    // the leader once: the change may already bind every node.
    let late = remaining_until().is_zero();
    loop {
        let wait = barrier_wait(remaining_until(), &timing);
        let request = AuthBarrierRequest {
            targets: targets.clone(),
            timeout_ms: u64::try_from(wait.as_millis()).unwrap_or(u64::MAX),
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
                timing.rpc_read_timeout(wait),
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
        if let Some(AuthBarrierOutcome::Released) = outcome {
            return Ok(());
        }
        if late {
            return Err(committed_but_pending(format!(
                "the statement deadline passed before the barrier started (statement budget \
                 {budget_ms} ms), and one leader attempt did not release it"
            )));
        }
        let remaining = remaining_until();
        if remaining.is_zero() {
            return Err(committed_but_pending(format!(
                "the barrier did not release within the statement deadline (statement budget \
                 {budget_ms} ms)"
            )));
        }
        // No leader known, a leader change, a timeout, or a lost message: ask
        // the leader again. A new leader holds the barrier to its own floors.
        tokio::time::sleep(timing.renew_every.min(remaining)).await;
    }
}

/// How long one leader attempt holds the barrier: the time left, and at
/// least one renewal interval. Coverage arrives only with a renewal, so a
/// shorter hold cannot release, and the leader never receives a zero wait.
fn barrier_wait(
    remaining: std::time::Duration,
    timing: &super::LeaseTiming,
) -> std::time::Duration {
    remaining.max(timing.renew_every)
}

/// Hold the acknowledgement of a Calvin transaction that wrote a
/// permission-tree source.
///
/// Its completion acks sit in the sequencer log, at or below the sequencer
/// group's commit index now. A node covers that index only once its own
/// replicas applied every acknowledged transaction below it. The wait ends
/// at `deadline`, the caller's statement deadline.
pub async fn calvin_write_barrier(
    state: &SharedState,
    deadline: BarrierDeadline,
) -> crate::Result<()> {
    lease_timing(state)?;
    let commit_index = state
        .raft_group_status(SEQUENCER_GROUP_ID)
        .map(|group| group.commit_index)
        .ok_or_else(|| committed_but_pending("this node does not replicate the sequencer group"))?;
    authorization_barrier(
        state,
        vec![GroupCoverage {
            group_id: SEQUENCER_GROUP_ID,
            through: commit_index,
        }],
        deadline,
    )
    .await
}

/// The lease timing `start_raft` installs.
pub(crate) fn lease_timing(state: &SharedState) -> crate::Result<super::LeaseTiming> {
    state
        .authorization_fence
        .timing()
        .ok_or(crate::Error::Internal {
            detail: "the authorization lease is not installed: start_raft has not run on this \
                     node"
                .to_owned(),
        })
}

fn committed_but_pending(detail: impl std::fmt::Display) -> crate::Error {
    behind(format!(
        "the authorization change is committed, but {detail}; nodes still planning against \
         the previous state refuse until they catch up"
    ))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::bridge::dispatch::Dispatcher;
    use crate::wal::WalManager;

    fn timing() -> super::super::LeaseTiming {
        super::super::LeaseTiming::from_raft(Duration::from_millis(150), Duration::from_millis(50))
            .expect("timing")
    }

    #[test]
    fn a_leader_attempt_waits_at_least_one_renewal() {
        let timing = timing();
        assert_eq!(barrier_wait(Duration::ZERO, &timing), timing.renew_every);
        assert_eq!(
            barrier_wait(Duration::from_millis(1), &timing),
            timing.renew_every
        );
        assert_eq!(
            barrier_wait(Duration::from_secs(3), &timing),
            Duration::from_secs(3)
        );
    }

    /// A barrier entered after its deadline makes one attempt, then names
    /// the late start and the statement's whole budget.
    #[tokio::test]
    async fn a_barrier_entered_late_reports_the_statement_budget() {
        let directory = tempfile::tempdir().expect("barrier test directory");
        let wal = std::sync::Arc::new(
            WalManager::open_for_testing(&directory.path().join("barrier.wal"))
                .expect("open barrier test WAL"),
        );
        let (dispatcher, _data_sides) = Dispatcher::new(2, 64);
        let state = SharedState::new(dispatcher, wal).expect("construct barrier state");
        assert!(state.authorization_fence.install_timing(timing()));
        let budget = state.tuning.network.default_deadline_secs * 1000;
        let deadline = BarrierDeadline {
            at: std::time::Instant::now(),
            budget: std::time::Duration::from_millis(budget),
        };

        let error = authorization_barrier(&state, Vec::new(), deadline)
            .await
            .expect_err("no leader releases the barrier");
        let message = error.to_string();
        assert!(
            message.contains("before the barrier started")
                && message.contains(&format!("statement budget {budget} ms")),
            "{message}"
        );
    }
}
