// SPDX-License-Identifier: BUSL-1.1

//! Closing a read cut `S` on the node that serves a read at it.
//!
//! A read at `S` sees every write with ordinal `<= S` and none above, on
//! every serving node:
//!
//! 1. [`close_local_cut`]: this node folds `S` into its ordinal clock and
//!    waits until every write it reserved at or below `S` applied here
//!    (`super::reservations`).
//! 2. [`close_group_cuts`]: for each Raft group the read observes here, the
//!    group's leader closes `S` the same way, then answers a read index
//!    (`RaftReadGate::cut_read_index`). Every replicated write at or below
//!    `S` applied on the leader, so it committed at or below that index.
//!    This node then waits until it applied the group through the index. A
//!    leader takes the index from its own lease or quorum round, after step 1
//!    closed `S` here.
//!
//! Both steps end at the statement deadline with `Error::DeadlineExceeded`,
//! never with a partial answer.
//!
//! The leader also raises the group's ordinal floor to `S` on a quorum of
//! voters, durably, before it answers the read index. A later leader gathers
//! the floor from a quorum of its own term before its first write, and its
//! write gate stamps above it. The two quorums share a voter, so no later
//! leader stamps at or below `S`, whatever its clock reads.

use std::time::Duration;

use crate::control::cluster::linearizable_read::{
    ReadIndexDeadline, StatementDeadline, wait_applied_through,
};
use crate::control::cluster::read_index::ReadIndexRefusal;
use crate::control::state::SharedState;

/// The pause before a refused read-index round asks again: a leader change
/// settles within a few Raft ticks.
const RETRY_PAUSE: Duration = Duration::from_millis(20);

/// Close `cut` on this node: fold it into the ordinal clock, then wait until
/// every write this node reserved at or below it applied, or until
/// `deadline`.
pub(crate) async fn close_local_cut(
    state: &SharedState,
    cut: i64,
    deadline: StatementDeadline,
) -> crate::Result<()> {
    let clock = state.wal.ordinal_clock();
    match state
        .read_cut
        .reservations
        .close_through(&clock, cut, deadline.tokio_instant())
        .await
    {
        Ok(()) => Ok(()),
        Err(blocker) => {
            if super::reservations::is_leaked(&blocker) {
                crate::diag::ordinal_reservation_leaked(
                    blocker.site,
                    blocker.ordinal,
                    blocker.open_for,
                );
            }
            tracing::warn!(
                cut,
                blocking_ordinal = blocker.ordinal,
                open_for_ms = blocker.open_for.as_millis() as u64,
                site = %blocker.site,
                "read cut: a write reserved at or below the cut did not apply within the \
                 statement deadline"
            );
            Err(deadline_exceeded())
        }
    }
}

/// Close `cut` on the leader of each of `groups`, then wait until this node
/// applied each group through the index its leader answered. A no-op with no
/// groups.
pub(crate) async fn close_group_cuts(
    state: &SharedState,
    groups: &[u64],
    cut: i64,
    deadline: StatementDeadline,
) -> crate::Result<()> {
    if groups.is_empty() {
        return Ok(());
    }
    let closes = groups.iter().map(|&group_id| async move {
        let read_index = cut_read_index(state, group_id, cut, deadline).await?;
        wait_applied_through(state, group_id, read_index, deadline)
            .await
            .map_err(|error| past_deadline_or(deadline, error))
    });
    futures::future::try_join_all(closes).await.map(|_| ())
}

/// Close `cut` on the leader of each of `groups`, wherever it runs, with no
/// wait for this node's own replicas. Each leader folds `cut` into its
/// ordinal clock, waits until every write it reserved at or below `cut`
/// applied there, and raises the group's ordinal floor to `cut` on a quorum.
/// So every write any leader of the group admits later stamps above `cut`.
/// A no-op with no groups.
pub(crate) async fn close_leader_cuts(
    state: &SharedState,
    groups: &[u64],
    cut: i64,
    deadline: StatementDeadline,
) -> crate::Result<()> {
    let closes = groups
        .iter()
        .map(|&group_id| cut_read_index(state, group_id, cut, deadline));
    futures::future::try_join_all(closes).await.map(|_| ())
}

/// A read index of `group_id` taken after its leader closed `cut`. A refused
/// round asks again until `deadline`.
async fn cut_read_index(
    state: &SharedState,
    group_id: u64,
    cut: i64,
    deadline: StatementDeadline,
) -> crate::Result<u64> {
    let Some(gate) = state.raft_read_gate.get() else {
        return Err(crate::Error::LinearizableReadRefused {
            group_id,
            detail: "raft is not serving on this node yet".into(),
        });
    };
    loop {
        let round = ReadIndexDeadline::of_round(state, deadline);
        let asked_at = std::time::Instant::now();
        let asked = gate.cut_read_index(group_id, round.remaining(), cut);
        let answer =
            tokio::time::timeout_at(tokio::time::Instant::from_std(round.instant()), asked).await;
        let refusal = match answer {
            Ok(Ok(read_index)) => return Ok(read_index),
            Ok(Err(refusal)) => refusal,
            Err(_) => ReadIndexRefusal::Timeout {
                waited_ms: u64::try_from(asked_at.elapsed().as_millis()).unwrap_or(u64::MAX),
            },
        };
        if deadline.remaining().is_zero() {
            tracing::warn!(
                group_id,
                cut,
                ?refusal,
                "read cut: the group's leader did not close the cut within the statement deadline"
            );
            return Err(deadline_exceeded());
        }
        if matches!(refusal, ReadIndexRefusal::NotLeader) {
            let pause = RETRY_PAUSE.min(deadline.remaining());
            tokio::time::sleep(pause).await;
        }
    }
}

/// `error`, or `Error::DeadlineExceeded` once `deadline` passed.
fn past_deadline_or(deadline: StatementDeadline, error: crate::Error) -> crate::Error {
    if deadline.remaining().is_zero() {
        deadline_exceeded()
    } else {
        error
    }
}

fn deadline_exceeded() -> crate::Error {
    crate::Error::DeadlineExceeded {
        request_id: crate::types::RequestId::new(0),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Instant;

    use async_trait::async_trait;

    use super::*;
    use crate::bridge::dispatch::Dispatcher;
    use crate::control::cluster::read_index::RaftReadGate;
    use crate::wal::WalManager;

    const GROUP: u64 = 4;

    fn test_state(dir: &tempfile::TempDir) -> Arc<SharedState> {
        let wal = Arc::new(WalManager::open_for_testing(&dir.path().join("cut.wal")).expect("wal"));
        let (dispatcher, _sides) = Dispatcher::new(1, 64);
        SharedState::new(dispatcher, wal).expect("shared state")
    }

    /// A leader that answers read index `index` for every cut.
    struct LeaderAt {
        index: u64,
    }

    #[async_trait]
    impl RaftReadGate for LeaderAt {
        async fn confirm_leader(
            &self,
            _group_id: u64,
            _timeout: Duration,
        ) -> Result<u64, ReadIndexRefusal> {
            Ok(self.index)
        }

        fn within_staleness_bound(&self, _: u64, _: Duration, _: u64) -> bool {
            false
        }

        fn holds_leader_lease(&self, _group_id: u64) -> bool {
            false
        }

        fn leader_lease_term(&self, _group_id: u64) -> Option<u64> {
            None
        }
    }

    /// A follower answers a read at the cut only once it applied through
    /// the index the leader took after closing the cut.
    #[tokio::test]
    async fn a_follower_waits_until_it_applied_the_leaders_index() {
        let dir = tempfile::tempdir().expect("tempdir");
        let state = test_state(&dir);
        assert!(
            state
                .raft_read_gate
                .set(Arc::new(LeaderAt { index: 9 }) as Arc<dyn RaftReadGate>)
                .is_ok()
        );
        let applier = {
            let state = Arc::clone(&state);
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(60)).await;
                state.applied_index_watcher(GROUP).bump(9);
            })
        };
        let started = Instant::now();
        close_group_cuts(
            &state,
            &[GROUP],
            100,
            StatementDeadline::after(Duration::from_secs(5)),
        )
        .await
        .expect("applied through the leader's index");
        assert!(started.elapsed() >= Duration::from_millis(50));
        applier.await.expect("join");
    }

    #[tokio::test]
    async fn a_follower_that_never_applies_fails_at_the_deadline() {
        let dir = tempfile::tempdir().expect("tempdir");
        let state = test_state(&dir);
        assert!(
            state
                .raft_read_gate
                .set(Arc::new(LeaderAt { index: 9 }) as Arc<dyn RaftReadGate>)
                .is_ok()
        );
        let closed = close_group_cuts(
            &state,
            &[GROUP],
            100,
            StatementDeadline::after(Duration::from_millis(100)),
        )
        .await;
        assert!(
            matches!(closed, Err(crate::Error::DeadlineExceeded { .. })),
            "{closed:?}"
        );
    }

    /// The local close waits for a write reserved below the cut, and ends
    /// at the deadline while it stays open.
    #[tokio::test]
    async fn the_local_close_waits_for_a_reserved_write() {
        let dir = tempfile::tempdir().expect("tempdir");
        let state = test_state(&dir);
        let clock = state.wal.ordinal_clock();
        let (ordinal, guard) = state
            .read_cut
            .reservations
            .reserve_with(|| Some(clock.next_ordinal()));
        let cut = ordinal.expect("drawn");
        let held = close_local_cut(
            &state,
            cut,
            StatementDeadline::after(Duration::from_millis(30)),
        )
        .await;
        assert!(
            matches!(held, Err(crate::Error::DeadlineExceeded { .. })),
            "{held:?}"
        );
        drop(guard);
        close_local_cut(
            &state,
            cut,
            StatementDeadline::after(Duration::from_secs(1)),
        )
        .await
        .expect("the write applied");
    }
}
