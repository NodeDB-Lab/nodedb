// SPDX-License-Identifier: BUSL-1.1

//! Making a linearizable read safe on the node that serves it.
//!
//! A read observes every write committed before it began only when the node
//! that runs it has applied its group through a read index taken after the
//! read began. The read index comes from the group's leader: this node when it
//! leads (under its lease or a quorum round), the leader over the transport
//! otherwise. Every path that serves a strong read calls
//! [`confirm_linearizable_read`] on the serving node before it reads.
//!
//! The two steps run under different bounds (see [`super::read_deadline`]):
//! - The quorum round for the read index ends at the transport RPC timeout,
//!   so a partition refuses the read quickly.
//! - This node's wait to apply through the index follows the statement
//!   deadline, so a lagging apply delays the read instead of refusing it.

use nodedb_cluster::WaitOutcome;

use crate::control::cluster::read_index::ReadIndexRefusal;
use crate::control::state::SharedState;

pub use super::read_deadline::{ReadIndexDeadline, StatementDeadline};

/// The Raft groups that hold `vshard_ids`, deduplicated. Empty on a node with
/// no routing table: without a cluster there is one copy and nothing to prove.
pub fn groups_of_vshards(
    state: &SharedState,
    vshard_ids: impl IntoIterator<Item = u32>,
) -> crate::Result<Vec<u64>> {
    let Some(routing) = state.cluster_routing.as_ref() else {
        return Ok(Vec::new());
    };
    let routing = routing.read().unwrap_or_else(|p| p.into_inner());
    let mut groups: Vec<u64> = Vec::new();
    for vshard_id in vshard_ids {
        let group_id = routing
            .group_for_vshard(vshard_id)
            .map_err(|_| crate::Error::NoLeader {
                vshard_id: crate::types::VShardId::new(vshard_id),
            })?;
        if !groups.contains(&group_id) {
            groups.push(group_id);
        }
    }
    Ok(groups)
}

/// Every data group this node replicates, as a voter or a learner. A read that
/// fans across all local cores observes each of them. The metadata group holds
/// no user rows, so it is left out.
pub fn groups_hosted_here(state: &SharedState) -> Vec<u64> {
    let Some(routing) = state.cluster_routing.as_ref() else {
        return Vec::new();
    };
    let routing = routing.read().unwrap_or_else(|p| p.into_inner());
    routing
        .group_ids()
        .into_iter()
        .filter(|&group_id| group_id != nodedb_cluster::METADATA_GROUP_ID)
        .filter(|&group_id| hosts_group(state, &routing, group_id))
        .collect()
}

/// The groups that hold `vshard_ids` and that this node replicates.
///
/// A read on this node's cores observes only groups this node replicates. A
/// group held elsewhere has no rows here and never applies here, so it is
/// left out.
pub fn hosted_groups_of_vshards(
    state: &SharedState,
    vshard_ids: impl IntoIterator<Item = u32>,
) -> crate::Result<Vec<u64>> {
    let groups = groups_of_vshards(state, vshard_ids)?;
    let Some(routing) = state.cluster_routing.as_ref() else {
        return Ok(groups);
    };
    let routing = routing.read().unwrap_or_else(|p| p.into_inner());
    Ok(groups
        .into_iter()
        .filter(|&group_id| hosts_group(state, &routing, group_id))
        .collect())
}

fn hosts_group(state: &SharedState, routing: &nodedb_cluster::RoutingTable, group_id: u64) -> bool {
    routing.group_info(group_id).is_some_and(|info| {
        info.members.contains(&state.node_id) || info.learners.contains(&state.node_id)
    })
}

/// Make a linearizable read of `groups` safe to serve on this node.
///
/// Each group gets a read index taken after this call, then this node waits
/// until it has applied the group through that index. The groups run
/// concurrently. Each quorum round ends at the transport RPC timeout, and the
/// apply waits end at `deadline`.
pub async fn confirm_linearizable_read(
    state: &SharedState,
    groups: &[u64],
    deadline: StatementDeadline,
) -> crate::Result<()> {
    if groups.is_empty() {
        return Ok(());
    }
    let Some(gate) = state.raft_read_gate.get() else {
        // A routing table without a gate: `start_raft` has not published it.
        return Err(refused(groups[0], "raft is not serving on this node yet"));
    };
    let round = ReadIndexDeadline::of_round(state, deadline);
    let reads = groups.iter().map(|&group_id| async move {
        let read_index = read_index_within(&**gate, group_id, round).await?;
        wait_applied_through(state, group_id, read_index, deadline).await
    });
    futures::future::try_join_all(reads).await.map(|_| ())
}

/// A read index of `group_id` from its leader, refused once `round` passes.
async fn read_index_within(
    gate: &dyn crate::control::cluster::read_index::RaftReadGate,
    group_id: u64,
    round: ReadIndexDeadline,
) -> crate::Result<u64> {
    let asked = gate.read_index(group_id, round.remaining());
    let answer = tokio::time::timeout_at(tokio::time::Instant::from_std(round.instant()), asked)
        .await
        .map_err(|_| refused(group_id, "no quorum confirmed a read index in time"))?;
    answer.map_err(|refusal| match refusal {
        ReadIndexRefusal::NotLeader => refused(group_id, "no leader confirmed a read index"),
        ReadIndexRefusal::Timeout { waited_ms } => refused(
            group_id,
            &format!("no quorum confirmed a read index within {waited_ms}ms"),
        ),
    })
}

/// Wait until this node has applied `group_id` through `read_index`, or
/// refuse at `deadline`.
///
/// A replica here that awaits a snapshot install applies nothing until one
/// installs, so its read is refused at once.
pub(crate) async fn wait_applied_through(
    state: &SharedState,
    group_id: u64,
    read_index: u64,
    deadline: StatementDeadline,
) -> crate::Result<()> {
    let watcher = state.applied_index_watcher(group_id);
    if watcher.current() >= read_index {
        return Ok(());
    }
    if awaits_snapshot(state, group_id) {
        return Err(refused(
            group_id,
            "this replica awaits a snapshot install and applies nothing until one installs",
        ));
    }
    match watcher.wait_for_async(read_index, deadline.instant()).await {
        WaitOutcome::Reached => Ok(()),
        WaitOutcome::TimedOut => Err(refused(
            group_id,
            &format!("this node did not apply through read index {read_index} in time"),
        )),
        WaitOutcome::GroupGone => Err(refused(group_id, "the group left this node")),
    }
}

/// Whether this node's replica of `group_id` awaits a snapshot install: its
/// data diverged from the log.
fn awaits_snapshot(state: &SharedState, group_id: u64) -> bool {
    state.multi_raft.get().is_some_and(|multi_raft| {
        multi_raft
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .replica_state(group_id)
            == Some(nodedb_raft::ReplicaState::AwaitingSnapshot)
    })
}

fn refused(group_id: u64, detail: &str) -> crate::Error {
    crate::Error::LinearizableReadRefused {
        group_id,
        detail: detail.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use async_trait::async_trait;

    use super::*;
    use crate::bridge::dispatch::Dispatcher;
    use crate::control::cluster::read_index::RaftReadGate;
    use crate::wal::WalManager;

    const GROUP: u64 = 3;

    fn test_state(dir: &tempfile::TempDir) -> Arc<SharedState> {
        let wal = Arc::new(
            WalManager::open_for_testing(&dir.path().join("linearizable.wal")).expect("wal"),
        );
        let (dispatcher, _sides) = Dispatcher::new(1, 64);
        SharedState::new(dispatcher, wal).expect("shared state")
    }

    /// A gate whose quorum rounds never answer.
    struct SilentGate;

    #[async_trait]
    impl RaftReadGate for SilentGate {
        async fn confirm_leader(
            &self,
            _group_id: u64,
            _timeout: Duration,
        ) -> Result<u64, ReadIndexRefusal> {
            std::future::pending().await
        }

        fn within_staleness_bound(
            &self,
            _group_id: u64,
            _max_staleness: Duration,
            _applied: u64,
        ) -> bool {
            false
        }

        fn holds_leader_lease(&self, _group_id: u64) -> bool {
            false
        }

        fn leader_lease_term(&self, _group_id: u64) -> Option<u64> {
            None
        }
    }

    /// A lagging apply delays the read: the wait follows the statement
    /// deadline, not a short fixed bound.
    #[tokio::test]
    async fn the_apply_wait_follows_the_statement_deadline() {
        let dir = tempfile::tempdir().expect("tempdir");
        let state = test_state(&dir);
        let applier = {
            let state = Arc::clone(&state);
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(1_500)).await;
                state.applied_index_watcher(GROUP).bump(9);
            })
        };
        let deadline = StatementDeadline::after(Duration::from_secs(5));
        let waited = wait_applied_through(&state, GROUP, 9, deadline).await;
        applier.await.expect("join applier");
        waited.expect("the read waits out the lag inside the statement deadline");
    }

    /// An apply that never reaches the read index is refused at the
    /// statement deadline.
    #[tokio::test]
    async fn an_apply_that_never_arrives_is_refused_at_the_deadline() {
        let dir = tempfile::tempdir().expect("tempdir");
        let state = test_state(&dir);
        let started = Instant::now();
        let budget = Duration::from_millis(300);
        let waited = wait_applied_through(&state, GROUP, 9, StatementDeadline::after(budget)).await;
        assert!(
            matches!(waited, Err(crate::Error::LinearizableReadRefused { .. })),
            "{waited:?}"
        );
        assert!(started.elapsed() >= budget);
    }

    /// A read index no quorum confirms is refused at the transport RPC
    /// timeout, long before a 30 s statement deadline.
    #[tokio::test]
    async fn a_read_index_with_no_quorum_is_refused_at_the_rpc_bound() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut state = test_state(&dir);
        Arc::get_mut(&mut state)
            .expect("sole owner of fresh state")
            .tuning
            .cluster_transport
            .rpc_timeout_secs = 1;
        assert!(
            state
                .raft_read_gate
                .set(Arc::new(SilentGate) as Arc<dyn RaftReadGate>)
                .is_ok()
        );
        let started = Instant::now();
        let deadline = StatementDeadline::after(Duration::from_secs(30));
        let read = confirm_linearizable_read(&state, &[GROUP], deadline).await;
        let waited = started.elapsed();
        assert!(
            matches!(read, Err(crate::Error::LinearizableReadRefused { .. })),
            "{read:?}"
        );
        assert!(waited >= Duration::from_secs(1), "{waited:?}");
        assert!(waited < Duration::from_secs(10), "{waited:?}");
    }
}
