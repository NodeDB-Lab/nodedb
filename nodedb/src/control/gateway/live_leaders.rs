// SPDX-License-Identifier: BUSL-1.1

//! Live Raft leadership snapshot for routing decisions.
//!
//! Lock order: take the Raft status first, then the routing guard.
//! `raft_status_fn` locks `MultiRaft`, and `MultiRaft` reads the routing table under that lock.
//! A caller that holds a routing guard while it takes Raft status inverts that order.
//! A queued routing writer then blocks the nested routing read, and the node deadlocks.
//! [`LiveLeaders::snapshot`] holds no routing guard, so callers take it before any guard.

use nodedb_cluster::{GroupStatus, RoutingTable};

use crate::control::state::SharedState;

use super::route::RouteDecision;
use super::router::resolve_decision;

/// Each hosted group's leader as this node's Raft knows it.
///
/// `None` means no Raft status source is wired. Routing then falls back to
/// the routing-table hint alone.
pub struct LiveLeaders {
    statuses: Option<Vec<GroupStatus>>,
}

impl LiveLeaders {
    /// Snapshot live leadership from `state.raft_status_fn`.
    ///
    /// The caller must not hold a `cluster_routing` guard. See the module docs.
    pub fn snapshot(state: &SharedState) -> Self {
        Self {
            statuses: state.raft_status_fn.get().map(|status| status()),
        }
    }

    /// Whether a Raft status source answered this snapshot.
    pub fn is_wired(&self) -> bool {
        self.statuses.is_some()
    }

    /// The live leader of `group_id`, or `0` when Raft knows none.
    pub fn leader_of(&self, group_id: u64) -> u64 {
        self.statuses
            .as_deref()
            .and_then(|statuses| statuses.iter().find(|s| s.group_id == group_id))
            .map_or(0, |s| s.leader_id)
    }

    /// This node's Raft term of `group_id`, or `0` when Raft knows no such
    /// group here. A leader's term is the term it leads in.
    pub fn term_of(&self, group_id: u64) -> u64 {
        self.statuses
            .as_deref()
            .and_then(|statuses| statuses.iter().find(|s| s.group_id == group_id))
            .map_or(0, |s| s.term)
    }

    /// Resolve `vshard_id` against this snapshot, with `routing` as the hint fallback.
    pub fn resolve(
        &self,
        vshard_id: u32,
        local_node_id: u64,
        routing: Option<&RoutingTable>,
    ) -> RouteDecision {
        let leader = |group_id: u64| self.leader_of(group_id);
        let live_lookup: Option<&dyn Fn(u64) -> u64> = if self.statuses.is_some() {
            Some(&leader)
        } else {
            None
        };
        resolve_decision(vshard_id, local_node_id, routing, live_lookup)
    }
}

/// Resolve `vshard_id` to a `RouteDecision` against live Raft leadership.
///
/// Takes the Raft snapshot first and the routing guard second. The guard drops on return.
pub fn resolve_live_decision(state: &SharedState, vshard_id: u32) -> RouteDecision {
    let live = LiveLeaders::snapshot(state);
    let routing = state
        .cluster_routing
        .as_ref()
        .map(|lock| lock.read().unwrap_or_else(|p| p.into_inner()));
    live.resolve(vshard_id, state.node_id, routing.as_deref())
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, RwLock};

    use super::*;
    use crate::bridge::dispatch::Dispatcher;
    use crate::wal::WalManager;

    /// A live leader that is neither this node nor the routing hint.
    const LIVE_LEADER: u64 = 7;

    fn status(group_id: u64) -> GroupStatus {
        GroupStatus {
            group_id,
            role: "Follower".into(),
            leader_id: LIVE_LEADER,
            term: 1,
            commit_index: 0,
            last_applied: 0,
            last_log_index: 0,
            snapshot_index: 0,
            replica_state: nodedb_raft::ReplicaState::Whole,
            member_count: 2,
            learner_count: 0,
            vshard_count: 0,
        }
    }

    /// Resolution takes the Raft status before the routing guard.
    ///
    /// Production Raft status locks `MultiRaft`, which re-reads routing under that lock.
    /// A caller that holds a routing read guard there deadlocks once a writer queues.
    /// The fake status source stands in for `MultiRaft`. It probes the routing lock with
    /// `try_write`, which fails while any read guard is held. A probe that fails proves
    /// the caller held a routing guard across the status call.
    #[test]
    fn raft_status_is_taken_with_no_routing_guard_held() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal =
            Arc::new(WalManager::open_for_testing(&dir.path().join("live.wal")).expect("wal"));
        let (dispatcher, _sides) = Dispatcher::new(1, 64);
        let mut state = SharedState::new(dispatcher, wal).expect("shared state");
        let routing = Arc::new(RwLock::new(RoutingTable::uniform(2, &[1, 2], 2)));
        Arc::get_mut(&mut state)
            .expect("sole owner of fresh state")
            .cluster_routing = Some(Arc::clone(&routing));
        let vshard_id = 0;
        let group_id = routing
            .read()
            .expect("routing lock")
            .group_for_vshard(vshard_id)
            .expect("the vShard has a group");

        let held_guard = Arc::new(AtomicBool::new(false));
        let probe_routing = Arc::clone(&routing);
        let probe_held = Arc::clone(&held_guard);
        let status_fn: Arc<dyn Fn() -> Vec<GroupStatus> + Send + Sync> = Arc::new(move || {
            if probe_routing.try_write().is_err() {
                probe_held.store(true, Ordering::SeqCst);
            }
            vec![status(group_id)]
        });
        assert!(state.raft_status_fn.set(status_fn).is_ok());

        let decision = resolve_live_decision(&state, vshard_id);
        assert!(
            !held_guard.load(Ordering::SeqCst),
            "the Raft status was read under a routing guard"
        );
        assert!(
            matches!(decision, RouteDecision::Remote { node_id, .. } if node_id == LIVE_LEADER),
            "live leadership routes the vShard: {decision:?}"
        );

        let live = LiveLeaders::snapshot(&state);
        assert!(!held_guard.load(Ordering::SeqCst));
        assert_eq!(live.leader_of(group_id), LIVE_LEADER);
    }
}
