//! [`ElectionGate`] adapter backed by [`MultiRaft`].
//!
//! Returns `true` (gate closed) when any Raft group on this node is
//! in the `Candidate` role, meaning an election is in progress.
//! The rebalancer driver will skip its sweep while this gate is open,
//! avoiding wasted migration work during leadership instability.

use std::sync::{Arc, Mutex, Weak};

use async_trait::async_trait;

use crate::multi_raft::MultiRaft;
use crate::rebalancer::driver::ElectionGate;

/// Wraps [`MultiRaft`] via a `Weak` reference and reports whether any
/// group is currently electing.
///
/// Uses `Weak` rather than `Arc` so the outer `start_cluster` bootstrap
/// path can still take ownership of the `Arc<Mutex<MultiRaft>>` to
/// reconstruct the plain `MultiRaft` for passing to `RaftLoop::new`.
/// If the `Weak` upgrade fails (should not happen in production), the
/// gate returns `false` so the rebalancer does not stall.
pub struct MultiRaftElectionGate {
    multi_raft: Weak<Mutex<MultiRaft>>,
}

impl MultiRaftElectionGate {
    pub fn new(multi_raft: &Arc<Mutex<MultiRaft>>) -> Self {
        Self {
            multi_raft: Arc::downgrade(multi_raft),
        }
    }
}

#[async_trait]
impl ElectionGate for MultiRaftElectionGate {
    async fn any_group_electing(&self) -> bool {
        let Some(arc) = self.multi_raft.upgrade() else {
            // MultiRaft was dropped — no elections possible.
            return false;
        };
        let guard = arc.lock().unwrap_or_else(|p| p.into_inner());
        guard
            .group_statuses()
            .iter()
            .any(|s| s.role.contains("Candidate"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn _assert_send_sync<T: Send + Sync>() {}

    #[test]
    fn multiraft_election_gate_is_send_sync() {
        _assert_send_sync::<MultiRaftElectionGate>();
    }
}
