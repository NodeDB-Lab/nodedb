// SPDX-License-Identifier: BUSL-1.1

//! The Raft read gate for a harness that runs as a single-node cluster.
//!
//! Production `start_raft` publishes a gate backed by the Raft loop. A
//! harness started with a routing table runs no Raft loop. Without a gate,
//! every linearizable read refuses with "no leader is currently serving
//! this range". This gate answers both questions the way a group with one
//! voter answers them.

use std::sync::{Arc, RwLock};
use std::time::Duration;

use nodedb::control::cluster::{RaftReadGate, ReadIndexRefusal};
use nodedb::control::state::SharedState;
use nodedb_cluster::RoutingTable;

/// Read gate for groups whose only voter is this node.
struct SingleVoterReadGate {
    node_id: u64,
    routing: Arc<RwLock<RoutingTable>>,
}

impl SingleVoterReadGate {
    /// Whether this node is the sole voter and the leader of `group_id`.
    fn is_sole_leader(&self, group_id: u64) -> bool {
        let routing = self.routing.read().unwrap_or_else(|p| p.into_inner());
        routing
            .group_info(group_id)
            .is_some_and(|info| info.leader == self.node_id && info.members == [self.node_id])
    }
}

#[async_trait::async_trait]
impl RaftReadGate for SingleVoterReadGate {
    /// A sole voter is its own quorum, so it confirms leadership at once.
    ///
    /// The harness keeps no Raft log, so the read index is `0`. The caller
    /// serves the read from local state.
    async fn confirm_leader(
        &self,
        group_id: u64,
        _timeout: Duration,
    ) -> Result<u64, ReadIndexRefusal> {
        if self.is_sole_leader(group_id) {
            Ok(0)
        } else {
            Err(ReadIndexRefusal::NotLeader)
        }
    }

    /// A sole voter holds the only copy, so it is never behind.
    fn within_staleness_bound(&self, group_id: u64, _max_staleness: Duration) -> bool {
        self.is_sole_leader(group_id)
    }
}

/// Publish the single-voter read gate when `shared` carries a routing table.
///
/// `raft_read_gate` is a `OnceLock`, so this runs once, after every
/// `Arc::get_mut` install.
pub(super) fn install_single_voter_read_gate(shared: &SharedState) {
    let Some(routing) = shared.cluster_routing.as_ref() else {
        return;
    };
    let gate: Arc<dyn RaftReadGate> = Arc::new(SingleVoterReadGate {
        node_id: shared.node_id,
        routing: Arc::clone(routing),
    });
    if shared.raft_read_gate.set(gate).is_err() {
        panic!("harness raft_read_gate installed twice");
    }
}
