// SPDX-License-Identifier: BUSL-1.1

//! The Control Plane side of the graph node and edge gauges.
//!
//! Each Data Plane core counts its own share of the graph
//! (`MetaOp::RefreshGraphCount`). A core cannot tell which vShards its node
//! hosts. A node stores a dual-homed edge on both endpoint homes, so a node
//! that hosts only the destination's home still holds the source node. Each
//! pass therefore sends every local core the vShards this node hosts, and a
//! core counts only those homes.

use std::time::Duration;

use nodedb_physical::physical_plan::{MetaOp, PhysicalPlan};

use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId, TraceId, VShardId};

/// How often the gauges are counted again. A count walks every node and edge
/// of every core's CSR, so it runs on this period, never per write.
pub const GRAPH_COUNT_PERIOD: Duration = Duration::from_secs(10);

/// The vShards this node hosts: every vShard of each Raft group this node is
/// a member of. A node with no cluster routing hosts every vShard.
pub fn hosted_vshards(state: &SharedState) -> Vec<u32> {
    match state.cluster_routing.as_ref() {
        Some(routing) => {
            crate::control::cluster::start_raft_helpers::hosted_vshards(routing, state.node_id)
        }
        None => (0..VShardId::COUNT).collect(),
    }
}

/// Send every local core the vShards this node hosts. Each core counts its
/// share against that set and moves the gauges by the change.
pub async fn refresh_graph_count(state: &SharedState) -> crate::Result<()> {
    let plan = PhysicalPlan::Meta(MetaOp::RefreshGraphCount {
        hosted_vshards: hosted_vshards(state),
    });
    crate::control::server::broadcast::broadcast_to_all_cores(
        state,
        TenantId::new(0),
        DatabaseId::DEFAULT,
        plan,
        TraceId::generate(),
    )
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, RwLock};

    use nodedb_cluster::RoutingTable;

    use super::*;
    use crate::bridge::dispatch::Dispatcher;
    use crate::wal::WalManager;

    fn state(dir: &std::path::Path) -> Arc<SharedState> {
        let wal = Arc::new(WalManager::open_for_testing(&dir.join("graph.wal")).expect("wal"));
        let (dispatcher, _sides) = Dispatcher::new(1, 64);
        SharedState::new(dispatcher, wal).expect("shared state")
    }

    /// A node with no cluster routing hosts every vShard.
    #[test]
    fn a_single_node_hosts_every_vshard() {
        let dir = tempfile::tempdir().expect("tempdir");
        let state = state(dir.path());
        let expected: Vec<u32> = (0..VShardId::COUNT).collect();
        assert_eq!(hosted_vshards(&state), expected);
    }

    /// In a cluster a node hosts only the vShards of the groups it is a
    /// member of.
    #[test]
    fn a_cluster_node_hosts_only_its_groups_vshards() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut state = state(dir.path());
        // Two data groups at RF 1: node 1 holds group 1, node 2 holds group 2.
        // Group 1 holds the even vShards.
        let routing = Arc::new(RwLock::new(RoutingTable::uniform(2, &[1, 2], 1)));
        let shared = Arc::get_mut(&mut state).expect("sole owner of fresh state");
        shared.cluster_routing = Some(routing);
        shared.node_id = 1;

        let expected: Vec<u32> = (0..VShardId::COUNT).filter(|v| v % 2 == 0).collect();
        assert_eq!(hosted_vshards(&state), expected);
    }
}
