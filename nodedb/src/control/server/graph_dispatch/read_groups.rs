// SPDX-License-Identifier: BUSL-1.1

//! The data groups a graph plan reads on this node's cores.
//!
//! A graph edge lives on the key vShard of each endpoint, and a node document
//! lives on its collection's home vShard (`types/record_home.rs`). A plan's
//! read set follows from what it expands:
//!
//! - A one-hop lookup (`Neighbors`, `NeighborsMulti`, `TemporalNeighbors`)
//!   reads the edges of the nodes it names. Its set is the key vShard of each
//!   named node, plus the collection home for node documents and RLS checks.
//! - Every other graph read walks the local CSR from node to node. A MATCH
//!   continuation keeps expanding locally until a node has no local edges
//!   (`engine/graph/pattern/executor/overlay_expand.rs`). Hop, Path and
//!   Subgraph traverse to any depth. Algorithms, BSP supersteps and stats scan
//!   the whole partition. Such a plan can read any key vShard held here, so its
//!   set is every group this node replicates.
//! - A gathered algorithm (`AlgoStage::Gathered`) runs over the edges its plan
//!   carries and reads no group.
//!
//! Only groups this node replicates count. The local cores hold no rows of any
//! other group.

use nodedb_physical::physical_plan::{AlgoStage, GraphOp};
use nodedb_types::{CollectionKey, QualifiedCollection};

use crate::bridge::envelope::PhysicalPlan;
use crate::control::cluster::linearizable_read::{
    confirm_linearizable_read, groups_hosted_here, hosted_groups_of_vshards,
    statement_read_deadline,
};
use crate::control::state::SharedState;
use crate::types::{DatabaseId, VShardId};

/// The groups `plan` reads when it runs on this node's cores.
pub fn graph_read_groups(
    state: &SharedState,
    database_id: DatabaseId,
    plan: &PhysicalPlan,
) -> crate::Result<Vec<u64>> {
    let PhysicalPlan::Graph(op) = plan else {
        return Ok(groups_hosted_here(state));
    };
    match op {
        GraphOp::Neighbors {
            collection,
            node_id,
            ..
        } => keyed_groups(
            state,
            database_id,
            collection.as_ref(),
            std::iter::once(node_id.as_str()),
        ),
        GraphOp::TemporalNeighbors {
            collection,
            node_id,
            ..
        } => keyed_groups(
            state,
            database_id,
            Some(collection),
            std::iter::once(node_id.as_str()),
        ),
        GraphOp::NeighborsMulti {
            collection,
            node_ids,
            ..
        } => keyed_groups(
            state,
            database_id,
            collection.as_ref(),
            node_ids.iter().map(String::as_str),
        ),
        // A presence read reads the documents on the vShard it names.
        GraphOp::NodePresenceRead { vshard, .. } => {
            hosted_groups_of_vshards(state, std::iter::once(*vshard))
        }
        // A gathered algorithm runs over edges the plan carries and reads no
        // group of this node.
        GraphOp::Algo {
            stage: AlgoStage::Gathered { .. },
            ..
        } => Ok(Vec::new()),
        _ => Ok(groups_hosted_here(state)),
    }
}

/// Confirm the groups `plan` reads on this node's cores, within the running
/// statement's budget.
pub async fn confirm_graph_read(
    state: &SharedState,
    database_id: DatabaseId,
    plan: &PhysicalPlan,
) -> crate::Result<()> {
    let groups = graph_read_groups(state, database_id, plan)?;
    confirm_linearizable_read(state, &groups, statement_read_deadline(state)).await
}

fn keyed_groups<'a>(
    state: &SharedState,
    database_id: DatabaseId,
    collection: Option<&QualifiedCollection>,
    node_keys: impl Iterator<Item = &'a str>,
) -> crate::Result<Vec<u64>> {
    let home = match collection {
        Some(collection) => Some(
            CollectionKey::from_qualified(database_id, collection)?
                .vshard()
                .as_u32(),
        ),
        None => None,
    };
    let key_vshards = node_keys.map(|key| VShardId::from_key(key.as_bytes()).as_u32());
    hosted_groups_of_vshards(state, home.into_iter().chain(key_vshards))
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, RwLock};

    use nodedb_cluster::RoutingTable;

    use super::*;
    use crate::bridge::dispatch::Dispatcher;
    use crate::engine::graph::edge_store::Direction;
    use crate::wal::WalManager;

    const THIS_NODE: u64 = 1;
    const OTHER_NODE: u64 = 2;

    /// A node that replicates every group except `foreign_group`, which only
    /// `OTHER_NODE` holds.
    fn state_with_routing(foreign_group: u64) -> (Arc<SharedState>, tempfile::TempDir) {
        let directory = tempfile::tempdir().expect("temporary WAL directory");
        let wal = Arc::new(
            WalManager::open_for_testing(&directory.path().join("graph.wal")).expect("test WAL"),
        );
        let (dispatcher, _sides) = Dispatcher::new(1, 8);
        let mut state = SharedState::new(dispatcher, wal).expect("shared state");
        let mut routing = RoutingTable::uniform(4, &[THIS_NODE, OTHER_NODE], 2);
        for group_id in routing.group_ids() {
            let members = if group_id == foreign_group {
                vec![OTHER_NODE]
            } else {
                vec![THIS_NODE, OTHER_NODE]
            };
            routing.set_group_members(group_id, members);
        }
        let shared = Arc::get_mut(&mut state).expect("sole owner of fresh state");
        shared.node_id = THIS_NODE;
        shared.cluster_routing = Some(Arc::new(RwLock::new(routing)));
        (state, directory)
    }

    fn group_of_key(state: &SharedState, key: &str) -> u64 {
        let routing = state
            .cluster_routing
            .as_ref()
            .expect("routing")
            .read()
            .expect("lock");
        routing
            .group_for_vshard(VShardId::from_key(key.as_bytes()).as_u32())
            .expect("group of key")
    }

    fn neighbors_multi(node_ids: &[&str]) -> PhysicalPlan {
        PhysicalPlan::Graph(GraphOp::NeighborsMulti {
            collection: None,
            node_ids: node_ids.iter().map(|id| (*id).to_owned()).collect(),
            edge_labels: Vec::new(),
            direction: Direction::Out,
            max_results: 0,
            rls_filters: Vec::new(),
            edge_predicate: Vec::new(),
            with_properties: false,
        })
    }

    #[test]
    fn a_one_hop_lookup_reads_only_the_key_groups_of_its_nodes() {
        let (state, _dir) = state_with_routing(u64::MAX);
        let key_group = group_of_key(&state, "alice");
        let groups = graph_read_groups(&state, DatabaseId::DEFAULT, &neighbors_multi(&["alice"]))
            .expect("read groups");
        assert_eq!(groups, vec![key_group]);
        assert!(groups_hosted_here(&state).len() > 1);
    }

    #[test]
    fn a_key_group_held_elsewhere_is_left_out() {
        let (probe, _dir) = state_with_routing(u64::MAX);
        let key_group = group_of_key(&probe, "alice");
        let (state, _dir) = state_with_routing(key_group);
        let groups = graph_read_groups(&state, DatabaseId::DEFAULT, &neighbors_multi(&["alice"]))
            .expect("read groups");
        assert!(groups.is_empty());
    }

    #[test]
    fn a_walking_plan_reads_every_group_held_here() {
        let (state, _dir) = state_with_routing(u64::MAX);
        let plan = PhysicalPlan::Graph(GraphOp::Match {
            query: Vec::new(),
            frontier_bitmap: None,
            cluster_mode: true,
        });
        let mut groups =
            graph_read_groups(&state, DatabaseId::DEFAULT, &plan).expect("read groups");
        let mut hosted = groups_hosted_here(&state);
        groups.sort_unstable();
        hosted.sort_unstable();
        assert_eq!(groups, hosted);
    }
}
