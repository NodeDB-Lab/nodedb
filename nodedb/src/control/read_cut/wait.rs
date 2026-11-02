// SPDX-License-Identifier: BUSL-1.1

//! The wait every node does before it serves a historical read.
//!
//! A historical statement pins one cut: `AS OF SYSTEM TIME`, capped at the
//! statement's read cut (`super::statement_cap`). Every plan of the
//! statement carries the ordinal and the Calvin cut marker: a graph read in
//! its `GraphReadScope`, and a document, columnar, timeseries, or array read
//! in its `SystemTimeScope::Pinned`. A node reads at the ordinal only once
//! the marker applied there and every Calvin scheduler it runs passed it, so
//! every Calvin version sequenced before the marker is installed on that
//! node. It then closes the cut (`super::close`): it folds the ordinal into
//! its ordinal clock, so no write it stamps later lands at or below the cut,
//! waits until every write it reserved at or below the cut applied, and
//! applies each group the read observes here through a read index its
//! leader took after closing the cut there. A node that has not installed
//! them answers with versions missing, so the wait is never skipped: it runs
//! on the coordinator's local cores and on every remote owner, and it ends
//! at the statement deadline with `Error::DeadlineExceeded`, never with a
//! partial answer.

use nodedb_physical::physical_plan::{
    GraphReadScope, GraphSystemCut, PhysicalPlan, plan_contains_cluster_partitioned_leaf,
};

use crate::control::cluster::linearizable_read::hosted_groups_of_vshards;
use crate::control::server::exchange::all_cores::read_cut::resolve_read_cut_by;
use crate::control::server::shared::session::StatementDeadline;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::close::{close_group_cuts, close_local_cut};

/// Wait until this node installed every version at or below each
/// historical cut `plan` reads at, or until `deadline`. A live plan and a
/// cut no marker caps return at once.
///
/// The marker wait installs every Calvin version sequenced before the
/// marker. The cut can sit above the node's Calvin cut: the statement's cap
/// holds a clock part for writes outside Calvin. The wait then closes the
/// cut here and on the leader of each of `groups` (`super::close`), so the
/// read sees every write at or below the cut and none above it.
///
/// `groups` are the Raft groups whose replicas on this node the plan reads:
/// [`cut_read_groups`] computes them.
pub(crate) async fn await_read_cut(
    state: &SharedState,
    plan: &PhysicalPlan,
    groups: &[u64],
    deadline: StatementDeadline,
) -> crate::Result<()> {
    for (ordinal, marker) in plan_marked_cuts(plan) {
        resolve_read_cut_by(state, marker, ordinal, deadline).await?;
        close_local_cut(state, ordinal, deadline).await?;
        close_group_cuts(state, groups, ordinal, deadline).await?;
    }
    Ok(())
}

/// The Raft groups whose replicas on this node `plan` reads, for
/// [`await_read_cut`]. Empty for a plan that reads no marked cut, and on a
/// node with no routing table.
///
/// A graph plan or a cluster-partitioned plan fans across every local core,
/// so it reads the groups `graph_dispatch::graph_read_groups` names. Any
/// other plan reads the group of `vshard_id`, or of its home collection's
/// vShard when the caller names none.
pub(crate) fn cut_read_groups(
    state: &SharedState,
    database_id: DatabaseId,
    plan: &PhysicalPlan,
    vshard_id: Option<u32>,
) -> crate::Result<Vec<u64>> {
    if state.cluster_routing.is_none() || plan_marked_cuts(plan).is_empty() {
        return Ok(Vec::new());
    }
    if matches!(plan, PhysicalPlan::Graph(_)) || plan_contains_cluster_partitioned_leaf(plan) {
        return crate::control::server::graph_dispatch::graph_read_groups(state, database_id, plan);
    }
    let vshard_id = match vshard_id {
        Some(vshard_id) => vshard_id,
        None => home_vshard(database_id, plan)?,
    };
    hosted_groups_of_vshards(state, [vshard_id])
}

/// The vShard `plan`'s home collection lives on, as the gateway router homes
/// it. vShard `0` for a plan that names no collection.
fn home_vshard(database_id: DatabaseId, plan: &PhysicalPlan) -> crate::Result<u32> {
    match crate::control::gateway::home_collection::home_collection(plan) {
        Some(name) => Ok(nodedb_cluster::routing::vshard_for_collection(
            nodedb_types::id::CollectionKey::from_qualified_str(database_id, &name)?,
        )),
        None => Ok(0),
    }
}

/// Each marker that caps a cut of `plan`, with the newest ordinal the plan
/// reads at under it. Empty when no marker caps a cut of the plan.
fn plan_marked_cuts(plan: &PhysicalPlan) -> Vec<(i64, u64)> {
    let graph_cut = if let PhysicalPlan::Graph(op) = plan {
        op.read_scope().and_then(marked_cut)
    } else {
        None
    };
    let scope_cuts = crate::control::planner::system_cut::plan_scopes(plan)
        .into_iter()
        .filter_map(|scope| scope.marked_cut());
    let mut cuts: Vec<(i64, u64)> = Vec::new();
    for (ordinal, marker) in graph_cut.into_iter().chain(scope_cuts) {
        match cuts.iter_mut().find(|(_, seen)| *seen == marker) {
            Some((newest, _)) => *newest = (*newest).max(ordinal),
            None => cuts.push((ordinal, marker)),
        }
    }
    cuts
}

/// The ordinal and the marker of `scope`, when a marker caps an `AtOrdinal`
/// or a `TxnSnapshot` cut.
fn marked_cut(scope: &GraphReadScope) -> Option<(i64, u64)> {
    match scope.system {
        GraphSystemCut::AtOrdinal(ordinal) | GraphSystemCut::TxnSnapshot(ordinal)
            if scope.cut_marker != 0 =>
        {
            Some((ordinal, scope.cut_marker))
        }
        GraphSystemCut::AtOrdinal(_)
        | GraphSystemCut::TxnSnapshot(_)
        | GraphSystemCut::Live
        | GraphSystemCut::AllVersions => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_physical::physical_plan::GraphOp;

    fn scope(system: GraphSystemCut, cut_marker: u64) -> GraphReadScope {
        GraphReadScope {
            system,
            valid_at_ms: None,
            cut_marker,
        }
    }

    #[test]
    fn only_a_marked_historical_cut_waits() {
        assert_eq!(
            marked_cut(&scope(GraphSystemCut::AtOrdinal(9), 4)),
            Some((9, 4))
        );
        assert_eq!(marked_cut(&scope(GraphSystemCut::AtOrdinal(9), 0)), None);
        assert_eq!(marked_cut(&scope(GraphSystemCut::Live, 4)), None);
        assert_eq!(marked_cut(&scope(GraphSystemCut::AllVersions, 4)), None);
    }

    #[test]
    fn a_marked_neighbors_read_names_its_cut() {
        let plan = PhysicalPlan::Graph(GraphOp::Neighbors {
            collection: None,
            node_id: "a".into(),
            edge_labels: Vec::new(),
            direction: crate::engine::graph::edge_store::Direction::Out,
            rls_filters: Vec::new(),
            scope: scope(GraphSystemCut::AtOrdinal(11), 3),
        });
        assert_eq!(plan_marked_cuts(&plan), vec![(11, 3)]);
    }

    #[test]
    fn a_pinned_document_read_names_its_cut() {
        let plan = PhysicalPlan::Document(nodedb_physical::physical_plan::DocumentOp::PointGet {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "docs",
            ),
            document_id: "a".into(),
            surrogate: None,
            pk_bytes: Vec::new(),
            rls_filters: Vec::new(),
            system_time: nodedb_types::SystemTimeScope::Pinned {
                ordinal: 21,
                cut_marker: 5,
            },
            valid_at_ms: None,
        });
        assert_eq!(plan_marked_cuts(&plan), vec![(21, 5)]);
        let unmarked =
            PhysicalPlan::Document(nodedb_physical::physical_plan::DocumentOp::PointGet {
                collection: nodedb_types::QualifiedCollection::new(
                    nodedb_types::DatabaseId::DEFAULT,
                    "docs",
                ),
                document_id: "a".into(),
                surrogate: None,
                pk_bytes: Vec::new(),
                rls_filters: Vec::new(),
                system_time: nodedb_types::SystemTimeScope::AsOf(21),
                valid_at_ms: None,
            });
        assert!(plan_marked_cuts(&unmarked).is_empty());
    }

    /// A pinned array aggregate or elementwise fan-out names its cut, so the
    /// coordinator and every shard wait for the marker before they read.
    #[test]
    fn a_pinned_array_fan_out_names_its_cut() {
        use nodedb_physical::physical_plan::{ArrayBinaryOp, ClusterArrayOp};
        let pinned = nodedb_types::SystemTimeScope::Pinned {
            ordinal: 33,
            cut_marker: 6,
        };
        let array =
            |name: &str| nodedb_array::types::ArrayId::new(nodedb_types::TenantId::new(1), name);
        let agg = PhysicalPlan::ClusterArray(ClusterArrayOp::Agg {
            array_id: array("a"),
            attr_idx: 0,
            reducer_msgpack: Vec::new(),
            group_by_dim: -1,
            slice_tile_keys: Vec::new(),
            prefix_bits: 0,
            system_time: pinned,
            valid_at_ms: None,
            region_msgpack: Vec::new(),
            physical: false,
        });
        assert_eq!(plan_marked_cuts(&agg), vec![(33, 6)]);
        let elementwise = PhysicalPlan::ClusterArray(ClusterArrayOp::Elementwise {
            left: array("l"),
            right: array("r"),
            op: ArrayBinaryOp::Add,
            attr_idx: 0,
            prefix_bits: 0,
            system_time: pinned,
            valid_at_ms: None,
        });
        assert_eq!(plan_marked_cuts(&elementwise), vec![(33, 6)]);
    }
}
