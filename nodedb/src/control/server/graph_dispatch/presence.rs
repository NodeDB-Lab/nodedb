// SPDX-License-Identifier: BUSL-1.1

//! Graph node presence: whether a node exists in the graph at all.
//!
//! One core's graph holds a node exactly while an edge names it. A walk from
//! a node that graph does not hold finds nothing, so `GRAPH PATH` and
//! `GRAPH TRAVERSE` check their endpoints against this node set first.

use std::collections::HashSet;

use crate::control::state::SharedState;
use crate::engine::graph::edge_store::Direction;
use crate::engine::graph::traversal_options::GraphTraversalOptions;
use crate::types::{DatabaseId, TenantId, TxnId};

use super::hop::{NeighborHopParams, execute_neighbor_hop};
use super::shard_reads::ShardReadLog;

/// The tenancy scope of a presence check.
pub(crate) struct PresenceScope<'a> {
    pub tenant_id: TenantId,
    pub database_id: DatabaseId,
    pub options: &'a GraphTraversalOptions,
    pub linearizable: bool,
    /// The session's transaction. Presence counts its staged edge puts and
    /// tombstones.
    pub txn_id: Option<TxnId>,
}

/// The nodes of `nodes` that exist in the graph: those with an edge of any
/// label, in either direction, in any collection. One core's graph holds a
/// node exactly while an edge names it, so this is the node set one core's
/// walk checks its start against.
pub(crate) async fn graph_nodes_present(
    shared: &SharedState,
    scope: PresenceScope<'_>,
    nodes: &[String],
    reads: &mut ShardReadLog,
) -> crate::Result<HashSet<String>> {
    let hop = execute_neighbor_hop(
        shared,
        scope.tenant_id,
        scope.database_id,
        NeighborHopParams {
            collection: None,
            frontier: nodes,
            edge_labels: &[],
            direction: Direction::Both,
            options: scope.options,
            discovered_so_far: 0,
            linearizable: scope.linearizable,
            // Presence is a node's existence, whatever its edges carry.
            edge_predicate: &[],
            with_properties: false,
            txn_id: scope.txn_id,
        },
    )
    .await?;
    reads.merge(hop.reads);
    Ok(hop.rows.into_iter().map(|row| row.src).collect())
}
