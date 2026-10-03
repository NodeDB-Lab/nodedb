// SPDX-License-Identifier: BUSL-1.1

//! One BFS hop: partition the incoming frontier by owning vShard, expand
//! each frontier node at the node that owns `from_key(node)`, decode the
//! `{src,label,node}` rows, and merge.
//!
//! Both `bfs::cross_core_bfs_with_options` and
//! `traverse_subgraph::cross_core_traverse_subgraph` execute the same
//! hop. They differ only in what they retain from each hop:
//!
//! * BFS keeps the merged destination set (flat reachable nodes).
//! * Subgraph traversal keeps the fully-attributed edge triples *plus*
//!   the merged destination set (for next-frontier expansion).
//!
//! ## Owner-targeted expansion (cluster mode)
//!
//! A graph edge lives on the key vShard of each endpoint:
//! `VShardId::from_key(src)` holds it for forward traversal and
//! `VShardId::from_key(dst)` for reverse traversal. Every edge of a node, in
//! either direction, therefore lives on the owner of `from_key(node)`. A
//! traversal coordinated from a node that does NOT own a frontier node
//! CANNOT expand that node on its local cores. Each hop partitions the *incoming*
//! frontier by `VShardId::from_key` owner BEFORE any expansion, expands
//! the locally-owned subset on local cores, and ships a
//! `NeighborsMulti{remote_subset}` plan to each remote owner via the typed
//! [`dispatch_route`] primitive. Both local and remote responses decode
//! through the same `{src,label,node}` decoder, so edges are
//! fully-attributed for BOTH the local-shard and remote-shard portions.
//!
//! Ownership is resolved against LIVE Raft leadership (via
//! [`LiveLeaders::resolve`]), not the cached routing
//! table, so a stale routing hint cannot misroute a frontier node.
//!
//! ## Read-your-own-writes
//!
//! A hop inside a transaction carries the session's `txn_id` to every core
//! that expands part of the frontier, local or remote. Each core merges that
//! transaction's staged edge writes into its rows (`NeighborsMulti` in
//! `data/executor/handlers/graph.rs`). The merge is complete:
//!
//! - A staged edge write lands on the leader of each home vShard of the edge,
//!   `from_key(src)` and `from_key(dst)`
//!   (`shared/ddl/neutral/graph_ops/edge_stage.rs`, `stage_edge_dual_home`).
//!   A write staged on a follower is forwarded to that leader
//!   (`shared/session/leader_forward.rs`).
//! - The hop expands each frontier node on the live leader of
//!   `from_key(node)` (`partition_frontier_by_owner`). The core that owns
//!   that vShard takes part in the expansion.
//!
//! So the core that expands a node holds every staged write to that node's
//! edges.

use std::collections::BTreeMap;

use futures::future::join_all;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::gateway::dispatcher::{
    DispatchRouteParams, dispatch_route, statement_deadline_ms,
};
use crate::control::gateway::live_leaders::LiveLeaders;
use crate::control::gateway::version_set::GatewayVersionSet;
use crate::control::gateway::{RouteDecision, TaskRoute};
use crate::control::state::SharedState;
use crate::engine::graph::edge_store::Direction;
use crate::engine::graph::traversal_options::GraphTraversalOptions;
use crate::types::{DatabaseId, TenantId, TraceId, TxnId, VShardId};
use nodedb_physical::physical_plan::GraphOp;
use nodedb_types::filter::MetadataFilter;

use super::neighbor_rows::{NeighborRow, decode_neighbor_rows};
use super::shard_reads::{ShardReadLog, key_vshards};

/// Result of one BFS hop.
pub(super) struct HopOutput {
    /// `(frontier node, label, neighbour)` rows of the edges crossed this
    /// hop. Fully-attributed for both the local-shard and remote-shard
    /// portions of the frontier.
    pub rows: Vec<NeighborRow>,
    /// Deduplicated destination node IDs after merging local + remote
    /// expansion. Feeds the next frontier.
    pub merged_destinations: Vec<String>,
    /// The key vShard of every frontier node, at the watermark its expansion
    /// served.
    pub reads: ShardReadLog,
}

/// Parameters for one BFS hop.
pub(super) struct NeighborHopParams<'a> {
    /// Collection whose edges this hop traverses.
    pub collection: Option<&'a str>,
    pub frontier: &'a [String],
    /// Empty keeps every edge. Otherwise an edge with any listed label.
    pub edge_labels: &'a [String],
    pub direction: Direction,
    pub options: &'a GraphTraversalOptions,
    /// Count of nodes already in the global visited set. Bounds the
    /// Data-Plane-side allocation under `options.max_visited` via
    /// `NeighborsMulti.max_results`.
    pub discovered_so_far: usize,
    /// Each node that expands part of the frontier confirms its groups first.
    pub linearizable: bool,
    /// AND-ed edge-property predicate, evaluated on the core that holds
    /// each edge. Empty admits every edge. Non-empty requires `collection`.
    pub edge_predicate: &'a [MetadataFilter],
    /// Each row carries the crossed edge's property object. Requires
    /// `collection`.
    pub with_properties: bool,
    /// The session's transaction. Its staged edge writes merge into the hop.
    pub txn_id: Option<TxnId>,
}

/// Execute one hop of BFS from `params.frontier`.
pub(super) async fn execute_neighbor_hop(
    shared: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    params: NeighborHopParams<'_>,
) -> crate::Result<HopOutput> {
    let NeighborHopParams {
        collection,
        frontier,
        edge_labels,
        direction,
        options,
        discovered_so_far,
        linearizable,
        edge_predicate,
        with_properties,
        txn_id,
    } = params;

    // Cap this hop's handler-side allocation to the remaining budget under
    // `max_visited` so a single wide hop cannot blow past the cap on the
    // Data-Plane side. Note: when the frontier is split across N owners each
    // owner receives the FULL remaining budget — correctness holds because
    // each handler independently caps its own visited count; only the
    // budgeting granularity shifts (per-owner instead of global).
    let remaining_budget = options
        .max_visited
        .saturating_sub(discovered_so_far)
        .min(u32::MAX as usize) as u32;

    let scope = ExpandScope {
        tenant_id,
        database_id,
        collection,
        edge_labels,
        direction,
        max_results: remaining_budget,
        linearizable,
        edge_predicate,
        with_properties,
        txn_id,
    };

    // Single-node mode: no routing table — every frontier node is local.
    if shared.cluster_routing.is_none() {
        let (rows, _watermark) = expand_local(shared, scope, frontier).await?;
        let merged = dedup_destinations(&rows);
        return Ok(HopOutput {
            rows,
            merged_destinations: merged,
            reads: ShardReadLog::new(),
        });
    }

    // Cluster mode: partition the incoming frontier by owning vShard, using
    // LIVE Raft leadership (not the stale routing-table hint).
    let (local_nodes, remote_by_owner) = partition_frontier_by_owner(shared, frontier)?;

    let mut reads = ShardReadLog::new();
    // Local-owned subset: expand on local cores.
    let mut all_rows: Vec<NeighborRow> = if local_nodes.is_empty() {
        Vec::new()
    } else {
        let (rows, watermark) = expand_local(shared, scope, &local_nodes).await?;
        reads.note(key_vshards(&local_nodes), watermark, shared.node_id);
        rows
    };

    // Remote-owned subsets: ship a typed `NeighborsMulti` to each owner and
    // decode its response with the SAME decoder. Issue all remote dispatches
    // concurrently.
    if !remote_by_owner.is_empty() {
        let remote_rows = expand_remote(shared, scope, remote_by_owner, &mut reads).await?;
        all_rows.extend(remote_rows);
    }

    let merged = dedup_destinations(&all_rows);
    Ok(HopOutput {
        rows: all_rows,
        merged_destinations: merged,
        reads,
    })
}

/// A remote-owned frontier subset: the owning node, its vShard, and the
/// frontier nodes that hash to it.
struct RemoteOwnerBatch {
    node_id: u64,
    vshard_id: u64,
    node_ids: Vec<String>,
}

/// Partition the incoming frontier into the locally-owned subset and the
/// remote-owned subsets grouped by `(owner node, vShard)`.
///
/// Ownership is resolved against LIVE Raft leadership via
/// [`LiveLeaders::resolve`], so a stale routing hint
/// cannot misroute a frontier node. A node whose owning vShard currently has
/// no known leader (`LeaderUnknown`) is a hard error — we never silently
/// degrade to a local-only expansion that will return a partial set.
fn partition_frontier_by_owner(
    shared: &SharedState,
    frontier: &[String],
) -> crate::Result<(Vec<String>, Vec<RemoteOwnerBatch>)> {
    // Raft snapshot first, routing guard second: see `LiveLeaders`.
    let live = LiveLeaders::snapshot(shared);
    let routing_guard = shared
        .cluster_routing
        .as_ref()
        .map(|rw| rw.read().unwrap_or_else(|p| p.into_inner()));

    let mut local: Vec<String> = Vec::new();
    // Group remote nodes by owning vShard so each owner gets one batched plan,
    // dispatched in vShard order.
    let mut remote: BTreeMap<u32, RemoteOwnerBatch> = BTreeMap::new();

    for node in frontier {
        let vshard_id = VShardId::from_key(node.as_bytes()).as_u32();
        let decision = live.resolve(vshard_id, shared.node_id, routing_guard.as_deref());
        match decision {
            RouteDecision::Local => local.push(node.clone()),
            RouteDecision::Remote {
                node_id,
                vshard_id: vs,
            } => {
                remote
                    .entry(vshard_id)
                    .or_insert_with(|| RemoteOwnerBatch {
                        node_id,
                        vshard_id: vs,
                        node_ids: Vec::new(),
                    })
                    .node_ids
                    .push(node.clone());
            }
            RouteDecision::LeaderUnknown { vshard_id: vs } => {
                return Err(crate::Error::NotLeader {
                    vshard_id: VShardId::new((vs % VShardId::COUNT as u64) as u32),
                    leader_node: 0,
                    leader_addr: String::new(),
                    leader_term: 0,
                });
            }
            RouteDecision::Broadcast { .. } => {
                // `resolve_decision` never returns Broadcast; it resolves a
                // single vShard. Treat it as an internal invariant violation.
                return Err(crate::Error::Internal {
                    detail: "graph hop: resolve_decision returned Broadcast for a single vShard"
                        .into(),
                });
            }
        }
    }

    Ok((local, remote.into_values().collect()))
}

/// Shared scope for one expansion of a BFS frontier.
#[derive(Clone, Copy)]
struct ExpandScope<'a> {
    tenant_id: TenantId,
    database_id: DatabaseId,
    /// Collection scope, or `None` for a label-only traversal.
    collection: Option<&'a str>,
    /// Empty keeps every edge. Otherwise an edge with any listed label.
    edge_labels: &'a [String],
    direction: Direction,
    max_results: u32,
    linearizable: bool,
    /// AND-ed edge-property predicate. Empty admits every edge.
    edge_predicate: &'a [MetadataFilter],
    /// Each row carries the crossed edge's property object.
    with_properties: bool,
    /// The session's transaction, whose staged edge writes merge in.
    txn_id: Option<TxnId>,
}

impl ExpandScope<'_> {
    /// The `NeighborsMulti` plan that expands `node_ids` in this scope.
    fn plan(&self, node_ids: Vec<String>) -> PhysicalPlan {
        PhysicalPlan::Graph(GraphOp::NeighborsMulti {
            collection: self
                .collection
                .map(|c| nodedb_types::QualifiedCollection::from_stored(c.to_string())),
            node_ids,
            edge_labels: self.edge_labels.to_vec(),
            direction: self.direction,
            max_results: self.max_results,
            rls_filters: Vec::new(),
            edge_predicate: self.edge_predicate.to_vec(),
            with_properties: self.with_properties,
        })
    }
}

/// Expand a locally-owned subset on all local Data-Plane cores. Returns the
/// crossed edges and the highest watermark a core served them at.
async fn expand_local(
    shared: &SharedState,
    scope: ExpandScope<'_>,
    node_ids: &[String],
) -> crate::Result<(Vec<NeighborRow>, crate::types::Lsn)> {
    let plan = scope.plan(node_ids.to_vec());
    // A linearizable hop confirms the groups its plan reads first.
    if scope.linearizable {
        super::read_groups::confirm_graph_read(shared, scope.database_id, &plan).await?;
    }
    let resp = crate::control::server::broadcast::broadcast_to_all_cores_txn(
        shared,
        scope.tenant_id,
        scope.database_id,
        plan,
        TraceId::ZERO,
        scope.txn_id,
    )
    .await?;
    Ok((decode_neighbor_rows(&resp.payload)?, resp.watermark_lsn))
}

/// Expand the remote-owned subsets concurrently: ship a typed
/// `NeighborsMulti` plan to each owning node via [`dispatch_route`] and
/// decode every returned payload with the shared `{src,label,node}` decoder.
/// Notes each owner's vShard in `reads` at the watermark it served.
async fn expand_remote(
    shared: &SharedState,
    scope: ExpandScope<'_>,
    owners: Vec<RemoteOwnerBatch>,
    reads: &mut ShardReadLog,
) -> crate::Result<Vec<NeighborRow>> {
    let ExpandScope {
        tenant_id,
        database_id,
        linearizable,
        txn_id,
        ..
    } = scope;
    // The dispatcher's remote path needs an owned `Arc<SharedState>`. In
    // cluster mode the gateway is always wired; `gateway_shared` fails loudly
    // if it is absent (rather than silently degrading to a partial local-only
    // reachable set) and upgrades the gateway's weak back-reference to the
    // owning `SharedState`.
    let shared_arc = super::cluster_resolve::gateway_shared(shared)?;

    let deadline_ms = statement_deadline_ms(&shared_arc);
    // Graph structural ops touch no named collection, so the version set is
    // empty (descriptor-version checks do not apply to node-id-keyed edges).
    let version_set = GatewayVersionSet::from_pairs(Vec::new());

    let dispatches = owners.into_iter().map(|owner| {
        let RemoteOwnerBatch {
            node_id,
            vshard_id,
            node_ids,
        } = owner;
        let plan = scope.plan(node_ids);
        let route = TaskRoute {
            plan,
            decision: RouteDecision::Remote { node_id, vshard_id },
            vshard_id: (vshard_id % VShardId::COUNT as u64) as u32,
        };
        let version_set = version_set.clone();
        let shared_arc = shared_arc.clone();
        let read_vshard = (vshard_id % VShardId::COUNT as u64) as u32;
        // Box::pin keeps the heterogeneous async dispatch futures uniform for
        // `join_all` and guards against any future async-recursion concerns.
        Box::pin(async move {
            dispatch_route(DispatchRouteParams {
                route,
                shared: &shared_arc,
                tenant_id,
                database_id,
                trace_id: TraceId::ZERO,
                deadline_ms,
                version_set: &version_set,
                txn_id,
                linearizable,
            })
            .await
            .map(|outcome| (read_vshard, node_id, outcome))
        })
    });

    let results = join_all(dispatches).await;

    let mut rows: Vec<NeighborRow> = Vec::new();
    for result in results {
        // A remote dispatch error is fatal: a dropped owner means a partial
        // reachable set.
        let (read_vshard, node_id, outcome) = result?;
        reads.note_leg([read_vshard], &outcome.shard_watermarks, node_id);
        for payload in outcome.payloads {
            rows.extend(decode_neighbor_rows(&payload)?);
        }
    }
    Ok(rows)
}

/// Deduplicate the neighbour node IDs of a row set, preserving order.
fn dedup_destinations(rows: &[NeighborRow]) -> Vec<String> {
    let mut seen: std::collections::HashSet<&String> = std::collections::HashSet::new();
    let mut out = Vec::new();
    for row in rows {
        if seen.insert(&row.node) {
            out.push(row.node.clone());
        }
    }
    out
}
