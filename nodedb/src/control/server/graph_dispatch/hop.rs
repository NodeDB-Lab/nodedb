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
//! Graph edges are Raft-homed on `VShardId::from_key(src)`, and each
//! Data-Plane core's CSR is partitioned (it holds only its owned nodes'
//! out-edges). A traversal coordinated from a node that does NOT own a
//! frontier node therefore CANNOT expand that node on its local cores —
//! the edges live on the owner. Each hop partitions the *incoming*
//! frontier by `VShardId::from_key` owner BEFORE any expansion, expands
//! the locally-owned subset on local cores, and ships a
//! `NeighborsMulti{remote_subset}` plan to each remote owner via the typed
//! [`dispatch_route`] primitive. Both local and remote responses decode
//! through the same `{src,label,node}` decoder, so edges are
//! fully-attributed for BOTH the local-shard and remote-shard portions.
//!
//! Ownership is resolved against LIVE Raft leadership (via
//! [`resolve_decision`] with a live-leader lookup), not the cached routing
//! table, so a stale routing hint cannot misroute a frontier node.

use std::collections::HashMap;

use futures::future::join_all;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::gateway::dispatcher::{default_deadline_ms, dispatch_route};
use crate::control::gateway::router::resolve_decision;
use crate::control::gateway::version_set::GatewayVersionSet;
use crate::control::gateway::{RouteDecision, TaskRoute};
use crate::control::state::SharedState;
use crate::engine::graph::edge_store::Direction;
use crate::engine::graph::traversal_options::GraphTraversalOptions;
use crate::types::{DatabaseId, TenantId, TraceId, VShardId};
use nodedb_physical::physical_plan::GraphOp;

/// A fully-attributed edge crossed by the hop: `(src, label, dst)`.
pub(super) type NeighborTriple = (String, String, String);

/// Result of one BFS hop.
pub(super) struct HopOutput {
    /// `(src,label,dst)` edges crossed this hop. Fully-attributed for both
    /// the local-shard and remote-shard portions of the frontier.
    pub local_triples: Vec<NeighborTriple>,
    /// Deduplicated destination node IDs after merging local + remote
    /// expansion. Feeds the next frontier.
    pub merged_destinations: Vec<String>,
}

/// Parameters for one BFS hop.
pub(super) struct NeighborHopParams<'a> {
    pub frontier: &'a [String],
    pub edge_label: Option<&'a str>,
    pub direction: Direction,
    pub options: &'a GraphTraversalOptions,
    /// Count of nodes already in the global visited set. Bounds the
    /// Data-Plane-side allocation under `options.max_visited` via
    /// `NeighborsMulti.max_results`.
    pub discovered_so_far: usize,
}

/// Execute one hop of BFS from `params.frontier`.
pub(super) async fn execute_neighbor_hop(
    shared: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    params: NeighborHopParams<'_>,
) -> crate::Result<HopOutput> {
    let NeighborHopParams {
        frontier,
        edge_label,
        direction,
        options,
        discovered_so_far,
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

    // Single-node mode: no routing table — every frontier node is local.
    if shared.cluster_routing.is_none() {
        let triples = expand_local(
            shared,
            tenant_id,
            database_id,
            frontier,
            edge_label,
            direction,
            remaining_budget,
        )
        .await?;
        let merged = dedup_destinations(&triples);
        return Ok(HopOutput {
            local_triples: triples,
            merged_destinations: merged,
        });
    }

    // Cluster mode: partition the incoming frontier by owning vShard, using
    // LIVE Raft leadership (not the stale routing-table hint).
    let (local_nodes, remote_by_owner) = partition_frontier_by_owner(shared, frontier)?;

    // Local-owned subset: expand on local cores.
    let mut all_triples: Vec<NeighborTriple> = if local_nodes.is_empty() {
        Vec::new()
    } else {
        expand_local(
            shared,
            tenant_id,
            database_id,
            &local_nodes,
            edge_label,
            direction,
            remaining_budget,
        )
        .await?
    };

    // Remote-owned subsets: ship a typed `NeighborsMulti` to each owner and
    // decode its response with the SAME decoder. Issue all remote dispatches
    // concurrently.
    if !remote_by_owner.is_empty() {
        let remote_triples = expand_remote(
            shared,
            tenant_id,
            database_id,
            remote_by_owner,
            edge_label,
            direction,
            remaining_budget,
        )
        .await?;
        all_triples.extend(remote_triples);
    }

    let merged = dedup_destinations(&all_triples);
    Ok(HopOutput {
        local_triples: all_triples,
        merged_destinations: merged,
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
/// [`resolve_decision`] with a live-leader lookup, so a stale routing hint
/// cannot misroute a frontier node. A node whose owning vShard currently has
/// no known leader (`LeaderUnknown`) is a hard error — we never silently
/// degrade to a local-only expansion that would return a partial set.
fn partition_frontier_by_owner(
    shared: &SharedState,
    frontier: &[String],
) -> crate::Result<(Vec<String>, Vec<RemoteOwnerBatch>)> {
    let routing_guard = shared
        .cluster_routing
        .as_ref()
        .map(|rw| rw.read().unwrap_or_else(|p| p.into_inner()));
    let raft_snapshot: Vec<nodedb_cluster::GroupStatus> =
        shared.raft_status_fn.get().map(|f| f()).unwrap_or_default();
    let live_leader = move |group_id: u64| -> u64 {
        raft_snapshot
            .iter()
            .find(|gs| gs.group_id == group_id)
            .map(|gs| gs.leader_id)
            .unwrap_or(0)
    };
    let live_lookup: Option<&dyn Fn(u64) -> u64> = if shared.raft_status_fn.get().is_some() {
        Some(&live_leader)
    } else {
        None
    };

    let mut local: Vec<String> = Vec::new();
    // Group remote nodes by owning vShard so each owner gets one batched plan.
    let mut remote: HashMap<u32, RemoteOwnerBatch> = HashMap::new();

    for node in frontier {
        let vshard_id = VShardId::from_key(node.as_bytes()).as_u32();
        let decision = resolve_decision(
            vshard_id,
            shared.node_id,
            routing_guard.as_deref(),
            live_lookup,
        );
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

/// Expand a locally-owned subset on all local Data-Plane cores.
async fn expand_local(
    shared: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    node_ids: &[String],
    edge_label: Option<&str>,
    direction: Direction,
    max_results: u32,
) -> crate::Result<Vec<NeighborTriple>> {
    let plan = PhysicalPlan::Graph(GraphOp::NeighborsMulti {
        node_ids: node_ids.to_vec(),
        edge_label: edge_label.map(str::to_string),
        direction,
        max_results,
        rls_filters: Vec::new(),
    });
    let resp = crate::control::server::broadcast::broadcast_to_all_cores(
        shared,
        tenant_id,
        database_id,
        plan,
        TraceId::ZERO,
    )
    .await?;
    Ok(decode_neighbor_triples(&resp.payload))
}

/// Expand the remote-owned subsets concurrently: ship a typed
/// `NeighborsMulti` plan to each owning node via [`dispatch_route`] and
/// decode every returned payload with the shared `{src,label,node}` decoder.
async fn expand_remote(
    shared: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    owners: Vec<RemoteOwnerBatch>,
    edge_label: Option<&str>,
    direction: Direction,
    max_results: u32,
) -> crate::Result<Vec<NeighborTriple>> {
    // The dispatcher's remote path needs an owned `Arc<SharedState>`. In
    // cluster mode the gateway is always wired; `gateway_shared` fails loudly
    // if it is absent (rather than silently degrading to a partial local-only
    // reachable set) and upgrades the gateway's weak back-reference to the
    // owning `SharedState`.
    let shared_arc = super::cluster_resolve::gateway_shared(shared)?;

    let deadline_ms = default_deadline_ms(&shared_arc);
    // Graph structural ops touch no named collection, so the version set is
    // empty (descriptor-version checks do not apply to node-id-keyed edges).
    let version_set = GatewayVersionSet::from_pairs(Vec::new());

    let edge_label_owned = edge_label.map(str::to_string);

    let dispatches = owners.into_iter().map(|owner| {
        let RemoteOwnerBatch {
            node_id,
            vshard_id,
            node_ids,
        } = owner;
        let plan = PhysicalPlan::Graph(GraphOp::NeighborsMulti {
            node_ids,
            edge_label: edge_label_owned.clone(),
            direction,
            max_results,
            rls_filters: Vec::new(),
        });
        let route = TaskRoute {
            plan,
            decision: RouteDecision::Remote { node_id, vshard_id },
            vshard_id: (vshard_id % VShardId::COUNT as u64) as u32,
        };
        let version_set = version_set.clone();
        let shared_arc = shared_arc.clone();
        // Box::pin keeps the heterogeneous async dispatch futures uniform for
        // `join_all` and guards against any future async-recursion concerns.
        Box::pin(async move {
            dispatch_route(
                route,
                &shared_arc,
                tenant_id,
                database_id,
                TraceId::ZERO,
                deadline_ms,
                &version_set,
            )
            .await
        })
    });

    let results = join_all(dispatches).await;

    let mut triples: Vec<NeighborTriple> = Vec::new();
    for result in results {
        // A remote dispatch error is fatal: a dropped owner means a partial
        // reachable set, exactly the silent-degradation bug this path fixes.
        // Graph hop traversal consumes payloads only; per-shard watermarks are
        // not part of the neighbor-triple decode.
        let payloads = result?.payloads;
        for payload in payloads {
            triples.extend(decode_neighbor_triples_bytes(&payload));
        }
    }
    Ok(triples)
}

/// Deduplicate the destination node IDs of a triple set, preserving order.
fn dedup_destinations(triples: &[NeighborTriple]) -> Vec<String> {
    let mut seen: std::collections::HashSet<&String> = std::collections::HashSet::new();
    let mut out = Vec::new();
    for (_, _, dst) in triples {
        if seen.insert(dst) {
            out.push(dst.clone());
        }
    }
    out
}

/// Decode a Data-Plane response [`Payload`] of `{src,label,node}` rows into
/// fully-typed triples. (`Payload` derefs to `[u8]`.)
///
/// [`Payload`]: crate::bridge::envelope::Payload
fn decode_neighbor_triples(payload: &crate::bridge::envelope::Payload) -> Vec<NeighborTriple> {
    decode_neighbor_triples_bytes(payload)
}

/// Decode raw Data-Plane response bytes (the shape both a local broadcast and
/// a remote `dispatch_route` return — the same `NeighborsMulti` op produces it
/// on any node) into fully-typed triples.
fn decode_neighbor_triples_bytes(payload: &[u8]) -> Vec<NeighborTriple> {
    if payload.is_empty() {
        return Vec::new();
    }
    let json_text = crate::data::executor::response_codec::decode_payload_to_json(payload);
    decode_neighbor_triples_json(&json_text)
}

/// Shared inner decode: parse the `{src,label,node}` JSON array into triples.
/// Malformed entries (missing or non-string `src`/`node`) are skipped;
/// `label` defaults to "" since label-less edges are a valid graph shape.
fn decode_neighbor_triples_json(json_text: &str) -> Vec<NeighborTriple> {
    let arr = match sonic_rs::from_str::<Vec<serde_json::Value>>(json_text) {
        Ok(arr) => arr,
        Err(_) => return Vec::new(),
    };
    let mut out = Vec::with_capacity(arr.len());
    for item in arr {
        let src = item.get("src").and_then(|v| v.as_str());
        let node = item.get("node").and_then(|v| v.as_str());
        let (src, node) = match (src, node) {
            (Some(s), Some(n)) if !s.is_empty() && !n.is_empty() => (s, n),
            _ => continue,
        };
        let label = item.get("label").and_then(|v| v.as_str()).unwrap_or("");
        out.push((src.to_string(), label.to_string(), node.to_string()));
    }
    out
}
