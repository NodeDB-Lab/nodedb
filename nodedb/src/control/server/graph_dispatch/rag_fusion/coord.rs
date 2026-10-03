// SPDX-License-Identifier: BUSL-1.1

//! The cluster RAG fusion coordinator.
//!
//! One core runs a fusion over its own vector index, text index and graph
//! partition. In a cluster those live apart, so the coordinator runs the same
//! pipeline in stages:
//!
//! 1. The collection's owner exports the ranked vector hits, and the BM25 hits
//!    of a three-source fusion (`stages::export_legs`).
//! 2. Every graph owner names the graph node each hit's surrogate is bound to
//!    (`stages::bindings`). A hit keys on that name, or on its surrogate
//!    identity when it names no node, as on one core.
//! 3. The coordinator walks the collection's edges from those nodes, hop by
//!    hop, each frontier node expanded at its key vShard's leader
//!    (`hop::execute_neighbor_hop`). Hop distances are breadth-first, and the
//!    visit cap is the single core's: `max_visited`, bounded by the BFS memory
//!    budget.
//! 4. Every graph owner reports which reached nodes carry a surrogate; the
//!    rest count as unaddressable.
//! 5. The weighted reciprocal-rank fusion and the response body are the
//!    single core's own functions (`graph_rag::rag_response_body` and the
//!    ranked-list builders), so the answer has the same shape and ranks.
//!
//! Under the visit cap both walks admit each level's nodes in node-name order,
//! so a capped walk admits the same nodes here as on one core.

use std::collections::{HashMap, HashSet};

use nodedb_physical::physical_plan::{GraphOp, RagStage};
use nodedb_types::{RowIdentity, Surrogate};

use crate::bridge::envelope::{Payload, PhysicalPlan, Response};
use crate::control::server::dispatch_utils::{not_found_response, ok_payload_response};
use crate::control::state::SharedState;
use crate::data::executor::handlers::graph_rag::{
    RagResponseParams, graph_nodes_to_ranked_results, rag_response_body, vector_ranked_list,
};
use crate::data::executor::handlers::graph_rag_triple::text_ranked_list;
use crate::engine::graph::edge_store::Direction;
use crate::engine::graph::traversal_options::GraphTraversalOptions;
use crate::query::fusion::reciprocal_rank_fusion_weighted;
use crate::types::{DatabaseId, TenantId};

use super::super::hop::{NeighborHopParams, execute_neighbor_hop};
use super::super::shard_reads::ShardReadLog;
use super::stages::{BindingsRequest, ExportScope, bindings, export_legs};

/// Run `plan` when it is a whole RAG fusion (`RagStage::Local`) in a cluster.
/// Returns `None` on a single node and for every other plan: one core runs
/// those.
pub async fn serve_rag_plan(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    plan: &PhysicalPlan,
    linearizable: bool,
) -> Option<crate::Result<Response>> {
    state.cluster_routing.as_ref()?;
    let PhysicalPlan::Graph(GraphOp::RagFusion {
        stage: RagStage::Local,
        ..
    }) = plan
    else {
        return None;
    };
    Some(
        run(
            state,
            RagScope {
                tenant_id,
                database_id,
                linearizable,
            },
            plan,
        )
        .await,
    )
}

#[derive(Clone, Copy)]
struct RagScope {
    tenant_id: TenantId,
    database_id: DatabaseId,
    linearizable: bool,
}

async fn run(state: &SharedState, scope: RagScope, plan: &PhysicalPlan) -> crate::Result<Response> {
    let PhysicalPlan::Graph(GraphOp::RagFusion {
        collection,
        edge_label,
        direction,
        expansion_depth,
        final_top_k,
        rrf_k,
        rrf_k_triple,
        options,
        bm25_query,
        bm25_field,
        ..
    }) = plan
    else {
        return Err(crate::Error::Internal {
            detail: "rag fusion coordinator received a plan that is not a RAG fusion".into(),
        });
    };
    let RagScope {
        tenant_id,
        database_id,
        linearizable,
    } = scope;
    let qualified = collection.as_str().to_owned();
    let vshard = nodedb_types::CollectionKey::from_qualified(database_id, collection)?
        .vshard()
        .as_u32();
    let mut reads = ShardReadLog::new();

    // 1. The owner's legs.
    let Some(exported) = export_legs(
        state,
        ExportScope {
            tenant_id,
            database_id,
            vshard,
            linearizable,
        },
        plan,
        &mut reads,
    )
    .await?
    else {
        reads.publish(state, tenant_id, database_id, Some(qualified));
        return Ok(not_found_response());
    };
    let legs = exported.legs;

    // 2. Name the hits' graph nodes.
    let mut hit_surrogates: Vec<u32> = Vec::new();
    for hit in &legs.vector {
        if let Some(raw) = hit.surrogate
            && !hit_surrogates.contains(&raw)
        {
            hit_surrogates.push(raw);
        }
    }
    let seed_bindings = if hit_surrogates.is_empty() {
        Default::default()
    } else {
        bindings(
            state,
            plan,
            BindingsRequest {
                tenant_id,
                database_id,
                collection: qualified.clone(),
                surrogates: hit_surrogates,
                names: Vec::new(),
                linearizable,
            },
        )
        .await?
    };
    let mut vector_scores: HashMap<RowIdentity, (usize, f32)> = HashMap::new();
    let mut seeds: Vec<String> = Vec::new();
    for (rank, hit) in legs.vector.iter().enumerate() {
        let key = match hit.surrogate {
            Some(raw) => match seed_bindings.by_surrogate.get(&raw) {
                Some(name) => {
                    seeds.push(name.clone());
                    RowIdentity::from_user_key(name.clone())
                }
                None => RowIdentity::for_surrogate(Surrogate::new(raw)),
            },
            None => RowIdentity::from_user_key(format!("__unbound_{}", hit.entry_id)),
        };
        vector_scores.insert(key, (rank, hit.distance));
    }

    // 3. Walk the collection's edges from the seeds. A collection no
    // partition holds edges of reaches nothing, not even its seeds.
    let walk = if seed_bindings.knows_collection {
        walk_from_seeds(
            state,
            scope,
            WalkSpec {
                collection: &qualified,
                edge_labels: edge_label.as_slice(),
                direction: *direction,
                max_depth: *expansion_depth,
                options,
            },
            seeds,
            &mut reads,
        )
        .await?
    } else {
        Walk::default()
    };

    // 4. Which reached nodes carry a surrogate.
    let unaddressable = if walk.order.is_empty() {
        0
    } else {
        let reached = bindings(
            state,
            plan,
            BindingsRequest {
                tenant_id,
                database_id,
                collection: qualified.clone(),
                surrogates: Vec::new(),
                names: walk.order.clone(),
                linearizable,
            },
        )
        .await?;
        walk.order
            .iter()
            .filter(|name| !reached.bound_names.contains(name.as_str()))
            .count()
    };

    // 5. Fuse, exactly as one core does.
    let graph_expanded_count = walk.order.len();
    let graph_list = graph_nodes_to_ranked_results(walk.order, &walk.distances);
    let vector_list = vector_ranked_list(&vector_scores);
    let (fused, op_name) = match (bm25_query, bm25_field, rrf_k_triple) {
        (Some(_), Some(_), Some((vector_k, text_k, graph_k))) => {
            let text_hits: Vec<(Surrogate, f32)> = legs
                .text
                .iter()
                .map(|hit| (Surrogate::new(hit.surrogate), hit.score))
                .collect();
            let text_list = text_ranked_list(&text_hits);
            (
                reciprocal_rank_fusion_weighted(
                    &[vector_list, text_list, graph_list],
                    &[*vector_k, *text_k, *graph_k],
                    *final_top_k,
                ),
                "graph rag fusion triple",
            )
        }
        _ => {
            let (vector_k, graph_k) = *rrf_k;
            (
                reciprocal_rank_fusion_weighted(
                    &[vector_list, graph_list],
                    &[vector_k, graph_k],
                    *final_top_k,
                ),
                "graph rag fusion",
            )
        }
    };
    let body = rag_response_body(
        &RagResponseParams {
            fused: &fused,
            vector_scores: &vector_scores,
            hop_distances: &walk.distances,
            vector_candidate_count: legs.vector.len(),
            graph_expanded_count,
            bfs_truncated: walk.truncated,
            graph_unaddressable: unaddressable,
            op_name,
        },
        exported.watermark_lsn.as_u64(),
    );
    let payload = zerompk::to_msgpack_vec(&body).map_err(|e| crate::Error::Codec {
        detail: format!("{op_name} encode: {e}"),
    })?;
    // Every vShard the fusion read joins the transaction read-set.
    reads.publish(state, tenant_id, database_id, Some(qualified));
    Ok(ok_payload_response(Payload::from_vec(payload)))
}

/// What a walk reads: the collection's edges under a label set and direction.
struct WalkSpec<'a> {
    collection: &'a str,
    /// Empty keeps every edge. Otherwise an edge with any listed label.
    edge_labels: &'a [String],
    direction: Direction,
    max_depth: usize,
    options: &'a GraphTraversalOptions,
}

/// Every node a walk reached, in discovery order, at its hop distance.
#[derive(Default)]
struct Walk {
    order: Vec<String>,
    distances: HashMap<String, usize>,
    truncated: bool,
}

/// Breadth-first walk from `seeds`, as one core's collection-scoped
/// expansion runs it (`CsrIndex::traverse_surrogates_in_collection`): seeds
/// at distance 0, each level's new nodes admitted in node-name order one hop
/// past the frontier, and the walk stops, marked truncated, when a new node
/// will pass the visit cap. The admitted set under the cap is therefore the
/// single core's.
async fn walk_from_seeds(
    state: &SharedState,
    scope: RagScope,
    spec: WalkSpec<'_>,
    seeds: Vec<String>,
    reads: &mut ShardReadLog,
) -> crate::Result<Walk> {
    let WalkSpec {
        collection,
        edge_labels,
        direction,
        max_depth,
        options,
    } = spec;
    let query = &state.tuning.query;
    let budget = query.bfs_memory_budget_bytes / query.bfs_bytes_per_node.max(1);
    let cap = options.max_visited.min(budget);
    // Each hop returns every neighbor of the frontier; the cap is applied
    // here, in name order, as one core applies it.
    let whole_hop = crate::control::server::graph_dispatch::bfs::whole_hop();

    let mut walk = Walk::default();
    let mut visited: HashSet<String> = HashSet::new();
    let mut frontier: Vec<String> = Vec::new();
    for seed in seeds {
        if visited.insert(seed.clone()) {
            walk.distances.insert(seed.clone(), 0);
            walk.order.push(seed.clone());
            frontier.push(seed);
        }
    }
    'walk: for depth in 0..max_depth {
        if frontier.is_empty() {
            break;
        }
        let hop = execute_neighbor_hop(
            state,
            scope.tenant_id,
            scope.database_id,
            NeighborHopParams {
                collection: Some(collection),
                frontier: &frontier,
                edge_labels,
                direction,
                options: &whole_hop,
                discovered_so_far: visited.len(),
                linearizable: scope.linearizable,
                edge_predicate: &[],
                with_properties: false,
                // A RAG fusion plan carries no session transaction.
                txn_id: None,
            },
        )
        .await?;
        reads.merge(hop.reads);
        let mut candidates: Vec<String> = hop
            .rows
            .into_iter()
            .map(|row| row.node)
            .filter(|dst| !visited.contains(dst))
            .collect();
        candidates.sort();
        candidates.dedup();
        let mut next: Vec<String> = Vec::new();
        for dst in candidates {
            if visited.len() >= cap {
                walk.truncated = true;
                break 'walk;
            }
            visited.insert(dst.clone());
            walk.distances.insert(dst.clone(), depth + 1);
            walk.order.push(dst.clone());
            next.push(dst);
        }
        frontier = next;
    }
    Ok(walk)
}
