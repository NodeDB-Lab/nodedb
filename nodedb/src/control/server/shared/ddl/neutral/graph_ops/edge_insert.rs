// SPDX-License-Identifier: BUSL-1.1

//! `GRAPH INSERT EDGE`: one or more edges into one collection.
//!
//! A statement binds its distinct endpoints in one surrogate exchange and
//! writes all its edges as one unit. A single edge is an `EdgePut`. Several
//! edges are one `EdgePutBatch`, so the statement costs one commit chain
//! however many edges it carries. When the edges' homes span more than one
//! vShard, that unit is one Calvin transaction, and the sequencer slices the
//! batch by home pair.

use std::collections::{BTreeSet, HashMap, HashSet};

use nodedb_physical::physical_plan::{BatchEdge, GraphOp};
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};
use nodedb_sql::ddl_ast::GraphEdgeSpec;
use nodedb_types::{CollectionKey, QualifiedCollection, Surrogate};

use crate::bridge::envelope::PhysicalPlan;
use crate::control::planner::calvin::{build_static_tx_class, submit_calvin_routed_write};
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::session::{DmlTxnCtx, TransactionState};
use crate::control::server::surrogate_exchange::assign_surrogates_routed;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, RecordHomes, TenantId, TraceId, VShardId};

use super::super::super::result::{DdlError, DdlResult};
use super::edge::{EdgeHomes, response_affected};
use super::edge_parse::{properties_to_msgpack, validate_edge_label};
use super::support::{data_plane_verdict, ddl_err};

/// An edge ready to write: its `EdgePut`, after write policy, and its homes.
struct PreparedEdge {
    op: GraphOp,
    homes: RecordHomes,
}

/// `GRAPH INSERT EDGE IN '<collection>' FROM '<src>' TO '<dst>' TYPE '<label>'`
/// `[PROPERTIES '<json object>' | { ... }] [VALID_TIME <from> [UNTIL <until>]]`
/// followed by any number of further edges.
pub async fn insert_edges(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    collection: String,
    edges: Vec<GraphEdgeSpec>,
    txn_ctx: &DmlTxnCtx<'_>,
) -> Result<Vec<DdlResult>, DdlError> {
    if collection.is_empty() {
        return Err(ddl_err(
            "42601",
            "GRAPH INSERT EDGE requires IN <collection>",
        ));
    }
    if edges.is_empty() {
        return Err(ddl_err(
            "42601",
            "GRAPH INSERT EDGE requires FROM, TO and TYPE",
        ));
    }
    for edge in &edges {
        if edge.src.is_empty() || edge.dst.is_empty() {
            return Err(ddl_err("42601", "GRAPH INSERT EDGE requires FROM and TO"));
        }
        validate_edge_label(&edge.label)?;
    }
    let tenant_id = identity.tenant_id;

    // Flags the collection edge-bearing so a later predicate DELETE routes through
    // OLLP instead of the fast path. Idempotent.
    crate::control::planner::implicit_edges::mark_collection_edge_bearing(
        state,
        database_id,
        tenant_id,
        &collection,
    )
    .await
    .map_err(|e| DdlError::from_error(&e))?;

    let surrogates =
        endpoint_surrogates(state, database_id, tenant_id, &collection, &edges).await?;
    let prepared = prepare_edges(
        state,
        identity,
        database_id,
        &collection,
        edges,
        &surrogates,
    )?;

    // In a transaction, an insert stages into `GraphTxnOverlay` instead of applying now
    // (COMMIT replays, ROLLBACK discards); a cross-shard edge stages into both endpoints.
    let affected =
        if txn_ctx.sessions.transaction_state(txn_ctx.session_id) == TransactionState::InBlock {
            let mut staged = 0u64;
            for PreparedEdge { op, homes } in prepared {
                staged += super::edge_stage::stage_edge_dual_home(
                    state,
                    tenant_id,
                    database_id,
                    EdgeHomes {
                        vsrc: homes.owner(),
                        vdst: homes.second(),
                        single_home: homes.is_single(),
                    },
                    op,
                    txn_ctx,
                )
                .await?;
            }
            staged
        } else {
            write_edges(state, tenant_id, database_id, prepared).await?
        };

    Ok(vec![DdlResult::Status {
        command: "INSERT EDGE".to_string(),
        rows_affected: Some(affected),
    }])
}

/// Bind every distinct endpoint of `edges` in one surrogate exchange.
async fn endpoint_surrogates(
    state: &SharedState,
    database_id: DatabaseId,
    tenant_id: TenantId,
    collection: &str,
    edges: &[GraphEdgeSpec],
) -> Result<HashMap<String, Surrogate>, DdlError> {
    let mut ids: Vec<&str> = Vec::with_capacity(edges.len() * 2);
    let mut seen: HashSet<&str> = HashSet::with_capacity(edges.len() * 2);
    for edge in edges {
        for id in [edge.src.as_str(), edge.dst.as_str()] {
            if seen.insert(id) {
                ids.push(id);
            }
        }
    }
    let keys: Vec<&[u8]> = ids.iter().map(|id| id.as_bytes()).collect();
    let bound = assign_surrogates_routed(
        state,
        CollectionKey::from_bare(database_id, collection),
        tenant_id,
        &keys,
        TraceId::ZERO,
    )
    .await
    .map_err(|e| DdlError::from_error(&e))?;
    if bound.len() != ids.len() {
        return Err(DdlError::internal(format!(
            "surrogate exchange bound {} of {} edge endpoints in '{collection}'",
            bound.len(),
            ids.len()
        )));
    }
    Ok(ids.into_iter().map(str::to_owned).zip(bound).collect())
}

fn bound_surrogate(
    surrogates: &HashMap<String, Surrogate>,
    node: &str,
) -> Result<Surrogate, DdlError> {
    surrogates
        .get(node)
        .copied()
        .ok_or_else(|| DdlError::internal(format!("edge endpoint '{node}' has no bound surrogate")))
}

/// Build each edge's `EdgePut` and run it through the collection's write
/// policy. This handler dispatches as trusted internal work, so nothing
/// downstream resolves a policy: the policy decides the `PROPERTIES` image
/// here, before staging.
fn prepare_edges(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    collection: &str,
    edges: Vec<GraphEdgeSpec>,
    surrogates: &HashMap<String, Surrogate>,
) -> Result<Vec<PreparedEdge>, DdlError> {
    edges
        .into_iter()
        .map(|edge| {
            let GraphEdgeSpec {
                src,
                dst,
                label,
                properties,
                valid_from_ms,
                valid_until_ms,
            } = edge;
            // Dual-home: a cross-shard edge must be written on the home vShard of
            // both src and dst, or reverse/IN traversal never finds it.
            let homes = RecordHomes::edge(&src, &dst);
            let src_surrogate = bound_surrogate(surrogates, &src)?;
            let dst_surrogate = bound_surrogate(surrogates, &dst)?;
            let op = super::edge_rls::resolve_edge_write_rls(
                state,
                identity,
                database_id,
                GraphOp::EdgePut {
                    collection: QualifiedCollection::new(database_id, collection),
                    src_id: src,
                    label,
                    dst_id: dst,
                    properties: properties_to_msgpack(properties)?,
                    src_surrogate,
                    dst_surrogate,
                    valid_from_ms,
                    valid_until_ms,
                },
            )?;
            Ok(PreparedEdge { op, homes })
        })
        .collect()
}

/// Write `prepared` as one unit: a direct write when every edge homes on one
/// vShard, otherwise one Calvin transaction over the union of the homes.
async fn write_edges(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    mut prepared: Vec<PreparedEdge>,
) -> Result<u64, DdlError> {
    let mut vshards: Vec<VShardId> = Vec::new();
    let mut seen: BTreeSet<u32> = BTreeSet::new();
    for edge in &prepared {
        for vshard in edge.homes.iter() {
            if seen.insert(vshard.as_u32()) {
                vshards.push(vshard);
            }
        }
    }
    let Some(&first_home) = vshards.first() else {
        return Err(DdlError::internal("an edge insert has no home vShard"));
    };

    let op = if prepared.len() == 1 {
        let Some(edge) = prepared.pop() else {
            return Err(DdlError::internal("an edge insert lost its only edge"));
        };
        edge.op
    } else {
        let edges = prepared
            .into_iter()
            .map(|edge| batch_edge(edge.op))
            .collect::<Result<Vec<_>, _>>()?;
        GraphOp::EdgePutBatch { edges }
    };
    let plan = PhysicalPlan::Graph(op);

    let response = if let [only_home] = vshards.as_slice() {
        // Every endpoint shares one home, so one write there stores the edges.
        crate::control::server::sync::raft_dispatch::dispatch_trusted_internal_sync_response(
            state,
            tenant_id,
            database_id,
            *only_home,
            plan,
            crate::event::EventSource::User,
        )
        .await
        .map_err(|e| DdlError::from_error(&e))?
    } else {
        // Cross-shard: every home commits atomically through Calvin, each
        // running the edges it holds with identical surrogates.
        let task = PhysicalTask {
            tenant_id,
            vshard_id: first_home,
            database_id,
            plan,
            post_set_op: PostSetOp::None,
            txn_id: None,
        };
        let tx_class =
            build_static_tx_class(&[task], tenant_id, &[]).map_err(|e| DdlError::from_error(&e))?;
        // Every participant of this graph-only transaction reports its
        // answer in its completion ack, so the count arrives on any node.
        submit_calvin_routed_write(state, tx_class)
            .await
            .map_err(|e| DdlError::from_error(&e))?
    };
    data_plane_verdict(&response)?;
    response_affected(&response)
}

/// The batch form of a resolved `EdgePut`.
fn batch_edge(op: GraphOp) -> Result<BatchEdge, DdlError> {
    match op {
        GraphOp::EdgePut {
            collection,
            src_id,
            label,
            dst_id,
            properties,
            src_surrogate,
            dst_surrogate,
            valid_from_ms,
            valid_until_ms,
        } => Ok(BatchEdge {
            collection,
            src_id,
            label,
            dst_id,
            src_surrogate,
            dst_surrogate,
            valid_from_ms,
            valid_until_ms,
            properties,
        }),
        other => Err(DdlError::internal(format!(
            "edge insert resolved to a non-insert plan: {other:?}"
        ))),
    }
}
