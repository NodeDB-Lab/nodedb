// SPDX-License-Identifier: BUSL-1.1

//! Batched edge mutation handlers: GRAPH INSERT EDGES, GRAPH DELETE EDGES.
//!
//! Split out of `edge.rs` to stay under the per-file line limit. The single
//! edge forms and their `EdgeRef`/`EdgeHomes` types stay in `edge.rs`; these
//! handlers reuse them edge by edge so surrogate assignment, write policy
//! resolution, dual-home routing, and Calvin semantics remain identical.

use std::collections::BTreeMap;

use nodedb_physical::physical_plan::{BatchEdge, GraphOp};
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};
use nodedb_sql::ddl_ast::{GraphEdgeTuple, GraphProperties};

use crate::bridge::envelope::PhysicalPlan;
use crate::control::planner::calvin::{build_static_tx_class, submit_calvin_routed};
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::session::{DmlTxnCtx, TransactionState};
use crate::control::server::surrogate_exchange::assign_surrogate_routed;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TraceId, VShardId};

use super::super::super::result::{DdlError, DdlResult};
use super::edge::{EdgeRef, delete_edge, insert_edge};
use super::edge_parse::validate_edge_label;
use super::support::{data_plane_verdict, ddl_err};

/// `GRAPH INSERT EDGES IN '<collection>' VALUES ('<src>','<dst>','<label>'), ...`
///
/// Batched twin of [`insert_edge`]. One statement carries many property-less
/// edges and costs one round trip; each edge then reuses the single-edge path,
/// so surrogate assignment, RLS resolution, dual-home routing, and Calvin
/// semantics stay identical to the singular form.
///
/// Collapsing this loop into one `GraphOp::EdgePutBatch` per home vShard is the
/// follow-up: it needs the physical
/// `BatchEdge` to carry a property object first, otherwise prop-less here does
/// not stay prop-less there.
pub async fn insert_edges(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    collection: String,
    edges: Vec<GraphEdgeTuple>,
    txn_ctx: &DmlTxnCtx<'_>,
) -> Result<Vec<DdlResult>, DdlError> {
    validate_edge_batch(&collection, &edges, "GRAPH INSERT EDGES")?;
    let count = edges.len() as u64;
    let tenant_id = identity.tenant_id;

    // Inside a transaction, the staging overlay is per edge; keep the
    // per-edge path so RYOW and rollback semantics stay identical.
    if txn_ctx.sessions.transaction_state(txn_ctx.session_id) == TransactionState::InBlock {
        for tuple in edges {
            insert_edge(
                state,
                identity,
                database_id,
                EdgeRef {
                    collection: collection.clone(),
                    src: tuple.src,
                    dst: tuple.dst,
                    label: tuple.label,
                },
                GraphProperties::None,
                txn_ctx,
            )
            .await?;
        }
        return Ok(vec![DdlResult::Status {
            command: "INSERT EDGES".to_string(),
            rows_affected: Some(count),
        }]);
    }

    // One collection flag for the whole batch; the call is idempotent.
    crate::control::planner::implicit_edges::mark_collection_edge_bearing(
        state,
        database_id,
        tenant_id,
        &collection,
    )
    .await
    .map_err(|e| ddl_err("XX000", e.to_string()))?;

    let calvin_available =
        state.cluster_transport.is_some() && state.sequencer_inbox.get().is_some();

    // Resolve every edge (surrogates + write policy) and collect it for the
    // batch plan.
    let mut batch: Vec<BatchEdge> = Vec::with_capacity(edges.len());
    // A policy that rewrites the property image cannot ride `BatchEdge`
    // (it carries no properties): those edges keep the single-edge dispatch.
    let mut singles: Vec<(VShardId, VShardId, GraphOp)> = Vec::new();
    for tuple in edges {
        if tuple.src.is_empty() || tuple.dst.is_empty() {
            return Err(ddl_err("42601", "GRAPH INSERT EDGES requires FROM and TO"));
        }
        validate_edge_label(&tuple.label)?;
        let vsrc = VShardId::from_key(tuple.src.as_bytes());
        let vdst = VShardId::from_key(tuple.dst.as_bytes());
        let src_surrogate = assign_surrogate_routed(
            state,
            vsrc,
            database_id,
            tenant_id,
            &collection,
            tuple.src.as_bytes(),
            TraceId::ZERO,
        )
        .await
        .map_err(|e| ddl_err("XX000", e.to_string()))?;
        let dst_surrogate = assign_surrogate_routed(
            state,
            vdst,
            database_id,
            tenant_id,
            &collection,
            tuple.dst.as_bytes(),
            TraceId::ZERO,
        )
        .await
        .map_err(|e| ddl_err("XX000", e.to_string()))?;

        let resolved = super::edge_rls::resolve_edge_write_rls(
            state,
            identity,
            database_id,
            GraphOp::EdgePut {
                collection: nodedb_types::QualifiedCollection::new(database_id, &collection),
                src_id: tuple.src,
                label: tuple.label,
                dst_id: tuple.dst,
                properties: Vec::new(),
                src_surrogate,
                dst_surrogate,
            },
        )?;

        match resolved {
            GraphOp::EdgePut {
                collection: qc,
                src_id,
                label,
                dst_id,
                properties,
                src_surrogate,
                dst_surrogate,
            } if properties.is_empty() => {
                batch.push(BatchEdge {
                    collection: qc,
                    src_id,
                    label,
                    dst_id,
                    src_surrogate,
                    dst_surrogate,
                });
            }
            other => singles.push((vsrc, vdst, other)),
        }
    }

    if calvin_available {
        // One tx class: the batch task routes to every home its edges touch,
        // and any policy-rewritten edge rides along as its own single task.
        let mut tasks: Vec<PhysicalTask> = Vec::with_capacity(1 + singles.len());
        if !batch.is_empty() {
            tasks.push(PhysicalTask {
                tenant_id,
                vshard_id: VShardId::from_key(collection.as_bytes()),
                database_id,
                plan: PhysicalPlan::Graph(GraphOp::EdgePutBatch { edges: batch }),
                post_set_op: PostSetOp::None,
                txn_id: None,
            });
        }
        for (vsrc, _vdst, op) in singles {
            tasks.push(PhysicalTask {
                tenant_id,
                vshard_id: vsrc,
                database_id,
                plan: PhysicalPlan::Graph(op),
                post_set_op: PostSetOp::None,
                txn_id: None,
            });
        }
        let tx_class = build_static_tx_class(&tasks, tenant_id, &[])
            .map_err(|e| ddl_err("XX000", e.to_string()))?;
        let response = submit_calvin_routed(state, tx_class)
            .await
            .map_err(|e| ddl_err("XX000", e.to_string()))?;
        if let Some(response) = response {
            data_plane_verdict(&response)?;
        }
    } else {
        // Single-node: bucket by src home and dispatch one apply burst per
        // home. A cross-shard edge is written by its src home, which writes
        // both the forward and reverse rows locally.
        let mut buckets: BTreeMap<u32, Vec<BatchEdge>> = BTreeMap::new();
        for edge in batch {
            buckets
                .entry(VShardId::from_key(edge.src_id.as_bytes()).as_u32())
                .or_default()
                .push(edge);
        }
        for (vshard, group) in buckets {
            let plan = PhysicalPlan::Graph(GraphOp::EdgePutBatch { edges: group });
            let response =
                crate::control::server::sync::raft_dispatch::dispatch_trusted_internal_sync_response(
                    state,
                    tenant_id,
                    database_id,
                    VShardId::new(vshard),
                    plan,
                    TraceId::ZERO,
                    crate::event::EventSource::User,
                )
                .await
                .map_err(|e| ddl_err("XX000", e.to_string()))?;
            data_plane_verdict(&response)?;
        }
        for (vsrc, vdst, op) in singles {
            let single_home = vsrc == vdst;
            if single_home {
                let response = crate::control::server::sync::raft_dispatch::dispatch_trusted_internal_sync_response(
                    state,
                    tenant_id,
                    database_id,
                    vsrc,
                    PhysicalPlan::Graph(op),
                    TraceId::ZERO,
                    crate::event::EventSource::User,
                )
                .await
                .map_err(|e| ddl_err("XX000", e.to_string()))?;
                data_plane_verdict(&response)?;
            } else {
                // Cross-shard without Calvin: write on the src home, which
                // owns both rows in single-node deployments.
                let response = crate::control::server::sync::raft_dispatch::dispatch_trusted_internal_sync_response(
                    state,
                    tenant_id,
                    database_id,
                    vsrc,
                    PhysicalPlan::Graph(op),
                    TraceId::ZERO,
                    crate::event::EventSource::User,
                )
                .await
                .map_err(|e| ddl_err("XX000", e.to_string()))?;
                data_plane_verdict(&response)?;
            }
        }
    }

    Ok(vec![DdlResult::Status {
        command: "INSERT EDGES".to_string(),
        rows_affected: Some(count),
    }])
}

/// `GRAPH DELETE EDGES IN '<collection>' VALUES ('<src>','<dst>','<label>'), ...`
///
/// Batched twin of [`delete_edge`], same shape and same per-edge reuse.
pub async fn delete_edges(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    collection: String,
    edges: Vec<GraphEdgeTuple>,
    txn_ctx: &DmlTxnCtx<'_>,
) -> Result<Vec<DdlResult>, DdlError> {
    validate_edge_batch(&collection, &edges, "GRAPH DELETE EDGES")?;
    let count = edges.len() as u64;
    for tuple in edges {
        delete_edge(
            state,
            identity,
            database_id,
            EdgeRef {
                collection: collection.clone(),
                src: tuple.src,
                dst: tuple.dst,
                label: tuple.label,
            },
            txn_ctx,
        )
        .await?;
    }
    Ok(vec![DdlResult::Status {
        command: "DELETE EDGES".to_string(),
        rows_affected: Some(count),
    }])
}

/// Shared guard for the batch entry points. The parser already enforces the
/// per-statement cap (`MAX_EDGES_PER_BATCH`); this rejects the empty and
/// collection-less shapes that can only arrive from a hand-built AST.
fn validate_edge_batch(
    collection: &str,
    edges: &[GraphEdgeTuple],
    stmt: &str,
) -> Result<(), DdlError> {
    if collection.is_empty() {
        return Err(ddl_err("42601", format!("{stmt} requires IN <collection>")));
    }
    if edges.is_empty() {
        return Err(ddl_err(
            "42601",
            format!("{stmt} requires at least one edge"),
        ));
    }
    Ok(())
}
