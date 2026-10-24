// SPDX-License-Identifier: BUSL-1.1

//! Batched edge mutation handlers: GRAPH INSERT EDGES, GRAPH DELETE EDGES.
//!
//! Split out of `edge.rs` to stay under the per-file line limit. Every triple is
//! resolved (surrogates + write policy) on its own, then the batch is dispatched
//! according to the route the process can serve. A delete under a write policy
//! is decided before any write (`edge_batch_resolve::decide_governed_deletes`),
//! so a denied edge refuses the whole statement and the allowed ones survive:
//!
//!
//!  * Calvin available: ONE Calvin tx class holding ONE `GraphOp::EdgePutBatch` /
//!    `EdgeDeleteBatch` task that carries every edge, whatever their endpoint
//!    homes. The submit path slices the task per `(src home, dst home)` pair
//!    (`calvin::submit::edge_slices`), streams the slices as parts when one
//!    sequencer entry cannot hold them, and sums the homes' owned-edge counts
//!    (`ReplyFold::SumOwnedEdges`). The statement is one sequenced transaction:
//!    every edge lands or none does, and a deadline that ends the wait leaves an
//!    unknown but still all-or-nothing outcome. A retry is safe: a put
//!    rewrites the same edge and a delete of an absent edge counts zero.
//!  * No Calvin (standalone): one `EdgePutBatch` / `EdgeDeleteBatch` task per
//!    source home, with the removed count summed across the buckets. Buckets
//!    are applied one after another, so a batch over several homes is not
//!    atomic on this route.

use std::collections::BTreeMap;

use nodedb_physical::physical_plan::{BatchEdge, GraphOp};
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};
use nodedb_sql::ddl_ast::{GraphEdgeTuple, GraphProperties};

use crate::bridge::envelope::PhysicalPlan;
use crate::bridge::envelope::Response;
use crate::control::planner::calvin::{build_single_vshard_tx_class, submit_calvin_routed};
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::session::{DmlTxnCtx, TransactionState};
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId, VShardId};

use super::super::super::result::{DdlError, DdlResult};
use super::bounded_edges::BoundedEdges;
use super::edge::{EdgeRef, insert_edge};
use super::support::{data_plane_verdict, ddl_err};

/// `GRAPH INSERT EDGES IN '<collection>' VALUES ('<src>','<dst>','<label>'), ...`
///
/// Batched twin of [`insert_edge`]. One statement carries many property-less
/// edges; each edge reuses the single-edge resolution, so surrogate assignment,
/// RLS resolution, dual-home routing, and Calvin semantics stay identical to the
/// singular form. See the module doc for which route the batch takes.
///
/// `rows_affected` is the number of edges the statement writes: the list after
/// repeats are dropped. An insert cannot report fewer, because a put of an edge
/// that already exists still writes it.
pub async fn insert_edges(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    collection: String,
    edges: Vec<GraphEdgeTuple>,
    txn_ctx: &DmlTxnCtx<'_>,
) -> Result<Vec<DdlResult>, DdlError> {
    // Validate the whole list and drop repeats before a single tuple is staged.
    // A rejection on a later tuple must not leave the tuples before it in the
    // transaction's write set: the session stays in the block, so a later
    // COMMIT would apply them. `rows_affected` then counts the edges the
    // statement writes, not the triples it was given.
    let edges = BoundedEdges::new(&collection, edges, "GRAPH INSERT EDGES")?;
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
    .map_err(|e| DdlError::from_error(&e))?;

    let calvin_available =
        state.cluster_transport.is_some() && state.sequencer_inbox.get().is_some();

    // Resolve every edge (surrogates + write policy) before any task is built.
    // Admission decides per edge, so a governed edge is refused with its own
    // collection named rather than as an anonymous member of a batch.
    let resolved = super::edge_batch_resolve::resolve_insert_edges(
        state,
        identity,
        database_id,
        &collection,
        edges,
    )
    .await?;
    let batch: Vec<BatchEdge> = resolved.batch;

    if calvin_available {
        let home = batch_home(&batch)?;
        let task = batch_task(
            tenant_id,
            database_id,
            PhysicalPlan::Graph(GraphOp::EdgePutBatch { edges: batch }),
            home,
        );
        let response = submit_batch_tx(state, task, tenant_id).await?;
        // An insert reports no affected count, so a submit that returns no
        // applied response is accepted. The delete path refuses it, because it
        // reads its count from that response.
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
                    crate::event::EventSource::User,
                )
                .await
                .map_err(|e| DdlError::from_error(&e))?;
            data_plane_verdict(&response)?;
        }
    }

    Ok(vec![DdlResult::Status {
        command: "INSERT EDGES".to_string(),
        rows_affected: Some(count),
    }])
}

/// `GRAPH DELETE EDGES IN '<collection>' VALUES ('<src>','<dst>','<label>'), ...`
///
/// Batched twin of [`delete_edge`]. Every edge is resolved (surrogates + RLS)
/// individually and carries its own write gate. Under a write policy the
/// governed edges are decided first, in one read-only pass per source home:
/// one denied edge refuses the whole statement before any write. The reported count is the Data Plane's own live-removal count:
/// summed across the source-home buckets on the standalone route, and read from
/// the tx class's summed owned-edge answer on the Calvin route, where each home
/// counts the edges it owns and the answers add up. It is never the input
/// length, because an absent edge (or a duplicate triple) tombstones nothing.
/// See the module doc for which route the batch takes.
pub async fn delete_edges(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    collection: String,
    edges: Vec<GraphEdgeTuple>,
    txn_ctx: &DmlTxnCtx<'_>,
) -> Result<Vec<DdlResult>, DdlError> {
    let tenant_id = identity.tenant_id;

    let edges = BoundedEdges::new(&collection, edges, "GRAPH DELETE EDGES")?;

    // Inside a transaction the staging overlay is per edge; keep the per-edge
    // path so RYOW and rollback semantics stay identical. A delete's count is
    // read from the staged result, so this path stays exact too.
    if txn_ctx.sessions.transaction_state(txn_ctx.session_id) == TransactionState::InBlock {
        let mut affected: u64 = 0;
        for tuple in edges {
            affected = affected.saturating_add(
                super::edge_batch_resolve::delete_one_summing(
                    state,
                    identity,
                    database_id,
                    &collection,
                    tuple.src,
                    tuple.dst,
                    tuple.label,
                    txn_ctx,
                )
                .await?,
            );
        }
        return Ok(vec![DdlResult::Status {
            command: "DELETE EDGES".to_string(),
            rows_affected: Some(affected),
        }]);
    }

    // Calvin needs cluster mode with a wired sequencer; single-node already
    // tombstones both EDGES and REVERSE_EDGES in one write per home.
    let calvin_available =
        state.cluster_transport.is_some() && state.sequencer_inbox.get().is_some();

    // Resolve every edge (surrogates + RLS). Each edge carries its own write
    // gate in `BatchEdge::write_gate`.
    let mut batch: Vec<BatchEdge> = super::edge_batch_resolve::resolve_delete_edges(
        state,
        identity,
        database_id,
        &collection,
        edges,
    )
    .await?;

    // Decide the governed edges before any write: one denied edge refuses the
    // whole statement and nothing is deleted. The batch then rides the route
    // below as ONE transaction, policy or not.
    super::edge_batch_resolve::decide_governed_deletes(state, tenant_id, database_id, &mut batch)
        .await?;

    let removed: u64 = if calvin_available {
        let home = batch_home(&batch)?;
        let task = batch_task(
            tenant_id,
            database_id,
            PhysicalPlan::Graph(GraphOp::EdgeDeleteBatch { edges: batch }),
            home,
        );
        let response = submit_batch_tx(state, task, tenant_id).await?;
        let Some(response) = response else {
            return Err(ddl_err(
                "XX000",
                "batch edge delete completed with no applied response to read its affected \
                 count from",
            ));
        };
        data_plane_verdict(&response)?;
        super::edge::affected_count(&response, "batch edge delete")?
    } else {
        // Single-node: one apply burst per source home.
        super::edge_batch_resolve::dispatch_delete_batches_by_src_home(
            state,
            tenant_id,
            database_id,
            batch,
        )
        .await?
    };

    Ok(vec![DdlResult::Status {
        command: "DELETE EDGES".to_string(),
        rows_affected: Some(removed),
    }])
}

/// The source home of the first edge of a batch. The batch is never empty
/// here: `BoundedEdges` refuses an empty statement.
fn batch_home(edges: &[BatchEdge]) -> Result<VShardId, DdlError> {
    edges
        .first()
        .map(|e| VShardId::from_key(e.src_id.as_bytes()))
        .ok_or_else(|| ddl_err("XX000", "batch edge statement resolved to no edge"))
}

/// The one task of a batch's tx class. Its plan names every edge, so the
/// plan's routing is the union of the edges' homes. `vshard_id` is only the
/// first edge's source home: the submit path routes by the plan.
fn batch_task(
    tenant_id: TenantId,
    database_id: DatabaseId,
    plan: PhysicalPlan,
    vshard_id: VShardId,
) -> PhysicalTask {
    PhysicalTask {
        tenant_id,
        vshard_id,
        database_id,
        plan,
        post_set_op: PostSetOp::None,
        txn_id: None,
    }
}

/// Sequence `task` as one Calvin tx class and wait for its applied answer.
///
/// The single-vShard builder accepts a batch whose endpoints all share one
/// home as well as one that spans many; the strict builder would refuse the
/// first shape.
async fn submit_batch_tx(
    state: &SharedState,
    task: PhysicalTask,
    tenant_id: TenantId,
) -> Result<Option<Response>, DdlError> {
    let tx_class = build_single_vshard_tx_class(std::slice::from_ref(&task), tenant_id, &[])
        .map_err(|e| DdlError::from_error(&e))?;
    submit_calvin_routed(state, tx_class).await.map_err(|e| {
        let error = DdlError::from_error(&e);
        // The batch is one sequenced transaction. A deadline ends the wait, not
        // the transaction, so the client must not read the error as a refusal.
        if matches!(e, crate::Error::DeadlineExceeded { .. }) {
            error.in_context("batch outcome unknown, every edge landed or none did, retry is safe")
        } else {
            error
        }
    })
}
