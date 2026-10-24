// SPDX-License-Identifier: BUSL-1.1

//! Per-edge resolution for the batched edge statements.
//!
//! Split out of `edge_batch.rs` to stay under the per-file line limit. The
//! batch entry points share one shape: resolve each triple's surrogates and its
//! row-level-security verdict, then decide whether the edge can ride a
//! property-less `BatchEdge` plan or has to keep the single-edge route.

use nodedb_physical::physical_plan::BatchEdge;
use nodedb_sql::ddl_ast::GraphEdgeTuple;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::surrogate_exchange::assign_surrogates_routed;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TraceId, VShardId};
use nodedb_types::Surrogate;

use super::super::super::result::DdlError;
use super::bounded_edges::BoundedEdges;
use super::support::ddl_err;

/// Resolve surrogate identities and the RLS verdict for every triple in a
/// `GRAPH DELETE EDGES` statement.
///
/// The verdict belongs to the injection pass: a delete carries no image, so a
/// policy compiles into the plan's write-gate slot. Each edge carries its own
/// verdict in `BatchEdge::write_gate`: `Predicate` when a policy restricts the
/// identity here, `NoPolicyApplies` otherwise. Every edge rides the batch.
pub(super) async fn resolve_delete_edges(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    collection: &str,
    edges: BoundedEdges,
) -> Result<Vec<BatchEdge>, DdlError> {
    let tenant_id = identity.tenant_id;
    let key = nodedb_types::CollectionKey::from_bare(database_id, collection);
    let mut batch: Vec<BatchEdge> = Vec::new();

    let surrogates = assign_endpoints(state, key, tenant_id, &edges).await?;
    debug_assert_eq!(
        edges.len(),
        surrogates.len(),
        "one surrogate pair per edge, else zip drops edges"
    );
    for (tuple, (src_surrogate, dst_surrogate)) in edges.into_iter().zip(surrogates) {
        let resolved = super::edge_rls::resolve_edge_write_rls(
            state,
            identity,
            database_id,
            nodedb_physical::physical_plan::GraphOp::EdgeDelete {
                collection: nodedb_types::QualifiedCollection::new(database_id, collection),
                src_id: tuple.src,
                label: tuple.label,
                dst_id: tuple.dst,
                src_surrogate,
                dst_surrogate,
                rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
            },
        )?;
        let nodedb_physical::physical_plan::GraphOp::EdgeDelete {
            collection: qc,
            src_id,
            label,
            dst_id,
            src_surrogate,
            dst_surrogate,
            rls_write_check,
        } = resolved
        else {
            return Err(ddl_err(
                "XX000",
                "edge delete plan changed shape during RLS resolution",
            ));
        };
        batch.push(BatchEdge {
            collection: qc,
            src_id,
            label,
            dst_id,
            src_surrogate,
            dst_surrogate,
            write_gate: rls_write_check,
        });
    }

    Ok(batch)
}

/// Decide the write gate of every governed edge before any edge is written.
///
/// A live predicate cannot be sequenced or replicated: a follower has no
/// writing identity, and a destination home holds no forward row to judge.
/// So each source home decides its own governed edges in one read-only pass
/// (`ResolveEdgeDelete` over an `EdgeDeleteBatch`). One denied edge refuses the
/// whole statement here, with nothing written. Each admitted edge then carries
/// `decided_earlier_in_request`. An edge with no predicate costs nothing.
///
/// The read and the later write are two steps, as in the single-edge delete:
/// an edge changed between them is decided against its earlier image.
pub(super) async fn decide_governed_deletes(
    state: &SharedState,
    tenant_id: crate::types::TenantId,
    database_id: DatabaseId,
    batch: &mut [BatchEdge],
) -> Result<(), DdlError> {
    use std::collections::BTreeMap;

    use nodedb_physical::physical_plan::GraphOp;

    let mut buckets: BTreeMap<u32, Vec<BatchEdge>> = BTreeMap::new();
    for edge in batch.iter().filter(|e| e.write_gate.has_predicate()) {
        buckets
            .entry(VShardId::from_key(edge.src_id.as_bytes()).as_u32())
            .or_default()
            .push(edge.clone());
    }
    for (vshard, group) in buckets {
        let plan = crate::bridge::envelope::PhysicalPlan::Graph(GraphOp::ResolveEdgeDelete(
            Box::new(GraphOp::EdgeDeleteBatch { edges: group }),
        ));
        let response = crate::control::maintenance::clone_materializer::dispatch_local_on_vshard(
            state,
            tenant_id,
            database_id,
            VShardId::new(vshard),
            plan,
            None,
        )
        .await
        .map_err(|e| DdlError::from_error(&e))?;
        if response.status != crate::bridge::envelope::Status::Ok {
            // A policy refusal surfaces as the same `RejectedAuthz` a directly
            // dispatched statement returns.
            let error = match response.error_code {
                Some(code) => match *code {
                    crate::bridge::envelope::ErrorCode::RejectedAuthz { resource } => {
                        crate::Error::RejectedAuthz {
                            tenant_id,
                            resource,
                        }
                    }
                    other => crate::Error::DataPlane(other),
                },
                None => crate::Error::Dispatch {
                    detail: format!(
                        "batch edge delete: write-gate pass returned status {:?} with no error code",
                        response.status
                    ),
                },
            };
            return Err(DdlError::from_error(&error));
        }
    }
    for edge in batch.iter_mut().filter(|e| e.write_gate.has_predicate()) {
        edge.write_gate = nodedb_types::RlsWriteCheck::decided_earlier_in_request();
    }
    Ok(())
}

/// Assign the surrogate of every endpoint of `edges`, as `(src, dst)` pairs in
/// `edges` order.
///
/// One call per statement. Each endpoint the local catalog does not bind goes
/// to the collection's home as part of one request set, so a statement costs
/// one assignment round instead of two per edge.
async fn assign_endpoints(
    state: &SharedState,
    key: nodedb_types::CollectionKey<'_>,
    tenant_id: crate::types::TenantId,
    edges: &[GraphEdgeTuple],
) -> Result<Vec<(Surrogate, Surrogate)>, DdlError> {
    let pks: Vec<&[u8]> = edges
        .iter()
        .flat_map(|t| [t.src.as_bytes(), t.dst.as_bytes()])
        .collect();
    let bound = assign_surrogates_routed(state, key, tenant_id, &pks, TraceId::ZERO)
        .await
        .map_err(|e| DdlError::from_error(&e))?;
    Ok(bound
        .as_chunks::<2>()
        .0
        .iter()
        .map(|[s, d]| (*s, *d))
        .collect())
}

/// One batch's worth of resolved insert edges.
pub(super) struct ResolvedInsert {
    /// Edges that can ride a property-less `EdgePutBatch` plan.
    pub batch: Vec<BatchEdge>,
}

/// Resolve surrogate identities and the write-policy verdict for every triple
/// in a `GRAPH INSERT EDGES` statement.
pub(super) async fn resolve_insert_edges(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    collection: &str,
    edges: BoundedEdges,
) -> Result<ResolvedInsert, DdlError> {
    let tenant_id = identity.tenant_id;
    let key = nodedb_types::CollectionKey::from_bare(database_id, collection);
    let mut batch: Vec<BatchEdge> = Vec::new();

    let surrogates = assign_endpoints(state, key, tenant_id, &edges).await?;
    debug_assert_eq!(
        edges.len(),
        surrogates.len(),
        "one surrogate pair per edge, else zip drops edges"
    );
    for (tuple, (src_surrogate, dst_surrogate)) in edges.into_iter().zip(surrogates) {
        let resolved = super::edge_rls::resolve_edge_write_rls(
            state,
            identity,
            database_id,
            nodedb_physical::physical_plan::GraphOp::EdgePut {
                collection: nodedb_types::QualifiedCollection::new(database_id, collection),
                src_id: tuple.src,
                label: tuple.label,
                dst_id: tuple.dst,
                properties: Vec::new(),
                src_surrogate,
                dst_surrogate,
            },
        )?;

        match resolved {
            nodedb_physical::physical_plan::GraphOp::EdgePut {
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
                    // A put is decided at injection, over its (empty) image.
                    write_gate: nodedb_types::RlsWriteCheck::decided_earlier_in_request(),
                });
            }
            // `BatchEdge` carries no properties, so an image-carrying plan
            // cannot ride the batch. Injection reads the image and never writes
            // it, so this arm is unreachable; fail loudly instead of dropping
            // the image if that ever changes.
            other => {
                return Err(DdlError::internal(format!(
                    "edge batch resolution cannot batch plan: {other:?}"
                )));
            }
        }
    }

    Ok(ResolvedInsert { batch })
}

/// Delete one edge through the single-edge path and return the count it
/// reported, so a batch caller sums real removals instead of counting triples.
#[allow(clippy::too_many_arguments)]
pub(super) async fn delete_one_summing(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    collection: &str,
    src: String,
    dst: String,
    label: String,
    txn_ctx: &crate::control::server::shared::session::DmlTxnCtx<'_>,
) -> Result<u64, DdlError> {
    let mut affected: u64 = 0;
    for result in super::edge::delete_edge(
        state,
        identity,
        database_id,
        super::edge::EdgeRef {
            collection: collection.to_string(),
            src,
            dst,
            label,
        },
        txn_ctx,
    )
    .await?
    {
        if let crate::control::server::shared::ddl::result::DdlResult::Status {
            rows_affected: Some(n),
            ..
        } = result
        {
            affected = affected.saturating_add(n);
        }
    }
    Ok(affected)
}

/// Bucket by source home and dispatch one apply burst per home, returning the
/// summed removed count.
///
/// Single-node only: the source home writes both the forward and the reverse
/// row locally, so each edge belongs to exactly one bucket and no edge is
/// counted twice. The count comes from the Data Plane's own live-pre-image
/// read, never from the batch length.
pub(super) async fn dispatch_delete_batches_by_src_home(
    state: &SharedState,
    tenant_id: crate::types::TenantId,
    database_id: DatabaseId,
    edges: Vec<BatchEdge>,
) -> Result<u64, DdlError> {
    use std::collections::BTreeMap;

    let mut buckets: BTreeMap<u32, Vec<BatchEdge>> = BTreeMap::new();
    for edge in edges {
        buckets
            .entry(VShardId::from_key(edge.src_id.as_bytes()).as_u32())
            .or_default()
            .push(edge);
    }

    let mut removed: u64 = 0;
    for (vshard, group) in buckets {
        let plan = crate::bridge::envelope::PhysicalPlan::Graph(
            nodedb_physical::physical_plan::GraphOp::EdgeDeleteBatch { edges: group },
        );
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
        super::support::data_plane_verdict(&response)?;
        removed =
            removed.saturating_add(super::edge::affected_count(&response, "batch edge delete")?);
    }
    Ok(removed)
}
