// SPDX-License-Identifier: BUSL-1.1

//! Plan-to-translator routing: inspect the executed [`PhysicalPlan`] and
//! apply the matching surrogate → user-PK translator, or return the payload
//! untouched for every plan kind that carries no search hits to translate.

use nodedb_types::{DatabaseId, TenantId};

use crate::bridge::envelope::PhysicalPlan;
use crate::control::state::SharedState;
use nodedb_physical::physical_plan::{GraphOp, QueryOp, RagStage, TextOp, VectorOp};

use super::rag_fusion::translate_rag_fusion_payload;
use super::text_hybrid::{translate_hybrid_search_payload, translate_text_search_payload};
use super::vector::{VectorHitCut, translate_vector_search_payload};

/// Where a translator runs: the node's state and the request's scope.
#[derive(Clone, Copy)]
struct TranslateScope<'a> {
    state: &'a SharedState,
    database_id: DatabaseId,
    tenant_id: TenantId,
}

/// Inspect the executed plan and apply the surrogate→PK translation that
/// matches its shape: vector search hits, full-text search hits, hybrid
/// (RRF) fusion hits, or GraphRAG fusion rows. Every other plan kind returns
/// the payload untouched.
pub async fn translate_search_response(
    payload: &[u8],
    plan: &PhysicalPlan,
    state: &SharedState,
    database_id: DatabaseId,
    tenant_id: TenantId,
) -> crate::Result<Vec<u8>> {
    // The coordinator wraps sharded reads (search included) in an
    // `Exchange{Gather}` node. The underlying op decides the translation.
    let mut plan = plan;
    while let PhysicalPlan::Query(QueryOp::Exchange(op)) = plan {
        plan = &*op.child;
    }
    let scope = TranslateScope {
        state,
        database_id,
        tenant_id,
    };
    match plan {
        PhysicalPlan::Vector(op) => translate_vector_op(payload, op, scope).await,
        PhysicalPlan::Text(op) => translate_text_op(payload, op, scope).await,
        PhysicalPlan::Graph(op) => translate_graph_op(payload, op, scope).await,
        PhysicalPlan::Document(_)
        | PhysicalPlan::Kv(_)
        | PhysicalPlan::Columnar(_)
        | PhysicalPlan::Timeseries(_)
        | PhysicalPlan::Spatial(_)
        | PhysicalPlan::Crdt(_)
        | PhysicalPlan::Query(_)
        | PhysicalPlan::Meta(_)
        | PhysicalPlan::Array(_)
        | PhysicalPlan::ClusterArray(_)
        | PhysicalPlan::ClusterEvent(_) => Ok(payload.to_vec()),
    }
}

/// The vector search ops' translation. Every other vector op carries no hits.
async fn translate_vector_op(
    payload: &[u8],
    op: &VectorOp,
    scope: TranslateScope<'_>,
) -> crate::Result<Vec<u8>> {
    let (collection, cut) = match op {
        // A slow-path search attached each body for its column projection,
        // so the body stays after the filter. The fast path attached it only
        // for the predicate.
        VectorOp::Search {
            collection,
            rls_filters,
            top_k,
            skip_payload_fetch,
            ..
        } => (
            collection,
            VectorHitCut {
                rls_filters: rls_filters.as_slice(),
                top_k: *top_k,
                strip_bodies: *skip_payload_fetch,
            },
        ),
        VectorOp::MultiSearch {
            collection,
            rls_filters,
            top_k,
            ..
        } => (
            collection,
            VectorHitCut {
                rls_filters: rls_filters.as_slice(),
                top_k: *top_k,
                strip_bodies: true,
            },
        ),
        // A sparse search attaches each body for its column projection, so
        // the body stays after the filter.
        VectorOp::SparseSearch {
            collection,
            filters,
            top_k,
            ..
        } => (
            collection,
            VectorHitCut {
                rls_filters: filters.as_slice(),
                top_k: *top_k,
                strip_bodies: false,
            },
        ),
        VectorOp::MultiVectorScoreSearch {
            collection, top_k, ..
        } => (
            collection,
            VectorHitCut {
                rls_filters: &[],
                top_k: *top_k,
                strip_bodies: false,
            },
        ),
        // Writes, maintenance and index stats answer no search hits.
        VectorOp::Insert { .. }
        | VectorOp::BatchInsert { .. }
        | VectorOp::Delete { .. }
        | VectorOp::DeleteBySurrogate { .. }
        | VectorOp::SetParams { .. }
        | VectorOp::DropIndex { .. }
        | VectorOp::QueryStats { .. }
        | VectorOp::Seal { .. }
        | VectorOp::CompactIndex { .. }
        | VectorOp::Rebuild { .. }
        | VectorOp::SparseInsert { .. }
        | VectorOp::SparseDelete { .. }
        | VectorOp::MultiVectorInsert { .. }
        | VectorOp::MultiVectorDelete { .. }
        | VectorOp::DirectUpsert { .. }
        | VectorOp::DirectInsert { .. }
        | VectorOp::DirectInsertIfAbsent { .. }
        | VectorOp::DirectBatchWrite { .. }
        | VectorOp::DirectDelete { .. }
        | VectorOp::DirectTruncate { .. }
        | VectorOp::DirectUpdate { .. }
        | VectorOp::ResolveDirectWrite(_)
        | VectorOp::ResolvedDirectWrite { .. } => return Ok(payload.to_vec()),
    };
    translate_vector_search_payload(
        payload,
        scope.state,
        scope.database_id,
        scope.tenant_id,
        collection.as_str(),
        cut,
    )
    .await
}

/// The text search ops' translation. Every other text op carries no hits.
async fn translate_text_op(
    payload: &[u8],
    op: &TextOp,
    scope: TranslateScope<'_>,
) -> crate::Result<Vec<u8>> {
    match op {
        TextOp::Search { collection, .. }
        | TextOp::PhraseSearch { collection, .. }
        | TextOp::BM25ScoreScan { collection, .. } => {
            translate_text_search_payload(
                payload,
                scope.state,
                scope.database_id,
                scope.tenant_id,
                collection.as_str(),
            )
            .await
        }
        TextOp::HybridSearch { collection, .. } | TextOp::HybridSearchTriple { collection, .. } => {
            translate_hybrid_search_payload(
                payload,
                scope.state,
                scope.database_id,
                scope.tenant_id,
                collection.as_str(),
            )
            .await
        }
        TextOp::FtsIndexDoc { .. } | TextOp::FtsDeleteDoc { .. } | TextOp::SetTextConfig { .. } => {
            Ok(payload.to_vec())
        }
    }
}

/// A whole GraphRAG fusion's translation. Its coordinator stages and every
/// other graph op answer no client rows to name.
async fn translate_graph_op(
    payload: &[u8],
    op: &GraphOp,
    scope: TranslateScope<'_>,
) -> crate::Result<Vec<u8>> {
    match op {
        GraphOp::RagFusion {
            collection,
            stage: RagStage::Local,
            ..
        } => {
            translate_rag_fusion_payload(
                payload,
                scope.state,
                scope.database_id,
                scope.tenant_id,
                collection.as_str(),
            )
            .await
        }
        GraphOp::RagFusion {
            stage: RagStage::ExportLegs | RagStage::Bindings { .. },
            ..
        }
        | GraphOp::EdgePut { .. }
        | GraphOp::EdgePutBatch { .. }
        | GraphOp::EdgeDelete { .. }
        | GraphOp::EdgeDeleteBatch { .. }
        | GraphOp::ResolveEdgeDelete(_)
        | GraphOp::SetNodeLabels { .. }
        | GraphOp::RemoveNodeLabels { .. }
        | GraphOp::Hop { .. }
        | GraphOp::Neighbors { .. }
        | GraphOp::NeighborsMulti { .. }
        | GraphOp::Path { .. }
        | GraphOp::Subgraph { .. }
        | GraphOp::Algo { .. }
        | GraphOp::Match { .. }
        | GraphOp::MatchContinuation { .. }
        | GraphOp::MatchVarLenResume { .. }
        | GraphOp::BspSuperstep(_)
        | GraphOp::WccSuperstep(_)
        | GraphOp::TemporalNeighbors { .. }
        | GraphOp::Stats { .. }
        | GraphOp::NodeEdgeGuard { .. }
        | GraphOp::NodePresenceGuard { .. }
        | GraphOp::TruncateEdges { .. }
        | GraphOp::NodePresenceRead { .. } => Ok(payload.to_vec()),
    }
}
