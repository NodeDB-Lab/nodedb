// SPDX-License-Identifier: BUSL-1.1

//! Plan-to-translator routing: inspect the executed [`PhysicalPlan`] and
//! apply the matching surrogate → user-PK translator, or return the payload
//! untouched for every plan kind that carries no search hits to translate.

use nodedb_types::{DatabaseId, TenantId};

use crate::bridge::envelope::PhysicalPlan;
use crate::control::state::SharedState;
use nodedb_physical::physical_plan::{TextOp, VectorOp};

use super::text_hybrid::{translate_hybrid_search_payload, translate_text_search_payload};
use super::vector::translate_vector_search_payload;

/// Inspect the executed plan and apply the surrogate→PK translation that
/// matches its shape: vector search hits, full-text search hits, or hybrid
/// (RRF) fusion hits. Every other plan kind returns the payload untouched.
pub fn translate_search_response(
    payload: &[u8],
    plan: &PhysicalPlan,
    state: &SharedState,
    database_id: DatabaseId,
    tenant_id: TenantId,
) -> Vec<u8> {
    // The coordinator wraps sharded reads (including search) in an
    // `Exchange{Gather}` node; unwrap it so the underlying op is visible and
    // translation still runs on the gathered payload.
    if let PhysicalPlan::Query(nodedb_physical::physical_plan::QueryOp::Exchange(op)) = plan {
        return translate_search_response(payload, &op.child, state, database_id, tenant_id);
    }

    match plan {
        PhysicalPlan::Vector(VectorOp::Search {
            collection,
            rls_filters,
            top_k,
            ..
        })
        | PhysicalPlan::Vector(VectorOp::MultiSearch {
            collection,
            rls_filters,
            top_k,
            ..
        }) => translate_vector_search_payload(
            payload,
            state,
            database_id,
            tenant_id,
            collection.as_str(),
            rls_filters.as_slice(),
            *top_k,
        ),
        PhysicalPlan::Vector(VectorOp::MultiVectorScoreSearch {
            collection, top_k, ..
        })
        | PhysicalPlan::Vector(VectorOp::SparseSearch {
            collection, top_k, ..
        }) => translate_vector_search_payload(
            payload,
            state,
            database_id,
            tenant_id,
            collection.as_str(),
            &[],
            *top_k,
        ),
        PhysicalPlan::Text(
            TextOp::Search { collection, .. }
            | TextOp::PhraseSearch { collection, .. }
            | TextOp::BM25ScoreScan { collection, .. },
        ) => translate_text_search_payload(
            payload,
            state,
            database_id,
            tenant_id,
            collection.as_str(),
        ),
        PhysicalPlan::Text(TextOp::HybridSearch { collection, .. })
        | PhysicalPlan::Text(TextOp::HybridSearchTriple { collection, .. }) => {
            translate_hybrid_search_payload(
                payload,
                state,
                database_id,
                tenant_id,
                collection.as_str(),
            )
        }
        _ => payload.to_vec(),
    }
}
