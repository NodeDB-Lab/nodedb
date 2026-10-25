// SPDX-License-Identifier: BUSL-1.1

//! Shared handler for all graph-vector fusion SQL surfaces.
//!
//! Both `GRAPH RAG FUSION ON <col> ...` and `SEARCH <col> USING FUSION(...)`
//! parse into the same [`FusionParams`] typed bag and dispatch through
//! this single function, so caps and defaults cannot drift between the
//! two surfaces.

use serde_json::{Map, Value as JsonValue};

use nodedb_sql::ddl_ast::FusionParams;
use nodedb_sql::ddl_ast::GraphDirection;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::response_shape::types::ShapedRows;
use crate::control::server::response_translate::translate_search_response;
use crate::control::server::shared::ddl::user_dispatch;
use crate::control::server::shared::session::StatementDeadline;
use crate::control::state::SharedState;
use crate::data::executor::response_codec;
use crate::engine::graph::edge_store::Direction;
use crate::engine::graph::traversal_options::{GraphTraversalOptions, MAX_GRAPH_TRAVERSAL_DEPTH};
use crate::types::DatabaseId;
use nodedb_physical::physical_plan::GraphOp;

use super::super::super::result::{DdlError, DdlResult};
use super::support::ddl_err;

const FUSION_VECTOR_TOP_K_CAP: usize = 10_000;
const FUSION_TOP_CAP: usize = 10_000;

/// One fusion statement.
pub struct RagFusionRequest {
    pub collection: String,
    pub params: FusionParams,
    pub linearizable: bool,
    /// `AS OF SYSTEM TIME` / `AS OF VALID TIME`: the graph expansion reads
    /// the edges at that cut. The vector and text legs read their indexes
    /// as they stand.
    pub temporal: nodedb_sql::temporal::TemporalScope,
}

pub async fn rag_fusion(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    request: RagFusionRequest,
) -> Result<Vec<DdlResult>, DdlError> {
    let RagFusionRequest {
        collection,
        params,
        linearizable,
        temporal,
    } = request;
    // Gate on catalog `is_active` (see `support::ensure_collection_active`):
    // a plain `DROP COLLECTION` only flips `is_active=false` without
    // reclaiming edges/CSR, so this read must independently hide the target
    // collection until UNDROP or a hard purge.
    super::support::ensure_collection_active(
        state,
        database_id,
        identity.tenant_id.as_u64(),
        &collection,
    )?;

    let query_vector = params
        .query_vector
        .ok_or_else(|| ddl_err("42601", "fusion query requires ARRAY[…] vector payload"))?;
    if query_vector.is_empty() {
        return Err(ddl_err("42601", "query vector must not be empty"));
    }

    let vector_top_k = params.vector_top_k.unwrap_or(20);
    if vector_top_k > FUSION_VECTOR_TOP_K_CAP {
        return Err(ddl_err(
            "22023",
            format!(
                "VECTOR_TOP_K {vector_top_k} exceeds maximum allowed value \
                 {FUSION_VECTOR_TOP_K_CAP}"
            ),
        ));
    }

    let expansion_depth = params.expansion_depth.unwrap_or(2);
    if expansion_depth > MAX_GRAPH_TRAVERSAL_DEPTH {
        return Err(ddl_err(
            "22023",
            format!(
                "EXPANSION_DEPTH {expansion_depth} exceeds maximum allowed value \
                 {MAX_GRAPH_TRAVERSAL_DEPTH}"
            ),
        ));
    }

    let final_top_k = params.final_top_k.unwrap_or(10);
    if final_top_k > FUSION_TOP_CAP {
        return Err(ddl_err(
            "22023",
            format!("FINAL_TOP_K {final_top_k} exceeds maximum allowed value {FUSION_TOP_CAP}"),
        ));
    }

    // Resolve RRF k constants. A three-value triple takes precedence.
    let rrf_k_triple = params.rrf_k_triple;
    let rrf_k = params.rrf_k.unwrap_or((60.0, 60.0));

    let engine_direction = match params.direction {
        Some(GraphDirection::In) => Direction::In,
        Some(GraphDirection::Both) => Direction::Both,
        _ => Direction::Out,
    };

    let options = match params.max_visited {
        Some(mv) => GraphTraversalOptions { max_visited: mv },
        None => GraphTraversalOptions::default(),
    };

    let scope = crate::control::server::graph_dispatch::graph_read_scope(state, &temporal)
        .await
        .map_err(|e| DdlError::from_error(&e))?;

    let plan = PhysicalPlan::Graph(GraphOp::RagFusion {
        collection: nodedb_types::QualifiedCollection::new(database_id, &collection),
        query_vector,
        vector_top_k,
        edge_label: params.edge_label,
        direction: engine_direction,
        expansion_depth,
        final_top_k,
        rrf_k,
        rrf_k_triple,
        vector_field: params.vector_field.unwrap_or_default(),
        options,
        bm25_query: params.bm25_query,
        bm25_field: params.bm25_field,
        stage: nodedb_physical::physical_plan::RagStage::Local,
        scope,
    });

    // Only reached through `shared::ddl::dispatch`, which native/pgwire/HTTP
    // each call after their own single per-request admission gate.
    let payload = user_dispatch::dispatch_for_identity(user_dispatch::DispatchRequest {
        state,
        identity,
        database_id,
        collection: &collection,
        // The response translation below reads the plan the fusion ran.
        plan: plan.clone(),
        deadline: StatementDeadline::of_statement(state),
        // Admitted at this request's own transport entry; this handler has no
        // session or peer information of its own.
        admission: user_dispatch::RequestAdmission::AlreadyAdmitted,
        linearizable,
    })
    .await
    .map_err(|e| DdlError::from_error(&e))?;

    // Rows named by no graph node take their primary keys here, through the
    // resolver every search response uses.
    let named = translate_search_response(&payload, &plan, state, database_id, identity.tenant_id)
        .await
        .map_err(|e| DdlError::from_error(&e))?;
    let json_text = response_codec::decode_payload_to_json(&named);
    let mut row = Map::new();
    row.insert("result".to_string(), JsonValue::String(json_text));

    Ok(vec![DdlResult::Rows(ShapedRows::text_rows(
        vec!["result".to_string()],
        vec![row],
    ))])
}
