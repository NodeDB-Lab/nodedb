//! Shared handler for all graph-vector fusion SQL surfaces.
//!
//! Both `GRAPH RAG FUSION ON <col> ...` and `SEARCH <col> USING FUSION(...)`
//! parse into the same [`FusionParams`] typed bag and dispatch through
//! this single function, so caps and defaults cannot drift between the
//! two surfaces.

use std::sync::Arc;
use std::time::Duration;

use futures::stream;
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response};
use pgwire::error::PgWireResult;

use nodedb_sql::ddl_ast::FusionParams;
use nodedb_sql::ddl_ast::GraphDirection;

use crate::bridge::envelope::PhysicalPlan;
use crate::bridge::physical_plan::GraphOp;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::pgwire::ddl::sync_dispatch;
use crate::control::server::pgwire::types::{sqlstate_error, text_field};
use crate::control::state::SharedState;
use crate::data::executor::response_codec;
use crate::engine::graph::edge_store::Direction;
use crate::engine::graph::traversal_options::{GraphTraversalOptions, MAX_GRAPH_TRAVERSAL_DEPTH};

const FUSION_VECTOR_TOP_K_CAP: usize = 10_000;
const FUSION_TOP_CAP: usize = 10_000;

pub async fn rag_fusion(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    collection: String,
    params: FusionParams,
) -> PgWireResult<Vec<Response>> {
    let query_vector = params
        .query_vector
        .ok_or_else(|| sqlstate_error("42601", "fusion query requires ARRAY[…] vector payload"))?;
    if query_vector.is_empty() {
        return Err(sqlstate_error("42601", "query vector must not be empty"));
    }

    let vector_top_k = params.vector_top_k.unwrap_or(20);
    if vector_top_k > FUSION_VECTOR_TOP_K_CAP {
        return Err(sqlstate_error(
            "22023",
            &format!(
                "VECTOR_TOP_K {vector_top_k} exceeds maximum allowed value \
                 {FUSION_VECTOR_TOP_K_CAP}"
            ),
        ));
    }

    let expansion_depth = params.expansion_depth.unwrap_or(2);
    if expansion_depth > MAX_GRAPH_TRAVERSAL_DEPTH {
        return Err(sqlstate_error(
            "22023",
            &format!(
                "EXPANSION_DEPTH {expansion_depth} exceeds maximum allowed value \
                 {MAX_GRAPH_TRAVERSAL_DEPTH}"
            ),
        ));
    }

    let final_top_k = params.final_top_k.unwrap_or(10);
    if final_top_k > FUSION_TOP_CAP {
        return Err(sqlstate_error(
            "22023",
            &format!("FINAL_TOP_K {final_top_k} exceeds maximum allowed value {FUSION_TOP_CAP}"),
        ));
    }

    let rrf_k = params.rrf_k.unwrap_or((60.0, 60.0));

    let engine_direction = match params.direction {
        Some(GraphDirection::In) => Direction::In,
        Some(GraphDirection::Both) => Direction::Both,
        _ => Direction::Out,
    };

    let options = match params.max_visited {
        Some(mv) => GraphTraversalOptions {
            max_visited: mv,
            ..Default::default()
        },
        None => GraphTraversalOptions::default(),
    };

    let plan = PhysicalPlan::Graph(GraphOp::RagFusion {
        collection: collection.clone(),
        query_vector,
        vector_top_k,
        edge_label: params.edge_label,
        direction: engine_direction,
        expansion_depth,
        final_top_k,
        rrf_k,
        vector_field: params.vector_field.unwrap_or_default(),
        options,
    });

    let payload = sync_dispatch::dispatch_async(
        state,
        identity.tenant_id,
        &collection,
        plan,
        Duration::from_secs(state.tuning.network.default_deadline_secs),
    )
    .await
    .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;

    let schema = Arc::new(vec![text_field("result")]);
    let json_text = response_codec::decode_payload_to_json(&payload);
    let mut encoder = DataRowEncoder::new(schema.clone());
    encoder
        .encode_field(&json_text)
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
    let row = encoder.take_row();
    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(vec![Ok(row)]),
    ))])
}
