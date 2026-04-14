//! `SEARCH <collection> USING FUSION(...)` DSL (vector + graph fusion).

use std::sync::Arc;
use std::time::Duration;

use futures::stream;
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response};
use pgwire::error::PgWireResult;

use crate::bridge::envelope::PhysicalPlan;
use crate::bridge::physical_plan::GraphOp;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::pgwire::types::{sqlstate_error, text_field};
use crate::control::state::SharedState;

use super::helpers::{extract_param, extract_string_param};

/// SEARCH <collection> USING FUSION(VECTOR(ARRAY[...], <k>), GRAPH(<label>, <depth>), TOP <n>)
pub async fn search_fusion(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    let parts: Vec<&str> = sql.split_whitespace().collect();
    if parts.len() < 4 {
        return Err(sqlstate_error(
            "42601",
            "syntax: SEARCH <collection> USING FUSION(...)",
        ));
    }
    let collection = parts[1];
    let tenant_id = identity.tenant_id;

    let array_start = sql.find("ARRAY[").or_else(|| sql.find("array["));
    let array_start = match array_start {
        Some(i) => i + 6,
        None => {
            return Err(sqlstate_error("42601", "expected ARRAY[...] in FUSION"));
        }
    };
    let array_end = sql[array_start..].find(']').map(|i| i + array_start);
    let array_end = match array_end {
        Some(i) => i,
        None => {
            return Err(sqlstate_error("42601", "unterminated ARRAY["));
        }
    };

    let vector_str = &sql[array_start..array_end];
    let query_vector: Vec<f32> = vector_str
        .split(',')
        .filter_map(|s| s.trim().parse::<f32>().ok())
        .collect();

    if query_vector.is_empty() {
        return Err(sqlstate_error("42601", "empty query vector in FUSION"));
    }

    let upper = sql.to_uppercase();
    let vector_top_k = extract_param(&upper, "VECTOR_TOP_K").unwrap_or(20);
    let expansion_depth = extract_param(&upper, "DEPTH").unwrap_or(2);
    let final_top_k = extract_param(&upper, "TOP").unwrap_or(10);

    let edge_label = extract_string_param(sql, "LABEL");

    let plan = PhysicalPlan::Graph(GraphOp::RagFusion {
        collection: collection.to_string(),
        query_vector: Arc::from(query_vector.as_slice()),
        vector_top_k,
        edge_label,
        direction: crate::engine::graph::edge_store::Direction::Out,
        expansion_depth,
        final_top_k,
        rrf_k: (60.0, 60.0),
        options: crate::engine::graph::traversal_options::GraphTraversalOptions::default(),
    });

    let payload = crate::control::server::pgwire::ddl::sync_dispatch::dispatch_async(
        state,
        tenant_id,
        collection,
        plan,
        Duration::from_secs(state.tuning.network.default_deadline_secs),
    )
    .await
    .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;

    let schema = Arc::new(vec![text_field("result")]);
    let text = crate::data::executor::response_codec::decode_payload_to_json(&payload);
    let mut encoder = DataRowEncoder::new(schema.clone());
    encoder
        .encode_field(&text)
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
    let row = encoder.take_row();

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(vec![Ok(row)]),
    ))])
}
