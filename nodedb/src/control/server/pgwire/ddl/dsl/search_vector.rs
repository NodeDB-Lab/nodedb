//! `SEARCH <collection> USING VECTOR(...)` DSL.

use std::sync::Arc;
use std::time::Duration;

use futures::stream;
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response};
use pgwire::error::PgWireResult;

use crate::bridge::envelope::PhysicalPlan;
use crate::bridge::physical_plan::VectorOp;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::pgwire::types::{sqlstate_error, text_field};
use crate::control::state::SharedState;

/// SEARCH <collection> USING VECTOR(ARRAY[...], <k>)
/// SEARCH <collection> USING VECTOR(ARRAY[...], <k>) WITH FILTER <field> <op> <value>
pub async fn search_vector(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    let parts: Vec<&str> = sql.split_whitespace().collect();
    if parts.len() < 4 {
        return Err(sqlstate_error(
            "42601",
            "syntax: SEARCH <collection> USING VECTOR(ARRAY[...], <k>)",
        ));
    }
    let collection = parts[1];
    let tenant_id = identity.tenant_id;

    let vector_paren = sql.find("VECTOR(").or_else(|| sql.find("vector("));
    let vector_paren = match vector_paren {
        Some(i) => i + 7,
        None => {
            return Err(sqlstate_error(
                "42601",
                "expected VECTOR(...) in SEARCH USING VECTOR",
            ));
        }
    };

    let array_start = sql.find("ARRAY[").or_else(|| sql.find("array["));
    let array_start = match array_start {
        Some(i) => i + 6,
        None => {
            return Err(sqlstate_error(
                "42601",
                "expected ARRAY[...] in SEARCH USING VECTOR",
            ));
        }
    };

    let field_name = sql[vector_paren..array_start - 6]
        .trim()
        .trim_end_matches(',')
        .trim()
        .to_string();

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
        return Err(sqlstate_error("42601", "empty query vector"));
    }

    let after_array = &sql[array_end + 1..];
    let top_k = after_array
        .split(|c: char| !c.is_ascii_digit())
        .find(|s| !s.is_empty())
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(10);

    let filter_bitmap: Option<std::sync::Arc<[u8]>> = None;

    let plan = PhysicalPlan::Vector(VectorOp::Search {
        collection: collection.to_string(),
        query_vector: Arc::from(query_vector.as_slice()),
        top_k,
        ef_search: 0,
        filter_bitmap,
        field_name,
        rls_filters: Vec::new(),
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
