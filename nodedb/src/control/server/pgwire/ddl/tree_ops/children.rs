//! `SELECT TREE_CHILDREN(graph_index, root_id)`
//!
//! BFS traversal from `root_id`, returns all descendant IDs.

use std::sync::Arc;

use futures::stream;
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response};
use pgwire::error::PgWireResult;
use sonic_rs;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::dispatch_utils;
use crate::control::server::pgwire::types::{sqlstate_error, text_field};
use crate::control::state::SharedState;

use super::parse::{extract_function_args, extract_number_after};

pub async fn tree_children(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    let tenant_id = identity.tenant_id;
    let upper = sql.to_uppercase();

    let args = extract_function_args(&upper, sql, "TREE_CHILDREN")?;
    if args.len() < 2 {
        return Err(sqlstate_error(
            "42601",
            "TREE_CHILDREN requires (graph_index, root_id)",
        ));
    }
    let graph_index = args[0].trim().to_lowercase();
    let root_id = args[1]
        .trim()
        .trim_matches('\'')
        .trim_matches('"')
        .to_string();

    let max_depth = extract_number_after(&upper, "MAX_DEPTH")?.unwrap_or(100);

    let dir = crate::engine::graph::edge_store::Direction::Out;
    let bfs_result = dispatch_utils::cross_core_bfs(
        state,
        tenant_id,
        vec![root_id],
        Some(graph_index),
        dir,
        max_depth,
    )
    .await
    .map_err(|e| sqlstate_error("XX000", &format!("BFS failed: {e}")))?;

    let bfs_json =
        crate::data::executor::response_codec::decode_payload_to_json(&bfs_result.payload);
    let node_ids: Vec<String> = sonic_rs::from_str::<Vec<serde_json::Value>>(&bfs_json)
        .unwrap_or_default()
        .into_iter()
        .filter_map(|v| v.as_str().map(String::from))
        .collect();

    let schema = Arc::new(vec![text_field("child_id")]);
    let mut rows = Vec::with_capacity(node_ids.len());
    for id in &node_ids {
        if id.is_empty() {
            continue;
        }
        let mut encoder = DataRowEncoder::new(schema.clone());
        encoder
            .encode_field(&id.to_string())
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        rows.push(Ok(encoder.take_row()));
    }

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    ))])
}
