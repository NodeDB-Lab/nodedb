//! `SELECT TREE_SUM(column, graph_index, root_id [, collection]) [MAX_DEPTH n]`
//!
//! BFS traversal from `root_id`, summing column values over all descendants
//! + root. If `collection` is provided, lookups are O(N). Without it,
//! lookups scan all tenant collections (O(N×C)) — pass the collection
//! name for production use.

use std::sync::Arc;

use futures::stream;
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response};
use pgwire::error::PgWireResult;
use sonic_rs;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::dispatch_utils;
use crate::control::server::pgwire::types::{sqlstate_error, text_field};
use crate::control::state::SharedState;
use crate::types::VShardId;

use super::parse::{extract_function_args, extract_number_after, json_to_decimal};

pub async fn tree_sum(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    let tenant_id = identity.tenant_id;
    let upper = sql.to_uppercase();

    // Parse: TREE_SUM(<column>, <graph_index>, '<root_id>' [, '<collection>'])
    let args = extract_function_args(&upper, sql, "TREE_SUM")?;
    if args.len() < 3 {
        return Err(sqlstate_error(
            "42601",
            "TREE_SUM requires (column, graph_index, root_id [, collection])",
        ));
    }
    let sum_column = args[0].trim().to_lowercase();
    let graph_index = args[1].trim().to_lowercase();
    let root_id = args[2]
        .trim()
        .trim_matches('\'')
        .trim_matches('"')
        .to_string();
    let explicit_collection = args
        .get(3)
        .map(|s| s.trim().trim_matches('\'').trim_matches('"').to_lowercase());

    let max_depth = extract_number_after(&upper, "MAX_DEPTH")?.unwrap_or(100);

    // BFS traversal to get all descendant node IDs.
    let dir = crate::engine::graph::edge_store::Direction::Out;
    let bfs_result = dispatch_utils::cross_core_bfs(
        state,
        tenant_id,
        vec![root_id.clone()],
        Some(graph_index),
        dir,
        max_depth,
    )
    .await
    .map_err(|e| sqlstate_error("XX000", &format!("BFS failed: {e}")))?;

    // Parse BFS result as JSON array of node IDs.
    let bfs_json =
        crate::data::executor::response_codec::decode_payload_to_json(&bfs_result.payload);
    let bfs_nodes: Vec<String> = sonic_rs::from_str::<Vec<serde_json::Value>>(&bfs_json)
        .unwrap_or_default()
        .into_iter()
        .filter_map(|v| v.as_str().map(String::from))
        .collect();

    // Include root itself.
    let mut all_ids: Vec<String> = vec![root_id];
    for id in bfs_nodes {
        if !id.is_empty() && !all_ids.contains(&id) {
            all_ids.push(id);
        }
    }

    // Scan the collection to build a lookup map: doc_id → column_value.
    // More efficient than N point lookups for trees with many nodes.
    // We need to know which collection the graph index was built on.
    // For now, scan using a PointGet per node (the collection is implicit
    // from the graph index name — stored as edge label).
    //
    // Since we don't have a collection→graph_index mapping yet, we sum
    // by looking up each node as a document ID across known collections.
    // This is a limitation — proper graph index metadata would resolve it.
    let mut total = rust_decimal::Decimal::ZERO;

    // Look up each node's document to extract the sum column value.
    // When the collection is specified (4th arg), this is O(N) point lookups.
    // Without it, we fall back to scanning all tenant collections (O(N×C)).
    let collections_to_search: Vec<String> = if let Some(ref coll) = explicit_collection {
        vec![coll.clone()]
    } else if let Some(catalog) = state.credentials.catalog() {
        catalog
            .load_collections_for_tenant(tenant_id.as_u32())
            .unwrap_or_default()
            .iter()
            .map(|c| c.name.clone())
            .collect()
    } else {
        Vec::new()
    };

    for node_id in &all_ids {
        for coll_name in &collections_to_search {
            let coll_vshard = VShardId::from_collection(coll_name);
            let get_plan =
                PhysicalPlan::Document(crate::bridge::physical_plan::DocumentOp::PointGet {
                    collection: coll_name.clone(),
                    document_id: node_id.clone(),
                    rls_filters: Vec::new(),
                });
            if let Ok(resp) =
                dispatch_utils::dispatch_to_data_plane(state, tenant_id, coll_vshard, get_plan, 0)
                    .await
            {
                let doc_json =
                    crate::data::executor::response_codec::decode_payload_to_json(&resp.payload);
                if let Ok(doc) = sonic_rs::from_str::<serde_json::Value>(&doc_json)
                    && let Some(val) = doc.get(&sum_column)
                {
                    total += json_to_decimal(val);
                    break; // Found in this collection, skip others.
                }
            }
        }
    }

    let schema = Arc::new(vec![text_field("tree_sum")]);
    let mut encoder = DataRowEncoder::new(schema.clone());
    encoder
        .encode_field(&total.to_string())
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(vec![Ok(encoder.take_row())]),
    ))])
}
