// SPDX-License-Identifier: BUSL-1.1

//! `SELECT TREE_CHILDREN(graph_index, root_id)`
//!
//! BFS traversal from `root_id`, returns all descendant IDs.

use serde_json::{Map, Value as JsonValue};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::response_shape::types::ShapedRows;
use crate::control::state::SharedState;
use crate::engine::graph::traversal_options::GraphTraversalOptions;
use crate::types::DatabaseId;

use super::super::super::result::{DdlError, DdlResult};
use super::super::refuse_gate::RefusingReadGate;
use super::parse::{extract_function_args, extract_number_after};
use super::support::ddl_err;

/// What the walk delivers instead of row bodies, for the refusal message.
const CHILDREN_WHAT: &str =
    "TREE_CHILDREN(), which returns node ids reached by the walk rather than rows";

pub async fn tree_children(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    sql: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    let tenant_id = identity.tenant_id;
    let upper = sql.to_uppercase();

    let args = extract_function_args(&upper, sql, "TREE_CHILDREN")?;
    if args.len() < 2 {
        return Err(ddl_err(
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

    // The walk names an edge label, not a collection, and the same label can
    // be written on edges of any collection in the tenant — so the set of
    // collections whose node ids can come back is the whole tenant, and that
    // is the set the caller must be granted. The first denial ends the
    // statement rather than returning the subset the caller happens to be
    // allowed, which would report a partial descendant set as the whole one.
    // The RLS half asks the same tenant-wide question: the reply is node ids,
    // which carry no row filter, so a read policy anywhere on this identity
    // cannot be honored through this shape.
    let gate = RefusingReadGate::for_request(state, identity, database_id);
    gate.authorize_every_collection()?;
    gate.refuse_if_any_read_policy(CHILDREN_WHAT)?;

    let dir = crate::engine::graph::edge_store::Direction::Out;
    let bfs_result = crate::control::server::graph_dispatch::cross_core_bfs_with_options(
        state,
        crate::control::server::graph_dispatch::CrossCoreBfsParams {
            tenant_id,
            // Tree-index BFS walks edges by index label; no catalog record maps
            // an index name back to the collection it was built on.
            collection: None,
            database_id,
            start_nodes: vec![root_id],
            edge_label: Some(graph_index),
            direction: dir,
            max_depth,
            options: &GraphTraversalOptions::default(),
        },
    )
    .await
    .map_err(|e| ddl_err("XX000", format!("BFS failed: {e}")))?;

    let bfs_json =
        crate::data::executor::response_codec::decode_payload_to_json(&bfs_result.payload);
    let node_ids: Vec<String> = sonic_rs::from_str::<Vec<serde_json::Value>>(&bfs_json)
        .unwrap_or_default()
        .into_iter()
        .filter_map(|v| v.as_str().map(String::from))
        .collect();

    let mut rows: Vec<Map<String, JsonValue>> = Vec::with_capacity(node_ids.len());
    for id in &node_ids {
        if id.is_empty() {
            continue;
        }
        let mut row = Map::new();
        row.insert("child_id".to_string(), JsonValue::String(id.to_string()));
        rows.push(row);
    }

    Ok(vec![DdlResult::Rows(ShapedRows::text_rows(
        vec!["child_id".to_string()],
        rows,
    ))])
}
