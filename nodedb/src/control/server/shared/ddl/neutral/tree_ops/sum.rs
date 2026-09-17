// SPDX-License-Identifier: BUSL-1.1

//! `SELECT TREE_SUM(column, graph_index, root_id [, collection]) [MAX_DEPTH n]`
//!
//! BFS traversal from `root_id`, summing column values over all descendants
//! plus root. If `collection` is provided, lookups are O(N). Without it,
//! lookups scan all tenant collections (O(N×C)) — pass the collection
//! name for production use.

use serde_json::{Map, Value as JsonValue};

use crate::bridge::envelope::PhysicalPlan;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::response_shape::types::ShapedRows;
use crate::control::server::shared::ddl::sql_parse::parse_ident_token;
use crate::control::state::SharedState;
use crate::engine::graph::traversal_options::GraphTraversalOptions;
use crate::types::{DatabaseId, TraceId, VShardId};

use super::super::super::result::{DdlError, DdlResult};
use super::super::read_gate::CollectionReadGate;
use super::parse::{extract_function_args, extract_number_after, json_to_decimal};
use super::support::ddl_err;

pub async fn tree_sum(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    sql: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    let tenant_id = identity.tenant_id;
    let upper = sql.to_uppercase();

    // Parse: TREE_SUM(<column>, <graph_index>, '<root_id>' [, '<collection>'])
    let args = extract_function_args(&upper, sql, "TREE_SUM")?;
    if args.len() < 3 {
        return Err(ddl_err(
            "42601",
            "TREE_SUM requires (column, graph_index, root_id [, collection])",
        ));
    }
    // The first two arguments are bare identifiers, not string literals.
    let sum_column = parse_ident_token(args[0].trim())?;
    let graph_index = parse_ident_token(args[1].trim())?;
    let root_id = args[2]
        .trim()
        .trim_matches('\'')
        .trim_matches('"')
        .to_string();
    // A string literal is data: the collection name resolves exactly as
    // written, with no case folding.
    let explicit_collection = args
        .get(3)
        .map(|s| s.trim().trim_matches('\'').trim_matches('"').to_string());

    let max_depth = extract_number_after(&upper, "MAX_DEPTH")?.unwrap_or(100);

    // The set of collections this sum reads is resolved, and authorized, before
    // any traversal runs. With the optional 4th argument it is the one
    // collection named; without it the sum genuinely point-gets from every
    // collection in the tenant, so every one of them is a read the caller must
    // hold a grant for and the first denial ends the statement. Narrowing the
    // sum to the subset a caller happens to be allowed would silently return a
    // smaller total reported as the whole one.
    let gate = CollectionReadGate::for_request(state, identity, database_id);
    let collections_to_search: Vec<String> = if let Some(ref coll) = explicit_collection {
        vec![coll.clone()]
    } else {
        state
            .credentials
            .catalog()
            .load_collections_for_tenant(database_id, tenant_id.as_u64())
            .unwrap_or_default()
            .iter()
            .map(|c| c.name.clone())
            .collect()
    };
    for coll_name in &collections_to_search {
        gate.authorize(coll_name)?;
        // The total is arithmetic over `sum_column`, so a redaction rule on it
        // has no honest answer: masking the total would report a figure no tree
        // sums to.
        gate.refuse_if_field_redacted(coll_name, &sum_column, "the tree sum")?;
    }

    // BFS traversal to get all descendant node IDs.
    let dir = crate::engine::graph::edge_store::Direction::Out;
    let bfs_result = crate::control::server::graph_dispatch::cross_core_bfs_with_options(
        state,
        crate::control::server::graph_dispatch::CrossCoreBfsParams {
            tenant_id,
            // Tree-index BFS walks edges by index label; no catalog record maps
            // an index name back to the collection it was built on.
            collection: None,
            database_id,
            start_nodes: vec![root_id.clone()],
            edge_label: Some(graph_index),
            direction: dir,
            max_depth,
            options: &GraphTraversalOptions::default(),
        },
    )
    .await
    .map_err(|e| ddl_err("XX000", format!("BFS failed: {e}")))?;

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
    for node_id in &all_ids {
        for coll_name in &collections_to_search {
            let coll_vshard = VShardId::from_collection_in_database(database_id, coll_name);
            let pk_bytes = node_id.as_bytes().to_vec();
            let surrogate = state
                .surrogate_assigner
                .lookup(database_id, tenant_id, coll_name, &pk_bytes)
                .map_err(|e| ddl_err("XX000", format!("surrogate lookup: {e}")))?
                .unwrap_or(nodedb_types::Surrogate::ZERO);
            let mut get_plan =
                PhysicalPlan::Document(nodedb_physical::physical_plan::DocumentOp::PointGet {
                    collection: nodedb_types::QualifiedCollection::new(database_id, coll_name),
                    document_id: node_id.clone(),
                    surrogate,
                    pk_bytes,
                    rls_filters: Vec::new(),
                    system_time: nodedb_types::SystemTimeScope::Current,
                    valid_at_ms: None,
                });
            gate.inject_rls(&mut get_plan)?;
            if let Ok(resp) = crate::control::server::dispatch_utils::dispatch_to_data_plane(
                state,
                tenant_id,
                database_id,
                coll_vshard,
                get_plan,
                TraceId::ZERO,
            )
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

    let mut row = Map::new();
    row.insert("tree_sum".to_string(), JsonValue::String(total.to_string()));

    Ok(vec![DdlResult::Rows(ShapedRows::text_rows(
        vec!["tree_sum".to_string()],
        vec![row],
    ))])
}
