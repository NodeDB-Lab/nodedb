//! Tree operations: CREATE GRAPH INDEX, TREE_SUM, TREE_CHILDREN.
//!
//! These build on the existing CSR graph engine for hierarchical aggregation
//! over self-referential collections (e.g. chart of accounts with parent_id).
//!
//! Syntax:
//! ```sql
//! CREATE GRAPH INDEX account_tree ON accounts (parent_id -> id);
//! SELECT TREE_SUM(balance, account_tree, 'assets');
//! SELECT TREE_CHILDREN(account_tree, 'expenses');
//! ```

use std::sync::Arc;

use futures::stream;
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response};
use pgwire::error::PgWireResult;
use sonic_rs;

use crate::bridge::envelope::PhysicalPlan;
use crate::bridge::physical_plan::GraphOp;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::dispatch_utils;
use crate::control::state::SharedState;
use crate::types::VShardId;

use super::super::types::{sqlstate_error, text_field};

/// CREATE GRAPH INDEX name ON collection (parent_col -> id_col)
///
/// Scans all documents in the collection, extracts parent→child edges,
/// and adds them to the CSR via GraphOp::EdgePut. The edge label is the
/// graph index name, enabling label-filtered traversal.
pub async fn create_graph_index(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    let tenant_id = identity.tenant_id;
    let parts: Vec<&str> = sql.split_whitespace().collect();

    // CREATE GRAPH INDEX <name> ON <collection> (<parent_col> -> <id_col>)
    let index_name = parts
        .get(3)
        .ok_or_else(|| sqlstate_error("42601", "missing graph index name"))?
        .to_lowercase();

    let on_idx = parts
        .iter()
        .position(|p| p.eq_ignore_ascii_case("ON"))
        .ok_or_else(|| sqlstate_error("42601", "CREATE GRAPH INDEX requires ON <collection>"))?;

    let collection = parts
        .get(on_idx + 1)
        .ok_or_else(|| sqlstate_error("42601", "missing collection name after ON"))?
        .to_lowercase();

    // Parse (parent_col -> id_col) from the parenthesized section.
    let (parent_col, id_col) = parse_edge_columns(sql)?;

    // Scan all documents in the collection and build edges.
    let Some(catalog) = state.credentials.catalog() else {
        return Err(sqlstate_error("XX000", "no catalog available"));
    };

    // Verify collection exists.
    if catalog
        .get_collection(tenant_id.as_u32(), &collection)
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?
        .is_none()
    {
        return Err(sqlstate_error(
            "42P01",
            &format!("collection '{collection}' not found"),
        ));
    }

    // Dispatch a scan to get all documents from this collection.
    let vshard = VShardId::from_collection(&collection);
    let scan_plan = PhysicalPlan::Document(crate::bridge::physical_plan::DocumentOp::Scan {
        collection: collection.clone(),
        limit: usize::MAX,
        offset: 0,
        sort_keys: Vec::new(),
        filters: Vec::new(),
        distinct: false,
        projection: Vec::new(),
        computed_columns: Vec::new(),
        window_functions: Vec::new(),
    });

    let scan_resp = dispatch_utils::dispatch_to_data_plane(state, tenant_id, vshard, scan_plan, 0)
        .await
        .map_err(|e| sqlstate_error("XX000", &format!("scan failed: {e}")))?;

    // Parse the scan payload as a JSON array of documents.
    let payload_json =
        crate::data::executor::response_codec::decode_payload_to_json(&scan_resp.payload);
    let docs: Vec<serde_json::Value> = sonic_rs::from_str(&payload_json)
        .map_err(|e| sqlstate_error("22P02", &format!("invalid JSON in scan response: {e}")))?;

    // For each document with a parent_id, add an edge: parent_id → doc_id.
    let mut edge_count = 0u64;
    for doc in &docs {
        let obj = match doc.as_object() {
            Some(o) => o,
            None => continue,
        };

        let doc_id = obj
            .get("id")
            .or_else(|| obj.get("_id"))
            .and_then(|v| v.as_str());
        // Also try the id_col field if different from "id".
        let doc_id = doc_id.or_else(|| obj.get(&id_col).and_then(|v| v.as_str()));

        let parent_id = obj.get(&parent_col).and_then(|v| v.as_str());

        if let (Some(child), Some(parent)) = (doc_id, parent_id) {
            if parent.is_empty() || parent == child {
                continue; // Skip root nodes and self-references.
            }
            // Add edge: parent → child with label = index_name.
            let edge_plan = PhysicalPlan::Graph(GraphOp::EdgePut {
                src_id: parent.to_string(),
                label: index_name.clone(),
                dst_id: child.to_string(),
                properties: Vec::new(),
            });
            let edge_vshard = VShardId::from_collection(&collection);
            match dispatch_utils::dispatch_to_data_plane(
                state,
                tenant_id,
                edge_vshard,
                edge_plan,
                0,
            )
            .await
            {
                Ok(_) => edge_count += 1,
                Err(e) => {
                    tracing::warn!(
                        parent = parent,
                        child = child,
                        index = %index_name,
                        error = %e,
                        "failed to insert graph edge"
                    );
                }
            }
        }
    }

    state.schema_version.bump();

    // Return result as a query with edge count.
    let schema = Arc::new(vec![text_field("edges_created")]);
    let mut encoder = DataRowEncoder::new(schema.clone());
    encoder
        .encode_field(&edge_count.to_string())
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(vec![Ok(encoder.take_row())]),
    ))])
}

/// SELECT TREE_SUM(column, graph_index, root_id [, collection]) [MAX_DEPTH n]
///
/// BFS traversal from root_id, summing column values over all descendants + root.
/// If `collection` is provided, lookups are O(N). Without it, lookups scan all
/// tenant collections (O(N×C)) — pass the collection name for production use.
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

/// SELECT TREE_CHILDREN(graph_index, root_id)
///
/// BFS traversal from root_id, returns all descendant IDs.
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

// ── Parsing helpers ──

/// Parse `(parent_col -> id_col)` from CREATE GRAPH INDEX DDL.
fn parse_edge_columns(sql: &str) -> PgWireResult<(String, String)> {
    let paren_start = sql
        .find('(')
        .ok_or_else(|| sqlstate_error("42601", "missing (parent_col -> id_col)"))?;
    let paren_end = sql
        .rfind(')')
        .ok_or_else(|| sqlstate_error("42601", "missing closing ')'"))?;
    let inner = &sql[paren_start + 1..paren_end];

    // Split on -> or →
    let (parent, child) = if let Some(pos) = inner.find("->") {
        (&inner[..pos], &inner[pos + 2..])
    } else if let Some(pos) = inner.find('→') {
        (&inner[..pos], &inner[pos + '→'.len_utf8()..])
    } else {
        return Err(sqlstate_error(
            "42601",
            "edge definition requires '->' or '→' between columns",
        ));
    };

    let parent = parent.trim().to_lowercase();
    let child = child.trim().to_lowercase();

    if parent.is_empty() || child.is_empty() {
        return Err(sqlstate_error(
            "42601",
            "both parent and child columns required in (parent -> child)",
        ));
    }

    Ok((parent, child))
}

/// Extract function arguments from `FUNC_NAME(arg1, arg2, arg3)`.
fn extract_function_args<'a>(
    upper: &str,
    original: &'a str,
    func_name: &str,
) -> PgWireResult<Vec<&'a str>> {
    let pos = upper
        .find(func_name)
        .ok_or_else(|| sqlstate_error("42601", &format!("missing {func_name}")))?;
    let after = &original[pos + func_name.len()..];
    let paren_start = after
        .find('(')
        .ok_or_else(|| sqlstate_error("42601", &format!("{func_name} requires (...) arguments")))?;
    let paren_end = after
        .find(')')
        .ok_or_else(|| sqlstate_error("42601", "missing closing ')'"))?;
    let inner = &after[paren_start + 1..paren_end];
    Ok(inner.split(',').collect())
}

/// Largest accepted value for tree MAX_DEPTH. Prevents a single
/// statement from saturating `cross_core_bfs` with an unbounded
/// fan-out per hop.
const TREE_MAX_DEPTH_CAP: usize = 1024;

/// Extract a number after a keyword and clamp to `TREE_MAX_DEPTH_CAP`.
///
/// Returns `Ok(None)` when the keyword is absent; the caller supplies
/// its own default. Rejects out-of-range values with SQLSTATE 22023
/// instead of forwarding them to the BFS loop.
fn extract_number_after(upper: &str, keyword: &str) -> pgwire::error::PgWireResult<Option<usize>> {
    let Some(pos) = upper.find(keyword) else {
        return Ok(None);
    };
    let after = upper[pos + keyword.len()..].trim_start();
    let Some(tok) = after.split_whitespace().next() else {
        return Ok(None);
    };
    let v: usize = tok.parse().map_err(|_| {
        super::super::types::sqlstate_error(
            "22023",
            &format!("{keyword} must be a non-negative integer"),
        )
    })?;
    if v > TREE_MAX_DEPTH_CAP {
        return Err(super::super::types::sqlstate_error(
            "22023",
            &format!("{keyword} {v} exceeds maximum allowed value {TREE_MAX_DEPTH_CAP}"),
        ));
    }
    Ok(Some(v))
}

/// Convert a JSON value to Decimal for summation.
///
/// Non-numeric values (null, objects, arrays, unparseable strings, NaN, Infinity)
/// are treated as zero — TREE_SUM skips them silently.
fn json_to_decimal(v: &serde_json::Value) -> rust_decimal::Decimal {
    if let Some(i) = v.as_i64() {
        rust_decimal::Decimal::from(i)
    } else if let Some(f) = v.as_f64() {
        rust_decimal::Decimal::try_from(f).unwrap_or(rust_decimal::Decimal::ZERO)
    } else if let Some(s) = v.as_str() {
        s.parse().unwrap_or(rust_decimal::Decimal::ZERO)
    } else {
        rust_decimal::Decimal::ZERO
    }
}
