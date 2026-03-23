//! Graph DSL commands: GRAPH INSERT EDGE, GRAPH DELETE EDGE,
//! GRAPH TRAVERSE, GRAPH NEIGHBORS, GRAPH PATH.

use std::sync::Arc;

use futures::stream;
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response, Tag};
use pgwire::error::PgWireResult;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::dispatch_utils;
use crate::control::state::SharedState;
use crate::data::executor::response_codec;
use crate::types::VShardId;

use super::super::types::{sqlstate_error, text_field};

/// GRAPH INSERT EDGE FROM '<src>' TO '<dst>' TYPE '<label>' [ON <collection>] [PROPERTIES '<json>']
pub async fn insert_edge(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    _parts: &[&str],
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    // Parse: GRAPH INSERT EDGE FROM 'src' TO 'dst' TYPE 'label' ...
    let upper = sql.to_uppercase();
    let src = extract_quoted_after(&upper, sql, "FROM")
        .ok_or_else(|| sqlstate_error("42601", "missing FROM '<node_id>'"))?;
    let dst = extract_quoted_after(&upper, sql, "TO")
        .ok_or_else(|| sqlstate_error("42601", "missing TO '<node_id>'"))?;
    let label = extract_quoted_after(&upper, sql, "TYPE")
        .ok_or_else(|| sqlstate_error("42601", "missing TYPE '<edge_label>'"))?;
    let properties = extract_quoted_after(&upper, sql, "PROPERTIES").unwrap_or_default();

    let tenant_id = identity.tenant_id;
    let vshard_id = VShardId::from_key(src.as_bytes());

    let plan = PhysicalPlan::EdgePut {
        src_id: src,
        label: label.clone(),
        dst_id: dst,
        properties: properties.into_bytes(),
    };

    dispatch_utils::wal_append_if_write(&state.wal, tenant_id, vshard_id, &plan)
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;

    match dispatch_utils::dispatch_to_data_plane(state, tenant_id, vshard_id, plan, 0).await {
        Ok(_) => Ok(vec![Response::Execution(Tag::new("INSERT EDGE"))]),
        Err(e) => Err(sqlstate_error("XX000", &e.to_string())),
    }
}

/// GRAPH DELETE EDGE FROM '<src>' TO '<dst>' TYPE '<label>'
pub async fn delete_edge(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    _parts: &[&str],
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    let upper = sql.to_uppercase();
    let src = extract_quoted_after(&upper, sql, "FROM")
        .ok_or_else(|| sqlstate_error("42601", "missing FROM '<node_id>'"))?;
    let dst = extract_quoted_after(&upper, sql, "TO")
        .ok_or_else(|| sqlstate_error("42601", "missing TO '<node_id>'"))?;
    let label = extract_quoted_after(&upper, sql, "TYPE")
        .ok_or_else(|| sqlstate_error("42601", "missing TYPE '<edge_label>'"))?;

    let tenant_id = identity.tenant_id;
    let vshard_id = VShardId::from_key(src.as_bytes());

    let plan = PhysicalPlan::EdgeDelete {
        src_id: src,
        label,
        dst_id: dst,
    };

    dispatch_utils::wal_append_if_write(&state.wal, tenant_id, vshard_id, &plan)
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;

    match dispatch_utils::dispatch_to_data_plane(state, tenant_id, vshard_id, plan, 0).await {
        Ok(_) => Ok(vec![Response::Execution(Tag::new("DELETE EDGE"))]),
        Err(e) => Err(sqlstate_error("XX000", &e.to_string())),
    }
}

/// GRAPH TRAVERSE FROM '<node_id>' [DEPTH <n>] [LABEL '<label>'] [DIRECTION in|out|both]
pub async fn traverse(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    _parts: &[&str],
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    let upper = sql.to_uppercase();
    let start = extract_quoted_after(&upper, sql, "FROM")
        .ok_or_else(|| sqlstate_error("42601", "missing FROM '<node_id>'"))?;
    let depth = extract_number_after(&upper, "DEPTH").unwrap_or(2);
    let label = extract_quoted_after(&upper, sql, "LABEL");
    let direction = extract_word_after(&upper, "DIRECTION").unwrap_or("out".into());

    let dir = match direction.as_str() {
        "IN" | "in" => crate::engine::graph::edge_store::Direction::In,
        "BOTH" | "both" => crate::engine::graph::edge_store::Direction::Both,
        _ => crate::engine::graph::edge_store::Direction::Out,
    };

    let tenant_id = identity.tenant_id;
    let vshard_id = VShardId::from_key(start.as_bytes());

    let plan = PhysicalPlan::GraphHop {
        start_nodes: vec![start],
        edge_label: label,
        direction: dir,
        depth,
        options: Default::default(),
    };

    match dispatch_utils::dispatch_to_data_plane(state, tenant_id, vshard_id, plan, 0).await {
        Ok(resp) => payload_to_query_response(&resp.payload),
        Err(e) => Err(sqlstate_error("XX000", &e.to_string())),
    }
}

/// GRAPH NEIGHBORS OF '<node_id>' [LABEL '<label>'] [DIRECTION in|out|both]
pub async fn neighbors(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    _parts: &[&str],
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    let upper = sql.to_uppercase();
    let node = extract_quoted_after(&upper, sql, "OF")
        .ok_or_else(|| sqlstate_error("42601", "missing OF '<node_id>'"))?;
    let label = extract_quoted_after(&upper, sql, "LABEL");
    let direction = extract_word_after(&upper, "DIRECTION").unwrap_or("out".into());

    let dir = match direction.as_str() {
        "IN" | "in" => crate::engine::graph::edge_store::Direction::In,
        "BOTH" | "both" => crate::engine::graph::edge_store::Direction::Both,
        _ => crate::engine::graph::edge_store::Direction::Out,
    };

    let tenant_id = identity.tenant_id;
    let vshard_id = VShardId::from_key(node.as_bytes());

    let plan = PhysicalPlan::GraphNeighbors {
        node_id: node,
        edge_label: label,
        direction: dir,
    };

    match dispatch_utils::dispatch_to_data_plane(state, tenant_id, vshard_id, plan, 0).await {
        Ok(resp) => payload_to_query_response(&resp.payload),
        Err(e) => Err(sqlstate_error("XX000", &e.to_string())),
    }
}

/// GRAPH PATH FROM '<src>' TO '<dst>' [MAX_DEPTH <n>] [LABEL '<label>']
pub async fn shortest_path(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    _parts: &[&str],
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    let upper = sql.to_uppercase();
    let src = extract_quoted_after(&upper, sql, "FROM")
        .ok_or_else(|| sqlstate_error("42601", "missing FROM '<node_id>'"))?;
    let dst = extract_quoted_after(&upper, sql, "TO")
        .ok_or_else(|| sqlstate_error("42601", "missing TO '<node_id>'"))?;
    let max_depth = extract_number_after(&upper, "MAX_DEPTH").unwrap_or(10);
    let label = extract_quoted_after(&upper, sql, "LABEL");

    let tenant_id = identity.tenant_id;
    let vshard_id = VShardId::from_key(src.as_bytes());

    let plan = PhysicalPlan::GraphPath {
        src,
        dst,
        max_depth,
        edge_label: label,
        options: Default::default(),
    };

    match dispatch_utils::dispatch_to_data_plane(state, tenant_id, vshard_id, plan, 0).await {
        Ok(resp) => payload_to_query_response(&resp.payload),
        Err(e) => Err(sqlstate_error("XX000", &e.to_string())),
    }
}

// ─── Helpers ───────────────────────────────────────────────────────

/// Convert a Data Plane payload to a pgwire QueryResponse.
fn payload_to_query_response(
    payload: &crate::bridge::envelope::Payload,
) -> PgWireResult<Vec<Response>> {
    if payload.is_empty() {
        let schema = Arc::new(vec![text_field("result")]);
        return Ok(vec![Response::Query(QueryResponse::new(
            schema,
            stream::empty(),
        ))]);
    }

    let json_text = response_codec::decode_payload_to_json(payload);
    let schema = Arc::new(vec![text_field("result")]);
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

/// Extract a single-quoted value after a keyword.
/// e.g., `FROM 'alice'` → `Some("alice")`
fn extract_quoted_after(upper: &str, original: &str, keyword: &str) -> Option<String> {
    let kw_pos = upper.find(keyword)?;
    let after = &original[kw_pos + keyword.len()..];
    let trimmed = after.trim_start();
    if let Some(content) = trimmed.strip_prefix('\'') {
        let end = content.find('\'')?;
        Some(content[..end].to_string())
    } else {
        // Unquoted word.
        let word = trimmed.split_whitespace().next()?;
        Some(word.to_string())
    }
}

/// Extract a number after a keyword. e.g., `DEPTH 3` → `Some(3)`
fn extract_number_after(upper: &str, keyword: &str) -> Option<usize> {
    let kw_pos = upper.find(keyword)?;
    let after = &upper[kw_pos + keyword.len()..];
    let word = after.split_whitespace().next()?;
    word.parse().ok()
}

/// Extract an unquoted word after a keyword. e.g., `DIRECTION out` → `Some("out")`
fn extract_word_after(upper: &str, keyword: &str) -> Option<String> {
    let kw_pos = upper.find(keyword)?;
    let after = &upper[kw_pos + keyword.len()..];
    let word = after.split_whitespace().next()?;
    Some(word.to_string())
}
