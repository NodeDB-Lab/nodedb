//! NodeDB DSL extensions — custom SQL-like commands beyond standard SQL.
//!
//! - SEARCH <collection> USING VECTOR(<field>, ARRAY[...], <k>)
//! - SEARCH <collection> USING VECTOR(...) WITH FILTER <predicate>
//! - SEARCH <collection> USING FUSION(vector=..., graph=..., top_k=...)
//! - CREATE VECTOR INDEX <name> ON <collection> [METRIC cosine|l2] [M <m>] [EF_CONSTRUCTION <ef>]
//! - CREATE FULLTEXT INDEX <name> ON <collection> (<field>)
//! - CRDT MERGE INTO <collection> FROM <source_id> TO <target_id>

use std::sync::Arc;
use std::time::Duration;

use futures::stream;
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response, Tag};
use pgwire::error::PgWireResult;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::types::{sqlstate_error, text_field};

const DSL_DEADLINE: Duration = Duration::from_secs(30);

// ── SEARCH USING VECTOR ─────────────────────────────────────────────

/// SEARCH <collection> USING VECTOR(ARRAY[...], <k>)
/// SEARCH <collection> USING VECTOR(ARRAY[...], <k>) WITH FILTER <field> <op> <value>
pub fn search_vector(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    // Extract collection name.
    let parts: Vec<&str> = sql.split_whitespace().collect();
    if parts.len() < 4 {
        return Err(sqlstate_error(
            "42601",
            "syntax: SEARCH <collection> USING VECTOR(ARRAY[...], <k>)",
        ));
    }
    let collection = parts[1];
    let tenant_id = identity.tenant_id;

    // Parse ARRAY[...] from the SQL.
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

    // Parse top_k: number after the closing bracket.
    let after_array = &sql[array_end + 1..];
    let top_k = after_array
        .split(|c: char| !c.is_ascii_digit())
        .find(|s| !s.is_empty())
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(10);

    // Future: parse WITH FILTER predicates, evaluate against documents, build Roaring bitmap.
    let filter_bitmap: Option<std::sync::Arc<[u8]>> = None;

    let plan = PhysicalPlan::VectorSearch {
        collection: collection.to_string(),
        query_vector: Arc::from(query_vector.as_slice()),
        top_k,
        ef_search: 0,
        filter_bitmap,
        field_name: String::new(),
    };

    let payload =
        super::sync_dispatch::dispatch_sync(state, tenant_id, collection, plan, DSL_DEADLINE)
            .map_err(|e| sqlstate_error("XX000", &e))?;

    let schema = Arc::new(vec![text_field("result")]);
    let text = String::from_utf8_lossy(&payload).into_owned();
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

// ── SEARCH USING FUSION ─────────────────────────────────────────────

/// SEARCH <collection> USING FUSION(VECTOR(ARRAY[...], <k>), GRAPH(<label>, <depth>), TOP <n>)
pub fn search_fusion(
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

    // Parse query vector from ARRAY[...].
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

    // Extract numeric parameters (vector_top_k, expansion_depth, final_top_k).
    let upper = sql.to_uppercase();
    let vector_top_k = extract_param(&upper, "VECTOR_TOP_K").unwrap_or(20);
    let expansion_depth = extract_param(&upper, "DEPTH").unwrap_or(2);
    let final_top_k = extract_param(&upper, "TOP").unwrap_or(10);

    // Extract edge label if specified.
    let edge_label = extract_string_param(sql, "LABEL");

    let plan = PhysicalPlan::GraphRagFusion {
        collection: collection.to_string(),
        query_vector: Arc::from(query_vector.as_slice()),
        vector_top_k,
        edge_label,
        direction: crate::engine::graph::edge_store::Direction::Out,
        expansion_depth,
        final_top_k,
        rrf_k: (60.0, 60.0),
        options: crate::engine::graph::traversal_options::GraphTraversalOptions::default(),
    };

    let payload =
        super::sync_dispatch::dispatch_sync(state, tenant_id, collection, plan, DSL_DEADLINE)
            .map_err(|e| sqlstate_error("XX000", &e))?;

    let schema = Arc::new(vec![text_field("result")]);
    let text = String::from_utf8_lossy(&payload).into_owned();
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

// ── CREATE VECTOR INDEX ─────────────────────────────────────────────

/// CREATE VECTOR INDEX <name> ON <collection> [METRIC cosine|l2|hamming] [M <m>] [EF_CONSTRUCTION <ef>] [DIM <dim>]
pub fn create_vector_index(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    // CREATE VECTOR INDEX <name> ON <collection> [options...]
    if parts.len() < 6 {
        return Err(sqlstate_error(
            "42601",
            "syntax: CREATE VECTOR INDEX <name> ON <collection> [METRIC cosine|l2] [M <m>] [EF_CONSTRUCTION <ef>] [DIM <dim>]",
        ));
    }

    let index_name = parts[3];
    if !parts[4].eq_ignore_ascii_case("ON") {
        return Err(sqlstate_error("42601", "expected ON after index name"));
    }
    let collection = parts[5];
    let tenant_id = identity.tenant_id;

    // Parse optional parameters.
    let upper_parts: Vec<String> = parts.iter().map(|p| p.to_uppercase()).collect();

    let metric = find_param_str(&upper_parts, "METRIC").unwrap_or_else(|| "COSINE".into());
    let m = find_param_usize(&upper_parts, "M").unwrap_or(16);
    let ef_construction = find_param_usize(&upper_parts, "EF_CONSTRUCTION").unwrap_or(200);
    let dim = find_param_usize(&upper_parts, "DIM").unwrap_or(0);

    // Store index metadata in catalog via ownership system.
    let catalog = state.credentials.catalog();
    state
        .permissions
        .set_owner(
            "vector_index",
            tenant_id,
            index_name,
            &identity.username,
            catalog.as_ref(),
        )
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(tenant_id),
        &identity.username,
        &format!(
            "created vector index '{index_name}' on '{collection}' (metric={metric}, m={m}, ef_construction={ef_construction}, dim={dim})"
        ),
    );

    Ok(vec![Response::Execution(Tag::new("CREATE VECTOR INDEX"))])
}

// ── CREATE FULLTEXT INDEX ───────────────────────────────────────────

/// CREATE FULLTEXT INDEX <name> ON <collection> (<field>)
pub fn create_fulltext_index(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    if parts.len() < 7 {
        return Err(sqlstate_error(
            "42601",
            "syntax: CREATE FULLTEXT INDEX <name> ON <collection> (<field>)",
        ));
    }

    let index_name = parts[3];
    if !parts[4].eq_ignore_ascii_case("ON") {
        return Err(sqlstate_error("42601", "expected ON after index name"));
    }
    let collection = parts[5];
    let field = parts[6].trim_matches(|c| c == '(' || c == ')');
    let tenant_id = identity.tenant_id;

    let catalog = state.credentials.catalog();
    state
        .permissions
        .set_owner(
            "fulltext_index",
            tenant_id,
            index_name,
            &identity.username,
            catalog.as_ref(),
        )
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(tenant_id),
        &identity.username,
        &format!("created fulltext index '{index_name}' on '{collection}' ({field})"),
    );

    Ok(vec![Response::Execution(Tag::new("CREATE FULLTEXT INDEX"))])
}

// ── CRDT MERGE INTO ─────────────────────────────────────────────────

/// CRDT MERGE INTO <collection> FROM '<source_id>' TO '<target_id>'
pub fn crdt_merge(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    // CRDT MERGE INTO <collection> FROM '<source>' TO '<target>'
    if parts.len() < 7 {
        return Err(sqlstate_error(
            "42601",
            "syntax: CRDT MERGE INTO <collection> FROM '<source_id>' TO '<target_id>'",
        ));
    }

    let collection = parts[3];
    let tenant_id = identity.tenant_id;

    let from_idx = parts
        .iter()
        .position(|p| p.eq_ignore_ascii_case("FROM"))
        .ok_or_else(|| sqlstate_error("42601", "expected FROM keyword"))?;
    let to_idx = parts
        .iter()
        .position(|p| p.eq_ignore_ascii_case("TO"))
        .ok_or_else(|| sqlstate_error("42601", "expected TO keyword"))?;

    let source_id = parts
        .get(from_idx + 1)
        .map(|s| s.trim_matches('\'').trim_matches('"'))
        .ok_or_else(|| sqlstate_error("42601", "missing source document ID"))?;
    let target_id = parts
        .get(to_idx + 1)
        .map(|s| s.trim_matches('\'').trim_matches('"'))
        .ok_or_else(|| sqlstate_error("42601", "missing target document ID"))?;

    // Read source CRDT state.
    let source_plan = PhysicalPlan::CrdtRead {
        collection: collection.to_string(),
        document_id: source_id.to_string(),
    };

    let source_bytes = super::sync_dispatch::dispatch_sync(
        state,
        tenant_id,
        collection,
        source_plan,
        DSL_DEADLINE,
    )
    .map_err(|e| sqlstate_error("XX000", &e))?;
    if source_bytes.is_empty() {
        return Err(sqlstate_error(
            "02000",
            &format!("source document '{source_id}' not found"),
        ));
    }

    // Apply source state as a delta to target.
    let apply_plan = PhysicalPlan::CrdtApply {
        collection: collection.to_string(),
        document_id: target_id.to_string(),
        delta: source_bytes,
        peer_id: identity.user_id,
        mutation_id: 0,
    };

    super::sync_dispatch::dispatch_sync(state, tenant_id, collection, apply_plan, DSL_DEADLINE)
        .map_err(|e| sqlstate_error("XX000", &e))?;

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(tenant_id),
        &identity.username,
        &format!("CRDT merge: {source_id} → {target_id} in '{collection}'"),
    );

    Ok(vec![Response::Execution(Tag::new("CRDT MERGE"))])
}

// ── Helpers ─────────────────────────────────────────────────────────

fn extract_param(upper: &str, name: &str) -> Option<usize> {
    let idx = upper.find(name)?;
    let rest = &upper[idx + name.len()..];
    rest.split(|c: char| !c.is_ascii_digit())
        .find(|s| !s.is_empty())
        .and_then(|s| s.parse().ok())
}

fn extract_string_param(sql: &str, name: &str) -> Option<String> {
    let upper = sql.to_uppercase();
    let idx = upper.find(name)?;
    let rest = &sql[idx + name.len()..];
    let rest = rest.trim();
    if rest.starts_with('\'') || rest.starts_with('"') {
        let quote = rest.chars().next()?;
        let end = rest[1..].find(quote)?;
        Some(rest[1..end + 1].to_string())
    } else {
        rest.split_whitespace().next().map(|s| s.to_string())
    }
}

fn find_param_str(upper_parts: &[String], name: &str) -> Option<String> {
    let idx = upper_parts.iter().position(|p| p == name)?;
    upper_parts.get(idx + 1).cloned()
}

fn find_param_usize(upper_parts: &[String], name: &str) -> Option<usize> {
    let idx = upper_parts.iter().position(|p| p == name)?;
    upper_parts
        .get(idx + 1)
        .and_then(|s| s.parse::<usize>().ok())
}
