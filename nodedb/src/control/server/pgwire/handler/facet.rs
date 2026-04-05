//! FACET_COUNTS and SEARCH_WITH_FACETS SQL interception.
//!
//! These are intercepted before DataFusion planning because they use a custom
//! SQL syntax that DataFusion cannot parse. The Control Plane parses the
//! arguments, builds a `QueryOp::FacetCounts` physical plan, dispatches to
//! the Data Plane, and formats the response.

use pgwire::api::results::Response;
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use crate::bridge::envelope::PhysicalPlan;
use crate::bridge::physical_plan::QueryOp;
use crate::control::planner::physical::PhysicalTask;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::types::VShardId;

use super::core::NodeDbPgHandler;
use super::plan::{PlanKind, payload_to_response};

/// Execute `SELECT FACET_COUNTS(collection => '...', filter => '...', fields => [...])`.
pub(super) async fn execute_facet_counts_sql(
    handler: &NodeDbPgHandler,
    identity: &AuthenticatedIdentity,
    _addr: &std::net::SocketAddr,
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    let parsed = parse_facet_counts_args(sql)?;

    let tenant_id = identity.tenant_id;
    let vshard = VShardId::from_collection(&parsed.collection);

    // Convert filter text to ScanFilter predicates.
    let filter_bytes = if parsed.filter.is_empty() {
        Vec::new()
    } else {
        build_filter_bytes(&parsed.filter)?
    };

    let task = PhysicalTask {
        tenant_id,
        vshard_id: vshard,
        plan: PhysicalPlan::Query(QueryOp::FacetCounts {
            collection: parsed.collection,
            filters: filter_bytes,
            fields: parsed.fields,
            limit_per_facet: parsed.limit_per_facet,
        }),
    };

    let resp = handler.dispatch_task(task).await.map_err(|e| {
        PgWireError::UserError(Box::new(ErrorInfo::new(
            "ERROR".to_owned(),
            "XX000".to_owned(),
            e.to_string(),
        )))
    })?;

    Ok(vec![payload_to_response(&resp.payload, PlanKind::MultiRow)])
}

/// Execute `SELECT SEARCH_WITH_FACETS(query => '...', facets => [...])`.
///
/// Runs the main search query via DataFusion, then runs a FacetCounts query
/// using the same WHERE predicate. Assembles `{ results: [...], facets: {...} }`.
pub(super) async fn execute_search_with_facets_sql(
    handler: &NodeDbPgHandler,
    identity: &AuthenticatedIdentity,
    addr: &std::net::SocketAddr,
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    let parsed = parse_search_with_facets_args(sql)?;

    // Step 1: Execute the main search query via the standard DataFusion path.
    let search_results = handler
        .execute_query_for_cursor(addr, &parsed.query, identity)
        .await?;

    // Step 2: Extract collection and filter from the query for facet counting.
    // Parse the inner query's FROM and WHERE clauses.
    let (collection, filter_text) = extract_collection_and_filter(&parsed.query)?;

    let tenant_id = identity.tenant_id;
    let vshard = VShardId::from_collection(&collection);

    let filter_bytes = if filter_text.is_empty() {
        Vec::new()
    } else {
        build_filter_bytes(&filter_text)?
    };

    let facet_task = PhysicalTask {
        tenant_id,
        vshard_id: vshard,
        plan: PhysicalPlan::Query(QueryOp::FacetCounts {
            collection,
            filters: filter_bytes,
            fields: parsed.facets,
            limit_per_facet: 0, // All values.
        }),
    };

    let facet_resp = handler.dispatch_task(facet_task).await.map_err(|e| {
        PgWireError::UserError(Box::new(ErrorInfo::new(
            "ERROR".to_owned(),
            "XX000".to_owned(),
            e.to_string(),
        )))
    })?;

    // Step 3: Assemble combined response.
    let results_json: Vec<serde_json::Value> = search_results
        .iter()
        .filter_map(|s| serde_json::from_str(s).ok())
        .collect();

    let facets_json: serde_json::Value = if facet_resp.payload.is_empty() {
        serde_json::json!({})
    } else {
        let text =
            crate::data::executor::response_codec::decode_payload_to_json(&facet_resp.payload);
        serde_json::from_str(&text).unwrap_or(serde_json::json!({}))
    };

    let combined = serde_json::json!({
        "results": results_json,
        "facets": facets_json,
    });

    let payload = serde_json::to_vec(&combined).unwrap_or_default();
    Ok(vec![payload_to_response(&payload, PlanKind::MultiRow)])
}

// ── Parsing helpers ───────────────────────────────────────────────────

struct FacetCountsArgs {
    collection: String,
    filter: String,
    fields: Vec<String>,
    limit_per_facet: usize,
}

struct SearchWithFacetsArgs {
    query: String,
    facets: Vec<String>,
}

/// Parse `SELECT FACET_COUNTS(collection => 'name', filter => 'pred', fields => ['a','b'])`.
fn parse_facet_counts_args(sql: &str) -> PgWireResult<FacetCountsArgs> {
    let collection = extract_named_string_arg(sql, "collection")
        .ok_or_else(|| syntax_error("FACET_COUNTS requires collection => 'name' argument"))?;

    let filter = extract_named_string_arg(sql, "filter").unwrap_or_default();

    let fields = extract_named_array_arg(sql, "fields").ok_or_else(|| {
        syntax_error("FACET_COUNTS requires fields => ['field1', 'field2'] argument")
    })?;

    let limit_per_facet = extract_named_int_arg(sql, "limit").unwrap_or(0);

    Ok(FacetCountsArgs {
        collection,
        filter,
        fields,
        limit_per_facet,
    })
}

/// Parse `SELECT SEARCH_WITH_FACETS(query => '...', facets => ['a','b'])`.
fn parse_search_with_facets_args(sql: &str) -> PgWireResult<SearchWithFacetsArgs> {
    let query = extract_named_string_arg(sql, "query").ok_or_else(|| {
        syntax_error("SEARCH_WITH_FACETS requires query => 'SELECT ...' argument")
    })?;

    let facets = extract_named_array_arg(sql, "facets").ok_or_else(|| {
        syntax_error("SEARCH_WITH_FACETS requires facets => ['field1', 'field2'] argument")
    })?;

    Ok(SearchWithFacetsArgs { query, facets })
}

/// Extract a named string argument: `name => 'value'` or `name => ''value with quotes''`.
fn extract_named_string_arg(sql: &str, name: &str) -> Option<String> {
    let lower = sql.to_lowercase();
    let pattern = format!("{name} =>");
    let pos = lower.find(&pattern)?;
    let after = sql[pos + pattern.len()..].trim_start();

    // Handle both single-quoted (with SQL escaping) values.
    if after.starts_with('\'') {
        // Find matching close quote (handle '' escapes).
        let bytes = after.as_bytes();
        let mut i = 1;
        let mut result = String::new();
        while i < bytes.len() {
            if bytes[i] == b'\'' {
                if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                    result.push('\'');
                    i += 2;
                    continue;
                }
                return Some(result);
            }
            result.push(bytes[i] as char);
            i += 1;
        }
    }
    None
}

/// Extract a named array argument: `name => ['val1', 'val2']`.
fn extract_named_array_arg(sql: &str, name: &str) -> Option<Vec<String>> {
    let lower = sql.to_lowercase();
    let pattern = format!("{name} =>");
    let pos = lower.find(&pattern)?;
    let after = sql[pos + pattern.len()..].trim_start();

    let open = after.find('[')?;
    let close = after.find(']')?;
    if close <= open {
        return None;
    }

    let inner = &after[open + 1..close];
    let items: Vec<String> = inner
        .split(',')
        .map(|s| s.trim().trim_matches('\'').trim_matches('"').to_string())
        .filter(|s| !s.is_empty())
        .collect();

    if items.is_empty() { None } else { Some(items) }
}

/// Extract a named integer argument: `name => 10`.
fn extract_named_int_arg(sql: &str, name: &str) -> Option<usize> {
    let lower = sql.to_lowercase();
    let pattern = format!("{name} =>");
    let pos = lower.find(&pattern)?;
    let after = sql[pos + pattern.len()..].trim_start();
    // Take digits until non-digit.
    let num_str: String = after.chars().take_while(|c| c.is_ascii_digit()).collect();
    num_str.parse().ok()
}

/// Extract collection name and WHERE clause from a SELECT query.
fn extract_collection_and_filter(query: &str) -> PgWireResult<(String, String)> {
    let upper = query.to_uppercase();

    let from_pos = upper
        .find(" FROM ")
        .ok_or_else(|| syntax_error("SEARCH_WITH_FACETS query must contain FROM clause"))?;

    let after_from = query[from_pos + 6..].trim_start();
    let collection = after_from
        .split_whitespace()
        .next()
        .unwrap_or("")
        .to_lowercase()
        .trim_end_matches(|c: char| !c.is_alphanumeric() && c != '_')
        .to_string();

    let filter = if let Some(where_pos) = upper.find(" WHERE ") {
        // Extract everything between WHERE and ORDER BY / LIMIT / end.
        let after_where = &query[where_pos + 7..];
        let end = ["ORDER BY", "LIMIT", "GROUP BY"]
            .iter()
            .filter_map(|kw| after_where.to_uppercase().find(kw))
            .min()
            .unwrap_or(after_where.len());
        after_where[..end].trim().to_string()
    } else {
        String::new()
    };

    Ok((collection, filter))
}

/// Build ScanFilter bytes from a filter text predicate.
///
/// Parses simple predicates like `field = 'value' AND field2 > 10`.
/// For complex predicates, returns an empty filter (matches all).
fn build_filter_bytes(filter_text: &str) -> PgWireResult<Vec<u8>> {
    let filters = crate::bridge::scan_filter::parse_simple_predicates(filter_text);
    zerompk::to_msgpack_vec(&filters).map_err(|e| {
        PgWireError::UserError(Box::new(ErrorInfo::new(
            "ERROR".to_owned(),
            "XX000".to_owned(),
            format!("filter serialization failed: {e}"),
        )))
    })
}

fn syntax_error(msg: &str) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_owned(),
        "42601".to_owned(),
        msg.to_owned(),
    )))
}
