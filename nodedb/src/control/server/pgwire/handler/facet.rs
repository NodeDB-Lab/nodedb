// SPDX-License-Identifier: BUSL-1.1

//! FACET_COUNTS and SEARCH_WITH_FACETS SQL interception.
//!
//! These are intercepted before DataFusion planning because they use a custom
//! SQL syntax that DataFusion cannot parse. The Control Plane parses the
//! arguments, builds a `QueryOp::FacetCounts` physical plan, dispatches to
//! the Data Plane, and formats the response.

use nodedb_sql::parser::preprocess::lex::find_ascii_case_insensitive;
use pgwire::api::results::Response;
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use sonic_rs;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::session::SessionId;
use crate::types::DatabaseId;
use nodedb_physical::physical_plan::QueryOp;
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

use super::super::types::error_to_sqlstate;
use super::core::NodeDbPgHandler;
use super::plan::multirow_payload_to_response;

/// Execute `SELECT FACET_COUNTS(collection => '...', filter => '...', fields => [...])`.
pub(super) async fn execute_facet_counts_sql(
    handler: &NodeDbPgHandler,
    identity: &AuthenticatedIdentity,
    session_id: SessionId,
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    let parsed = parse_facet_counts_args(sql)?;

    let tenant_id = identity.tenant_id;
    let database_id = handler
        .sessions
        .get_current_database(session_id)
        .unwrap_or(DatabaseId::DEFAULT);
    let vshard = nodedb_types::CollectionKey::from_bare(database_id, &parsed.collection).vshard();

    // Convert filter text to ScanFilter predicates.
    let filter_bytes = if parsed.filter.is_empty() {
        Vec::new()
    } else {
        build_filter_bytes(&parsed.filter)?
    };

    let (system_time, txn_id) = facet_read_scope(handler, session_id).await?;
    let task = PhysicalTask {
        tenant_id,
        vshard_id: vshard,
        database_id,
        plan: PhysicalPlan::Query(QueryOp::FacetCounts {
            collection: nodedb_types::QualifiedCollection::new(database_id, &parsed.collection),
            filters: filter_bytes,
            fields: parsed.fields,
            limit_per_facet: parsed.limit_per_facet,
            system_time,
        }),
        post_set_op: PostSetOp::None,
        txn_id,
    };

    let resp = handler
        .dispatch_authorized_task(task, None, identity, strong_read(handler, session_id))
        .await
        .map_err(|error| {
            let (severity, code, message) = error_to_sqlstate(&error);
            PgWireError::UserError(Box::new(ErrorInfo::new(
                severity.to_owned(),
                code.to_owned(),
                message,
            )))
        })?;

    Ok(vec![multirow_payload_to_response(&resp.payload)?.response])
}

/// Execute `SELECT SEARCH_WITH_FACETS(query => '...', facets => [...])`.
///
/// Runs the main search query via DataFusion, then runs a FacetCounts query
/// using the same WHERE predicate. Assembles `{ results: [...], facets: {...} }`.
pub(super) async fn execute_search_with_facets_sql(
    handler: &NodeDbPgHandler,
    identity: &AuthenticatedIdentity,
    session_id: SessionId,
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    let parsed = parse_search_with_facets_args(sql)?;

    // Step 1: Execute the main search query via the standard DataFusion path.
    let search_results = handler
        .execute_query_for_cursor(session_id, &parsed.query, identity)
        .await?;

    // Step 2: Extract collection and filter from the query for facet counting.
    // Parse the inner query's FROM and WHERE clauses.
    let (collection, filter_text) = extract_collection_and_filter(&parsed.query)?;

    let tenant_id = identity.tenant_id;
    let database_id = handler
        .sessions
        .get_current_database(session_id)
        .unwrap_or(DatabaseId::DEFAULT);
    let vshard = nodedb_types::CollectionKey::from_bare(database_id, &collection).vshard();

    let filter_bytes = if filter_text.is_empty() {
        Vec::new()
    } else {
        build_filter_bytes(&filter_text)?
    };

    let (system_time, txn_id) = facet_read_scope(handler, session_id).await?;
    let facet_task = PhysicalTask {
        tenant_id,
        vshard_id: vshard,
        database_id,
        plan: PhysicalPlan::Query(QueryOp::FacetCounts {
            collection: nodedb_types::QualifiedCollection::new(database_id, &collection),
            filters: filter_bytes,
            fields: parsed.facets,
            limit_per_facet: 0, // All values.
            system_time,
        }),
        post_set_op: PostSetOp::None,
        txn_id,
    };

    let facet_resp = handler
        .dispatch_authorized_task(facet_task, None, identity, strong_read(handler, session_id))
        .await
        .map_err(|error| {
            let (severity, code, message) = error_to_sqlstate(&error);
            PgWireError::UserError(Box::new(ErrorInfo::new(
                severity.to_owned(),
                code.to_owned(),
                message,
            )))
        })?;

    // Step 3: Assemble combined response.
    let results_json: Vec<serde_json::Value> = search_results
        .iter()
        .filter_map(|s| sonic_rs::from_str(s).ok())
        .collect();

    let facets_json: serde_json::Value = if facet_resp.payload.is_empty() {
        serde_json::json!({})
    } else {
        let text =
            crate::data::executor::response_codec::decode_payload_to_json(&facet_resp.payload);
        sonic_rs::from_str(&text).unwrap_or(serde_json::json!({}))
    };

    let combined = serde_json::json!({
        "results": results_json,
        "facets": facets_json,
    });

    let payload = sonic_rs::to_vec(&combined).unwrap_or_default();
    Ok(vec![multirow_payload_to_response(&payload)?.response])
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
/// Whether the session's reads must be linearizable.
/// The scope and transaction a facet count reads in. Inside a transaction
/// block it reads the block's snapshot with the block's staged writes, as
/// every other read of the block does. Outside one it reads the current
/// state.
async fn facet_read_scope(
    handler: &NodeDbPgHandler,
    session_id: SessionId,
) -> PgWireResult<(nodedb_types::SystemTimeScope, Option<crate::types::TxnId>)> {
    let snapshot = crate::control::server::shared::session::ensure_txn_snapshot(
        &handler.sessions,
        session_id,
        &handler.state,
    )
    .await
    .map_err(|error| {
        let (severity, code, message) = error_to_sqlstate(&error);
        PgWireError::UserError(Box::new(ErrorInfo::new(
            severity.to_owned(),
            code.to_owned(),
            message,
        )))
    })?;
    Ok(match snapshot {
        Some(snapshot) => (snapshot.scope(), handler.sessions.tx_id(session_id)),
        None => (nodedb_types::SystemTimeScope::Current, None),
    })
}

fn strong_read(handler: &NodeDbPgHandler, session_id: SessionId) -> bool {
    handler
        .sessions
        .read_consistency(session_id)
        .requires_leader()
}

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
    let pattern = format!("{name} =>");
    let pos = find_ascii_case_insensitive(sql, &pattern)?;
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
    let pattern = format!("{name} =>");
    let pos = find_ascii_case_insensitive(sql, &pattern)?;
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
    let pattern = format!("{name} =>");
    let pos = find_ascii_case_insensitive(sql, &pattern)?;
    let after = sql[pos + pattern.len()..].trim_start();
    // Take digits until non-digit.
    let num_str: String = after.chars().take_while(|c| c.is_ascii_digit()).collect();
    num_str.parse().ok()
}

/// Extract collection name and WHERE clause from a SELECT query.
fn extract_collection_and_filter(query: &str) -> PgWireResult<(String, String)> {
    let from_pos = find_ascii_case_insensitive(query, " FROM ")
        .ok_or_else(|| syntax_error("SEARCH_WITH_FACETS query must contain FROM clause"))?;

    let after_from = query[from_pos + 6..].trim_start();
    let collection = after_from
        .split_whitespace()
        .next()
        .unwrap_or("")
        .to_lowercase()
        .trim_end_matches(|c: char| !c.is_alphanumeric() && c != '_')
        .to_string();

    let filter = if let Some(where_pos) = find_ascii_case_insensitive(query, " WHERE ") {
        // Extract everything between WHERE and ORDER BY / LIMIT / end.
        let after_where = &query[where_pos + 7..];
        let end = ["ORDER BY", "LIMIT", "GROUP BY"]
            .iter()
            .filter_map(|kw| find_ascii_case_insensitive(after_where, kw))
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
            nodedb_types::error::sqlstate::INTERNAL_ERROR.to_owned(),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn named_argument_after_unicode_text_preserves_original_offsets() {
        let sql = "SELECT SEARCH_WITH_FACETS(note => 'İ', query => 'SELECT * FROM items', facets => ['kind'])";
        assert_eq!(
            extract_named_string_arg(sql, "query"),
            Some("SELECT * FROM items".to_string())
        );
    }

    #[test]
    fn inner_query_keywords_after_unicode_text_preserve_original_offsets() {
        let (collection, filter) =
            extract_collection_and_filter("SELECT ﬀﬀ FROM items WHERE kind = 'book' ORDER BY id")
                .unwrap();
        assert_eq!(collection, "items");
        assert_eq!(filter, "kind = 'book'");
    }
}
