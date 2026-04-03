//! DDL handlers for atomic KV SQL functions: KV_INCR, KV_DECR, KV_CAS, KV_GETSET.
//!
//! These are side-effecting operations that dispatch to the Data Plane via the
//! SPSC bridge, so they cannot be pure DataFusion UDFs. Instead they're intercepted
//! in the DDL router before DataFusion parsing.

use futures::stream;
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response};
use pgwire::error::PgWireResult;

use crate::bridge::physical_plan::{KvOp, PhysicalPlan};
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::types::VShardId;

/// Handle `SELECT KV_INCR(collection, key, delta [, TTL => seconds])`
///
/// Returns `{"value": <new_value>}` as a single text column.
pub async fn kv_incr(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
    negate: bool,
) -> PgWireResult<Vec<Response>> {
    let func_name = if negate { "KV_DECR" } else { "KV_INCR" };
    let args = parse_function_args(sql, func_name)?;

    if args.len() < 3 {
        return Err(sqlstate_error(
            "42601",
            &format!("{func_name} requires at least 3 arguments: (collection, key, delta)"),
        ));
    }

    let collection = unquote(&args[0]).to_lowercase();
    let key = unquote(&args[1]);
    let delta: i64 = parse_i64(&args[2], func_name)?;
    let delta = if negate { -delta } else { delta };

    let ttl_ms = parse_optional_ttl(&args[3..])?;

    let vshard = VShardId::from_collection(&collection);
    let plan = PhysicalPlan::Kv(KvOp::Incr {
        collection,
        key: key.as_bytes().to_vec(),
        delta,
        ttl_ms,
    });

    dispatch_and_respond(state, identity, vshard, plan, func_name).await
}

/// Handle `SELECT KV_INCR_FLOAT(collection, key, delta)`
///
/// Returns `{"value": <new_value>}` as a single text column.
pub async fn kv_incr_float(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    let args = parse_function_args(sql, "KV_INCR_FLOAT")?;

    if args.len() < 3 {
        return Err(sqlstate_error(
            "42601",
            "KV_INCR_FLOAT requires 3 arguments: (collection, key, delta)",
        ));
    }

    let collection = unquote(&args[0]).to_lowercase();
    let key = unquote(&args[1]);
    let delta: f64 = args[2].trim().parse().map_err(|_| {
        sqlstate_error(
            "42601",
            &format!("KV_INCR_FLOAT: delta must be a float, got '{}'", args[2]),
        )
    })?;

    let vshard = VShardId::from_collection(&collection);
    let plan = PhysicalPlan::Kv(KvOp::IncrFloat {
        collection,
        key: key.as_bytes().to_vec(),
        delta,
    });

    dispatch_and_respond(state, identity, vshard, plan, "KV_INCR_FLOAT").await
}

/// Handle `SELECT KV_CAS(collection, key, expected, new_value)`
///
/// Returns `{"success": bool, "current_value": "<base64>"}` as a single text column.
pub async fn kv_cas(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    let args = parse_function_args(sql, "KV_CAS")?;

    if args.len() < 4 {
        return Err(sqlstate_error(
            "42601",
            "KV_CAS requires 4 arguments: (collection, key, expected, new_value)",
        ));
    }

    let collection = unquote(&args[0]).to_lowercase();
    let key = unquote(&args[1]);
    let expected = unquote(&args[2]);
    let new_value = unquote(&args[3]);

    let vshard = VShardId::from_collection(&collection);
    let plan = PhysicalPlan::Kv(KvOp::Cas {
        collection,
        key: key.as_bytes().to_vec(),
        expected: expected.into_bytes(),
        new_value: new_value.into_bytes(),
    });

    dispatch_and_respond(state, identity, vshard, plan, "KV_CAS").await
}

/// Handle `SELECT KV_GETSET(collection, key, new_value)`
///
/// Returns `{"old_value": "<base64>"}` as a single text column.
pub async fn kv_getset(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    let args = parse_function_args(sql, "KV_GETSET")?;

    if args.len() < 3 {
        return Err(sqlstate_error(
            "42601",
            "KV_GETSET requires 3 arguments: (collection, key, new_value)",
        ));
    }

    let collection = unquote(&args[0]).to_lowercase();
    let key = unquote(&args[1]);
    let new_value = unquote(&args[2]);

    let vshard = VShardId::from_collection(&collection);
    let plan = PhysicalPlan::Kv(KvOp::GetSet {
        collection,
        key: key.as_bytes().to_vec(),
        new_value: new_value.into_bytes(),
    });

    dispatch_and_respond(state, identity, vshard, plan, "KV_GETSET").await
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Dispatch a KvOp to the Data Plane and return the JSON response as a pgwire row.
async fn dispatch_and_respond(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    vshard: VShardId,
    plan: PhysicalPlan,
    func_name: &str,
) -> PgWireResult<Vec<Response>> {
    let tenant_id = identity.tenant_id;

    match crate::control::server::dispatch_utils::dispatch_to_data_plane(
        state, tenant_id, vshard, plan, 0,
    )
    .await
    {
        Ok(resp) => {
            let payload_text =
                crate::data::executor::response_codec::decode_payload_to_json(&resp.payload);
            let col_name = func_name.to_lowercase();
            let schema = std::sync::Arc::new(vec![super::super::types::text_field(&col_name)]);
            let mut encoder = DataRowEncoder::new(schema.clone());
            let _ = encoder.encode_field(&payload_text);
            let row = encoder.take_row();
            Ok(vec![Response::Query(QueryResponse::new(
                schema,
                stream::iter(vec![Ok(row)]),
            ))])
        }
        Err(e) => Err(sqlstate_error("XX000", &e.to_string())),
    }
}

/// Parse function arguments from `SELECT FUNC_NAME(arg1, arg2, ...)`.
///
/// Handles quoted strings with commas inside them.
fn parse_function_args(sql: &str, _func_name: &str) -> PgWireResult<Vec<String>> {
    let start = sql
        .find('(')
        .ok_or_else(|| sqlstate_error("42601", "expected '(' in function call"))?;
    let end = sql
        .rfind(')')
        .ok_or_else(|| sqlstate_error("42601", "expected ')' in function call"))?;
    if start >= end {
        return Ok(Vec::new());
    }

    let inner = &sql[start + 1..end];
    Ok(split_args(inner))
}

/// Split comma-separated arguments, respecting single-quoted strings.
pub(super) fn split_args(s: &str) -> Vec<String> {
    let mut args = Vec::new();
    let mut current = String::new();
    let mut in_quote = false;

    for ch in s.chars() {
        match ch {
            '\'' if !in_quote => {
                in_quote = true;
                current.push(ch);
            }
            '\'' if in_quote => {
                in_quote = false;
                current.push(ch);
            }
            ',' if !in_quote => {
                args.push(current.trim().to_string());
                current.clear();
            }
            _ => current.push(ch),
        }
    }
    let trimmed = current.trim().to_string();
    if !trimmed.is_empty() {
        args.push(trimmed);
    }
    args
}

/// Remove surrounding single quotes from a string argument.
fn unquote(s: &str) -> String {
    let t = s.trim();
    if t.starts_with('\'') && t.ends_with('\'') && t.len() >= 2 {
        t[1..t.len() - 1].to_string()
    } else {
        t.to_string()
    }
}

/// Parse an i64 from a string argument.
fn parse_i64(s: &str, func_name: &str) -> PgWireResult<i64> {
    s.trim().parse().map_err(|_| {
        sqlstate_error(
            "42601",
            &format!("{func_name}: delta must be an integer, got '{}'", s.trim()),
        )
    })
}

/// Parse optional `TTL => seconds` from remaining args.
///
/// Supports: `TTL => 86400` or just a bare number as the 4th arg.
fn parse_optional_ttl(args: &[String]) -> PgWireResult<u64> {
    if args.is_empty() {
        return Ok(0);
    }

    // Check for `TTL => value` pattern.
    for (i, arg) in args.iter().enumerate() {
        let upper = arg.trim().to_uppercase();
        if upper.starts_with("TTL") {
            // Formats: "TTL => 86400" or split across args: "TTL", "=>", "86400"
            if let Some(val_str) = upper
                .strip_prefix("TTL")
                .map(|r| r.trim_start_matches("=>").trim_start_matches('=').trim())
                && !val_str.is_empty()
            {
                return parse_ttl_seconds(val_str);
            }
            // Look at next arg(s) for the value.
            let remaining: Vec<&str> = args[i + 1..].iter().map(|s| s.trim()).collect();
            for r in &remaining {
                let cleaned = r.trim_start_matches("=>").trim_start_matches('=').trim();
                if !cleaned.is_empty() {
                    return parse_ttl_seconds(cleaned);
                }
            }
        }
    }

    Ok(0)
}

/// Parse TTL seconds → milliseconds.
fn parse_ttl_seconds(s: &str) -> PgWireResult<u64> {
    let secs: u64 = s.parse().map_err(|_| {
        sqlstate_error(
            "42601",
            &format!("TTL must be a positive integer (seconds), got '{s}'"),
        )
    })?;
    Ok(secs * 1000)
}

/// Create a pgwire SQL state error.
fn sqlstate_error(code: &str, message: &str) -> pgwire::error::PgWireError {
    super::super::types::sqlstate_error(code, message)
}
