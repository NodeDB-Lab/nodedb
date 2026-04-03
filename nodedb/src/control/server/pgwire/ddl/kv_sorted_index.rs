//! DDL and SQL function handlers for sorted indexes (leaderboards).
//!
//! DDL:
//!   CREATE SORTED INDEX name ON collection (score DESC [, tiebreak ASC]) KEY key_col [WINDOW DAILY ON ts_col]
//!   DROP SORTED INDEX name
//!
//! SQL functions (intercepted in DDL router):
//!   SELECT RANK(index_name, 'key_value')
//!   SELECT * FROM TOPK(index_name, k)
//!   SELECT * FROM RANGE(index_name, score_min, score_max)
//!   SELECT COUNT(index_name)  -- when index_name is a sorted index

use futures::stream;
use pgwire::api::results::{DataRowEncoder, FieldInfo, QueryResponse, Response};
use pgwire::error::PgWireResult;

use crate::bridge::physical_plan::{KvOp, PhysicalPlan};
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::types::VShardId;

/// Handle `CREATE SORTED INDEX name ON collection (col DIR, ...) KEY key_col [WINDOW ...]`
pub async fn create_sorted_index(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    let upper = sql.to_uppercase();

    // Extract index name.
    let rest = upper
        .strip_prefix("CREATE SORTED INDEX ")
        .ok_or_else(|| sqlstate_error("42601", "expected CREATE SORTED INDEX"))?;
    let index_name = rest
        .split_whitespace()
        .next()
        .ok_or_else(|| sqlstate_error("42601", "missing index name"))?
        .to_lowercase();

    // Extract collection name after ON.
    let on_pos = upper
        .find(" ON ")
        .ok_or_else(|| sqlstate_error("42601", "missing ON clause"))?;
    let after_on = sql[on_pos + 4..].trim();
    let collection = after_on
        .split_whitespace()
        .next()
        .ok_or_else(|| sqlstate_error("42601", "missing collection name after ON"))?
        .to_lowercase();

    // Extract sort columns from parentheses.
    let paren_start = sql
        .find('(')
        .ok_or_else(|| sqlstate_error("42601", "missing '(' for sort columns"))?;
    let paren_end = sql
        .find(')')
        .ok_or_else(|| sqlstate_error("42601", "missing ')' for sort columns"))?;
    let cols_str = &sql[paren_start + 1..paren_end];
    let sort_columns = parse_sort_columns(cols_str)?;

    // Extract KEY column.
    let key_column = parse_key_column(&upper, sql)?;

    // Extract WINDOW clause (optional).
    let (window_type, window_ts_col, window_start, window_end) = parse_window_clause(&upper, sql);

    let vshard = VShardId::from_collection(&collection);
    let plan = PhysicalPlan::Kv(KvOp::RegisterSortedIndex {
        collection,
        index_name: index_name.clone(),
        sort_columns,
        key_column,
        window_type,
        window_timestamp_column: window_ts_col,
        window_start_ms: window_start,
        window_end_ms: window_end,
    });

    dispatch_and_respond_tag(state, identity, vshard, plan, "CREATE SORTED INDEX").await
}

/// Handle `DROP SORTED INDEX name`
pub async fn drop_sorted_index(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    let upper = sql.to_uppercase();
    let rest = upper
        .strip_prefix("DROP SORTED INDEX ")
        .ok_or_else(|| sqlstate_error("42601", "expected DROP SORTED INDEX"))?;
    let index_name = rest
        .split_whitespace()
        .next()
        .ok_or_else(|| sqlstate_error("42601", "missing index name"))?
        .to_lowercase();

    let plan = PhysicalPlan::Kv(KvOp::DropSortedIndex {
        index_name: index_name.clone(),
    });

    // Drop doesn't target a specific collection, use default vShard.
    let vshard = VShardId::from_collection(&index_name);
    dispatch_and_respond_tag(state, identity, vshard, plan, "DROP SORTED INDEX").await
}

/// Handle `SELECT RANK(index_name, 'key_value')`
pub async fn select_rank(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    let args = parse_function_args(sql)?;
    if args.len() < 2 {
        return Err(sqlstate_error(
            "42601",
            "RANK requires 2 arguments: (index_name, key_value)",
        ));
    }

    let index_name = unquote(&args[0]).to_lowercase();
    let key_value = unquote(&args[1]);

    let plan = PhysicalPlan::Kv(KvOp::SortedIndexRank {
        index_name: index_name.clone(),
        primary_key: key_value.into_bytes(),
    });

    let vshard = VShardId::from_collection(&index_name);
    dispatch_and_respond_json(state, identity, vshard, plan, "rank").await
}

/// Handle `SELECT * FROM TOPK(index_name, k)` or `SELECT TOPK(index_name, k)`
pub async fn select_topk(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    let args = parse_function_args(sql)?;
    if args.len() < 2 {
        return Err(sqlstate_error(
            "42601",
            "TOPK requires 2 arguments: (index_name, k)",
        ));
    }

    let index_name = unquote(&args[0]).to_lowercase();
    let k: u32 = args[1].trim().parse().map_err(|_| {
        sqlstate_error(
            "42601",
            &format!("TOPK: k must be a positive integer, got '{}'", args[1]),
        )
    })?;

    let plan = PhysicalPlan::Kv(KvOp::SortedIndexTopK {
        index_name: index_name.clone(),
        k,
    });

    let vshard = VShardId::from_collection(&index_name);
    dispatch_and_respond_rows(state, identity, vshard, plan).await
}

/// Handle `SELECT * FROM RANGE(index_name, score_min, score_max)`
pub async fn select_range(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    let args = parse_function_args(sql)?;
    if args.len() < 3 {
        return Err(sqlstate_error(
            "42601",
            "RANGE requires 3 arguments: (index_name, score_min, score_max)",
        ));
    }

    let index_name = unquote(&args[0]).to_lowercase();
    let score_min = parse_score_arg(&args[1]);
    let score_max = parse_score_arg(&args[2]);

    let plan = PhysicalPlan::Kv(KvOp::SortedIndexRange {
        index_name: index_name.clone(),
        score_min,
        score_max,
    });

    let vshard = VShardId::from_collection(&index_name);
    dispatch_and_respond_rows(state, identity, vshard, plan).await
}

/// Handle `SELECT SORTED_COUNT(index_name)`
pub async fn select_sorted_count(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    let args = parse_function_args(sql)?;
    if args.is_empty() {
        return Err(sqlstate_error(
            "42601",
            "SORTED_COUNT requires 1 argument: (index_name)",
        ));
    }

    let index_name = unquote(&args[0]).to_lowercase();

    let plan = PhysicalPlan::Kv(KvOp::SortedIndexCount {
        index_name: index_name.clone(),
    });

    let vshard = VShardId::from_collection(&index_name);
    dispatch_and_respond_json(state, identity, vshard, plan, "sorted_count").await
}

// ── Helpers ────────────────────────────────────────────────────────────

fn parse_sort_columns(cols_str: &str) -> PgWireResult<Vec<(String, String)>> {
    let mut columns = Vec::new();
    for part in cols_str.split(',') {
        let tokens: Vec<&str> = part.split_whitespace().collect();
        if tokens.is_empty() {
            continue;
        }
        let name = tokens[0].to_lowercase();
        let dir = if tokens.len() > 1 {
            tokens[1].to_uppercase()
        } else {
            "ASC".into()
        };
        if dir != "ASC" && dir != "DESC" {
            return Err(sqlstate_error(
                "42601",
                &format!("invalid sort direction '{dir}', expected ASC or DESC"),
            ));
        }
        columns.push((name, dir));
    }
    if columns.is_empty() {
        return Err(sqlstate_error("42601", "at least one sort column required"));
    }
    Ok(columns)
}

fn parse_key_column(upper: &str, _sql: &str) -> PgWireResult<String> {
    // Look for "KEY <column_name>" after the closing paren.
    let key_pos = upper
        .find(") KEY ")
        .or_else(|| upper.find(")KEY "))
        .ok_or_else(|| sqlstate_error("42601", "missing KEY clause"))?;
    let after_key = upper[key_pos..].trim_start_matches(')').trim();
    let after_key = after_key
        .strip_prefix("KEY ")
        .ok_or_else(|| sqlstate_error("42601", "missing KEY clause"))?;
    let key_col = after_key
        .split_whitespace()
        .next()
        .ok_or_else(|| sqlstate_error("42601", "missing key column name"))?
        .trim_end_matches(';')
        .to_lowercase();
    Ok(key_col)
}

fn parse_window_clause(upper: &str, _sql: &str) -> (String, String, u64, u64) {
    // Look for "WINDOW <type> ON <ts_col>" or "WINDOW CUSTOM START '...' END '...'"
    let Some(win_pos) = upper.find(" WINDOW ") else {
        return ("none".into(), String::new(), 0, 0);
    };

    let after_window = &upper[win_pos + 8..];
    let tokens: Vec<&str> = after_window.split_whitespace().collect();

    if tokens.is_empty() {
        return ("none".into(), String::new(), 0, 0);
    }

    let win_type = tokens[0].to_lowercase();

    match win_type.as_str() {
        "daily" | "weekly" | "monthly" => {
            // WINDOW DAILY ON ts_col
            let ts_col = if tokens.len() >= 3 && tokens[1] == "ON" {
                tokens[2].to_lowercase()
            } else {
                "updated_at".into()
            };
            (win_type, ts_col, 0, 0)
        }
        "custom" => {
            // WINDOW CUSTOM START '2026-01-01' END '2026-03-31'
            let mut start_ms = 0u64;
            let mut end_ms = 0u64;
            let mut ts_col = "updated_at".to_string();

            for i in 1..tokens.len() {
                if tokens[i] == "START" && i + 1 < tokens.len() {
                    start_ms = tokens[i + 1].trim_matches('\'').parse().unwrap_or(0);
                }
                if tokens[i] == "END" && i + 1 < tokens.len() {
                    end_ms = tokens[i + 1].trim_matches('\'').parse().unwrap_or(0);
                }
                if tokens[i] == "ON" && i + 1 < tokens.len() {
                    ts_col = tokens[i + 1].to_lowercase();
                }
            }
            ("custom".into(), ts_col, start_ms, end_ms)
        }
        _ => ("none".into(), String::new(), 0, 0),
    }
}

fn parse_function_args(sql: &str) -> PgWireResult<Vec<String>> {
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
    Ok(super::kv_atomic::split_args(inner))
}

fn unquote(s: &str) -> String {
    let t = s.trim();
    if t.starts_with('\'') && t.ends_with('\'') && t.len() >= 2 {
        t[1..t.len() - 1].to_string()
    } else {
        t.to_string()
    }
}

fn parse_score_arg(s: &str) -> Option<Vec<u8>> {
    let t = unquote(s);
    if t.eq_ignore_ascii_case("NULL") || t.eq_ignore_ascii_case("NONE") || t == "*" {
        return None;
    }
    // Try to parse as i64 and encode.
    if let Ok(v) = t.parse::<i64>() {
        return Some(crate::engine::kv::sorted_index::key::SortKeyEncoder::encode_i64(v).to_vec());
    }
    // Fallback: raw bytes.
    Some(t.into_bytes())
}

/// Dispatch plan and return a tag response (for DDL).
async fn dispatch_and_respond_tag(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    vshard: VShardId,
    plan: PhysicalPlan,
    tag: &str,
) -> PgWireResult<Vec<Response>> {
    match crate::control::server::dispatch_utils::dispatch_to_data_plane(
        state,
        identity.tenant_id,
        vshard,
        plan,
        0,
    )
    .await
    {
        Ok(_) => Ok(vec![Response::Execution(pgwire::api::results::Tag::new(
            tag,
        ))]),
        Err(e) => Err(sqlstate_error("XX000", &e.to_string())),
    }
}

/// Dispatch plan and return a single-row JSON response.
async fn dispatch_and_respond_json(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    vshard: VShardId,
    plan: PhysicalPlan,
    col_name: &str,
) -> PgWireResult<Vec<Response>> {
    match crate::control::server::dispatch_utils::dispatch_to_data_plane(
        state,
        identity.tenant_id,
        vshard,
        plan,
        0,
    )
    .await
    {
        Ok(resp) => {
            let payload_text =
                crate::data::executor::response_codec::decode_payload_to_json(&resp.payload);
            let schema = std::sync::Arc::new(vec![text_field(col_name)]);
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

/// Dispatch plan and return multi-row response (for TOPK, RANGE).
async fn dispatch_and_respond_rows(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    vshard: VShardId,
    plan: PhysicalPlan,
) -> PgWireResult<Vec<Response>> {
    match crate::control::server::dispatch_utils::dispatch_to_data_plane(
        state,
        identity.tenant_id,
        vshard,
        plan,
        0,
    )
    .await
    {
        Ok(resp) => {
            let schema = std::sync::Arc::new(vec![text_field("rank"), text_field("key")]);

            let rows_json: Vec<serde_json::Value> =
                serde_json::from_slice(&resp.payload).unwrap_or_default();

            let mut rows = Vec::with_capacity(rows_json.len());
            for row in &rows_json {
                let mut encoder = DataRowEncoder::new(schema.clone());
                let rank = row
                    .get("rank")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0)
                    .to_string();
                let key = row
                    .get("key")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let _ = encoder.encode_field(&rank);
                let _ = encoder.encode_field(&key);
                rows.push(Ok(encoder.take_row()));
            }

            Ok(vec![Response::Query(QueryResponse::new(
                schema,
                stream::iter(rows),
            ))])
        }
        Err(e) => Err(sqlstate_error("XX000", &e.to_string())),
    }
}

fn text_field(name: &str) -> FieldInfo {
    super::super::types::text_field(name)
}

fn sqlstate_error(code: &str, message: &str) -> pgwire::error::PgWireError {
    super::super::types::sqlstate_error(code, message)
}
