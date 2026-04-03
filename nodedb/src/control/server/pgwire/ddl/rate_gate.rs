//! Rate gate / cooldown SQL functions.
//!
//! `SELECT RATE_CHECK(gate_name, key, max_count, window_secs)`
//!   — Atomically increments a counter for `(gate, key)`.
//!   — If counter > max_count → returns error with retry_after_ms.
//!   — Counter stored as KV key `_rate:{gate}:{key}` with TTL = window_secs.
//!   — Fixed window: TTL is set on first call only, not reset on subsequent calls.
//!
//! `SELECT RATE_REMAINING(gate_name, key, max_count, window_secs)`
//!   — Read-only: returns remaining budget and time until reset.
//!
//! `SELECT RATE_RESET(gate_name, key)`
//!   — Deletes the counter key (admin cooldown clear).

use futures::stream;
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response};
use pgwire::error::PgWireResult;

use crate::bridge::envelope::{PhysicalPlan, Status};
use crate::bridge::physical_plan::KvOp;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::types::VShardId;

/// Internal collection used for rate gate counters.
const RATE_COLLECTION: &str = "_system_rate_gates";

/// Handle `SELECT RATE_CHECK(gate_name, key, max_count, window_secs)`
pub async fn rate_check(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    let args = super::kv_atomic::parse_function_args(sql, "RATE_CHECK")?;
    if args.len() < 4 {
        return Err(sqlstate_error(
            "42601",
            "RATE_CHECK requires 4 arguments: (gate_name, key, max_count, window_secs)",
        ));
    }

    let gate_name = unquote(&args[0]);
    let key = unquote(&args[1]);
    let max_count: i64 = parse_i64(&args[2], "RATE_CHECK", "max_count")?;
    let window_secs: u64 = parse_u64(&args[3], "RATE_CHECK", "window_secs")?;

    if max_count <= 0 {
        return Err(sqlstate_error(
            "42601",
            "RATE_CHECK: max_count must be positive",
        ));
    }
    if window_secs == 0 {
        return Err(sqlstate_error(
            "42601",
            "RATE_CHECK: window_secs must be positive",
        ));
    }

    let rate_key = format!("_rate:{gate_name}:{key}");
    let tenant_id = identity.tenant_id;
    let vshard = VShardId::from_collection(RATE_COLLECTION);
    let ttl_ms = window_secs * 1000;

    // Fixed-window semantics: TTL is set ONLY on the first call (new key).
    // Subsequent calls within the window increment without resetting TTL.
    // To achieve this: check if key exists first, then INCR with ttl_ms=0
    // if it does, or ttl_ms=window if it doesn't.
    let key_exists = {
        let check = PhysicalPlan::Kv(KvOp::GetTtl {
            collection: RATE_COLLECTION.to_string(),
            key: rate_key.as_bytes().to_vec(),
        });
        match crate::control::server::dispatch_utils::dispatch_to_data_plane(
            state, tenant_id, vshard, check, 0,
        )
        .await
        {
            Ok(resp) if resp.status == Status::Ok => {
                let text =
                    crate::data::executor::response_codec::decode_payload_to_json(&resp.payload);
                // ttl_ms == -2 means key does not exist.
                !text.contains("-2")
            }
            _ => false,
        }
    };

    let actual_ttl = if key_exists { 0 } else { ttl_ms };

    let plan = PhysicalPlan::Kv(KvOp::Incr {
        collection: RATE_COLLECTION.to_string(),
        key: rate_key.as_bytes().to_vec(),
        delta: 1,
        ttl_ms: actual_ttl,
    });

    match crate::control::server::dispatch_utils::dispatch_to_data_plane(
        state, tenant_id, vshard, plan, 0,
    )
    .await
    {
        Ok(resp) if resp.status == Status::Ok => {
            let payload_text =
                crate::data::executor::response_codec::decode_payload_to_json(&resp.payload);
            let current: i64 = serde_json::from_str::<serde_json::Value>(&payload_text)
                .ok()
                .and_then(|v| v.get("value")?.as_i64())
                .unwrap_or(1);

            if current > max_count {
                // Read TTL to compute retry_after_ms.
                let ttl_remaining = read_ttl_ms(state, tenant_id, vshard, &rate_key).await;
                Err(sqlstate_error(
                    "54001",
                    &format!(
                        "rate limit exceeded for {gate_name}:{key}, retry after {ttl_remaining}ms (current={current}, max={max_count})"
                    ),
                ))
            } else {
                let result = serde_json::json!({
                    "allowed": true,
                    "current": current,
                    "max_count": max_count,
                    "remaining": max_count - current,
                });
                respond_json("rate_check", &result.to_string())
            }
        }
        Ok(resp) => {
            let payload_text =
                crate::data::executor::response_codec::decode_payload_to_json(&resp.payload);
            Err(sqlstate_error("XX000", &payload_text))
        }
        Err(e) => Err(sqlstate_error("XX000", &e.to_string())),
    }
}

/// Handle `SELECT RATE_REMAINING(gate_name, key, max_count, window_secs)`
pub async fn rate_remaining(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    let args = super::kv_atomic::parse_function_args(sql, "RATE_REMAINING")?;
    if args.len() < 4 {
        return Err(sqlstate_error(
            "42601",
            "RATE_REMAINING requires 4 arguments: (gate_name, key, max_count, window_secs)",
        ));
    }

    let gate_name = unquote(&args[0]);
    let key = unquote(&args[1]);
    let max_count: i64 = parse_i64(&args[2], "RATE_REMAINING", "max_count")?;
    let _window_secs: u64 = parse_u64(&args[3], "RATE_REMAINING", "window_secs")?;

    let rate_key = format!("_rate:{gate_name}:{key}");
    let tenant_id = identity.tenant_id;
    let vshard = VShardId::from_collection(RATE_COLLECTION);

    // Read current counter value (non-destructive).
    let plan = PhysicalPlan::Kv(KvOp::Get {
        collection: RATE_COLLECTION.to_string(),
        key: rate_key.as_bytes().to_vec(),
        rls_filters: Vec::new(),
    });

    let current = match crate::control::server::dispatch_utils::dispatch_to_data_plane(
        state, tenant_id, vshard, plan, 0,
    )
    .await
    {
        Ok(resp) if resp.status == Status::Ok && !resp.payload.is_empty() => {
            // Counter is stored as MessagePack i64.
            rmp_serde::from_slice::<i64>(&resp.payload).unwrap_or(0)
        }
        _ => 0, // Key doesn't exist yet — no usage.
    };

    let ttl_remaining = if current > 0 {
        read_ttl_ms(state, tenant_id, vshard, &rate_key).await
    } else {
        0
    };

    let remaining = (max_count - current).max(0);
    let result = serde_json::json!({
        "remaining": remaining,
        "current": current,
        "max_count": max_count,
        "resets_in_ms": ttl_remaining,
    });
    respond_json("rate_remaining", &result.to_string())
}

/// Handle `SELECT RATE_RESET(gate_name, key)`
pub async fn rate_reset(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    let args = super::kv_atomic::parse_function_args(sql, "RATE_RESET")?;
    if args.len() < 2 {
        return Err(sqlstate_error(
            "42601",
            "RATE_RESET requires 2 arguments: (gate_name, key)",
        ));
    }

    let gate_name = unquote(&args[0]);
    let key = unquote(&args[1]);

    let rate_key = format!("_rate:{gate_name}:{key}");
    let tenant_id = identity.tenant_id;
    let vshard = VShardId::from_collection(RATE_COLLECTION);

    let plan = PhysicalPlan::Kv(KvOp::Delete {
        collection: RATE_COLLECTION.to_string(),
        keys: vec![rate_key.as_bytes().to_vec()],
    });

    match crate::control::server::dispatch_utils::dispatch_to_data_plane(
        state, tenant_id, vshard, plan, 0,
    )
    .await
    {
        Ok(_) => {
            let result = serde_json::json!({
                "gate": gate_name,
                "key": key,
                "reset": true,
            });
            respond_json("rate_reset", &result.to_string())
        }
        Err(e) => Err(sqlstate_error("XX000", &e.to_string())),
    }
}

// ── Helpers ────────────────────────────────────────────────────────────

/// Read TTL remaining for a KV key (in milliseconds).
async fn read_ttl_ms(
    state: &SharedState,
    tenant_id: crate::types::TenantId,
    vshard: VShardId,
    key: &str,
) -> u64 {
    let plan = PhysicalPlan::Kv(KvOp::GetTtl {
        collection: RATE_COLLECTION.to_string(),
        key: key.as_bytes().to_vec(),
    });

    match crate::control::server::dispatch_utils::dispatch_to_data_plane(
        state, tenant_id, vshard, plan, 0,
    )
    .await
    {
        Ok(resp) if resp.status == Status::Ok => {
            let payload_text =
                crate::data::executor::response_codec::decode_payload_to_json(&resp.payload);
            serde_json::from_str::<serde_json::Value>(&payload_text)
                .ok()
                .and_then(|v| v.get("ttl_ms")?.as_i64())
                .map(|ttl| if ttl > 0 { ttl as u64 } else { 0 })
                .unwrap_or(0)
        }
        _ => 0,
    }
}

fn respond_json(col_name: &str, json_text: &str) -> PgWireResult<Vec<Response>> {
    let schema = std::sync::Arc::new(vec![super::super::types::text_field(col_name)]);
    let mut encoder = DataRowEncoder::new(schema.clone());
    let _ = encoder.encode_field(&json_text.to_string());
    let row = encoder.take_row();
    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(vec![Ok(row)]),
    ))])
}

fn unquote(s: &str) -> String {
    let t = s.trim();
    if t.starts_with('\'') && t.ends_with('\'') && t.len() >= 2 {
        t[1..t.len() - 1].to_string()
    } else {
        t.to_string()
    }
}

fn parse_i64(s: &str, func: &str, param: &str) -> PgWireResult<i64> {
    s.trim().parse().map_err(|_| {
        sqlstate_error(
            "42601",
            &format!("{func}: {param} must be an integer, got '{}'", s.trim()),
        )
    })
}

fn parse_u64(s: &str, func: &str, param: &str) -> PgWireResult<u64> {
    s.trim().parse().map_err(|_| {
        sqlstate_error(
            "42601",
            &format!(
                "{func}: {param} must be a positive integer, got '{}'",
                s.trim()
            ),
        )
    })
}

fn sqlstate_error(code: &str, message: &str) -> pgwire::error::PgWireError {
    super::super::types::sqlstate_error(code, message)
}
