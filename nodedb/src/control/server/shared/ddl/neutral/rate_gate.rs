// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral rate gate / cooldown SQL functions.
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

use crate::bridge::envelope::{PhysicalPlan, Status};
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TraceId, VShardId};
use nodedb_physical::physical_plan::KvOp;

use super::super::result::{DdlError, DdlResult};
use super::kv_atomic::single_text_col;

/// Internal collection used for rate gate counters.
const RATE_COLLECTION: &str = "_system_rate_gates";

/// Handle `SELECT RATE_CHECK(gate_name, key, max_count, window_secs)`
pub async fn rate_check(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    let args = super::kv_atomic::parse_function_args(sql, "RATE_CHECK")?;
    if args.len() < 4 {
        return Err(ddl_err(
            "42601",
            "RATE_CHECK requires 4 arguments: (gate_name, key, max_count, window_secs)",
        ));
    }

    let gate_name = unquote(&args[0]);
    let key = unquote(&args[1]);
    let max_count: i64 = parse_i64(&args[2], "RATE_CHECK", "max_count")?;
    let window_secs: u64 = parse_u64(&args[3], "RATE_CHECK", "window_secs")?;

    if max_count <= 0 {
        return Err(ddl_err("42601", "RATE_CHECK: max_count must be positive"));
    }
    if window_secs == 0 {
        return Err(ddl_err("42601", "RATE_CHECK: window_secs must be positive"));
    }

    let rate_key = format!("_rate:{gate_name}:{key}");
    let tenant_id = identity.tenant_id;
    let vshard = VShardId::from_collection_in_database(DatabaseId::DEFAULT, RATE_COLLECTION);
    let ttl_ms = window_secs * 1000;

    // Fixed-window semantics: TTL is set ONLY on the first call (new key).
    // Subsequent calls within the window increment without resetting TTL.
    // To achieve this: check if key exists first, then INCR with ttl_ms=0
    // if it does, or ttl_ms=window if it doesn't.
    let key_exists = {
        let check = PhysicalPlan::Kv(KvOp::GetTtl {
            collection: nodedb_types::QualifiedCollection::new(
                DatabaseId::DEFAULT,
                RATE_COLLECTION,
            ),
            key: rate_key.as_bytes().to_vec(),
        });
        match crate::control::server::dispatch_utils::dispatch_to_data_plane(
            state,
            tenant_id,
            crate::types::DatabaseId::DEFAULT,
            vshard,
            check,
            TraceId::ZERO,
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

    let surrogate = state
        .surrogate_assigner
        .assign(
            DatabaseId::DEFAULT,
            tenant_id,
            RATE_COLLECTION,
            rate_key.as_bytes(),
        )
        .map_err(|e| super::kv_atomic::ddl_err("XX000", e.to_string()))?;
    let plan = PhysicalPlan::Kv(KvOp::Incr {
        collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, RATE_COLLECTION),
        key: rate_key.as_bytes().to_vec(),
        delta: 1,
        ttl_ms: actual_ttl,
        surrogate,
        // The rate-gate collection is internal bookkeeping, not user rows. It
        // is never user-created and can never carry an RLS policy, so this
        // dispatch bypasses the injection pass by design rather than pending
        // a decision that will never come.
        rls_write_check: nodedb_types::RlsWriteCheck::system_internal_collection(),
        // The rate-gate collection declares no columns: a counter is decimal
        // text, which `rate_remaining` reads back.
        shape: nodedb_physical::physical_plan::KvCounterShape::Raw,
    });

    match dispatch_counter_write(state, tenant_id, vshard, plan).await {
        Ok(resp) if resp.status == Status::Ok => {
            let payload_text =
                crate::data::executor::response_codec::decode_payload_to_json(&resp.payload);
            let current: i64 = sonic_rs::from_str::<serde_json::Value>(&payload_text)
                .ok()
                .and_then(|v| v.get("value")?.as_i64())
                .unwrap_or(1);

            if current > max_count {
                // Read TTL to compute retry_after_ms.
                let ttl_remaining = read_ttl_ms(state, tenant_id, vshard, &rate_key).await;
                Err(ddl_err(
                    "54001",
                    format!(
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
                Ok(vec![single_text_col("rate_check", result.to_string())])
            }
        }
        Ok(resp) => {
            let payload_text =
                crate::data::executor::response_codec::decode_payload_to_json(&resp.payload);
            Err(ddl_err("XX000", payload_text))
        }
        Err(e) => Err(ddl_err("XX000", e.to_string())),
    }
}

/// Handle `SELECT RATE_REMAINING(gate_name, key, max_count, window_secs)`
pub async fn rate_remaining(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    let args = super::kv_atomic::parse_function_args(sql, "RATE_REMAINING")?;
    if args.len() < 4 {
        return Err(ddl_err(
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
    let vshard = VShardId::from_collection_in_database(DatabaseId::DEFAULT, RATE_COLLECTION);

    // Read current counter value (non-destructive).
    let plan = PhysicalPlan::Kv(KvOp::Get {
        collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, RATE_COLLECTION),
        key: rate_key.as_bytes().to_vec(),
        rls_filters: Vec::new(),
        surrogate_ceiling: None,
    });

    let current = match crate::control::server::dispatch_utils::dispatch_to_data_plane(
        state,
        tenant_id,
        crate::types::DatabaseId::DEFAULT,
        vshard,
        plan,
        TraceId::ZERO,
    )
    .await
    {
        Ok(resp) if resp.status == Status::Ok && !resp.payload.is_empty() => {
            // `KV_INCR` stores the counter as a raw body: its decimal text.
            std::str::from_utf8(&resp.payload)
                .ok()
                .and_then(|text| text.parse::<i64>().ok())
                .ok_or(ddl_err(
                    "XX000",
                    format!(
                        "RATE_REMAINING: counter '{rate_key}' does not hold decimal text; \
                         reset the gate with RATE_RESET"
                    ),
                ))?
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
    Ok(vec![single_text_col("rate_remaining", result.to_string())])
}

/// Handle `SELECT RATE_RESET(gate_name, key)`
pub async fn rate_reset(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    let args = super::kv_atomic::parse_function_args(sql, "RATE_RESET")?;
    if args.len() < 2 {
        return Err(ddl_err(
            "42601",
            "RATE_RESET requires 2 arguments: (gate_name, key)",
        ));
    }

    let gate_name = unquote(&args[0]);
    let key = unquote(&args[1]);

    let rate_key = format!("_rate:{gate_name}:{key}");
    let tenant_id = identity.tenant_id;
    let vshard = VShardId::from_collection_in_database(DatabaseId::DEFAULT, RATE_COLLECTION);

    let plan = PhysicalPlan::Kv(KvOp::Delete {
        collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, RATE_COLLECTION),
        keys: vec![rate_key.as_bytes().to_vec()],
        // Internal bookkeeping collection — see `rate_gate`'s INCR above.
        rls_write_check: nodedb_types::RlsWriteCheck::system_internal_collection(),
        returning: None,
        rls_filters: Vec::new(),
        provenance: None,
    });

    match dispatch_counter_write(state, tenant_id, vshard, plan).await {
        Ok(resp) if resp.status == Status::Ok => {
            let result = serde_json::json!({
                "gate": gate_name,
                "key": key,
                "reset": true,
            });
            Ok(vec![single_text_col("rate_reset", result.to_string())])
        }
        // A refusal arrives as an error status inside an `Ok` response. The
        // counter is still there, so the reset did not happen.
        Ok(resp) => Err(ddl_err(
            "XX000",
            format!(
                "RATE_RESET: the counter delete was refused: {:?}",
                resp.error_code
            ),
        )),
        Err(e) => Err(ddl_err("XX000", e.to_string())),
    }
}

// ── Helpers ────────────────────────────────────────────────────────────

/// Apply a write to the rate-gate counter collection on the durable route:
/// Raft in cluster mode, else the write funnel's `AppendHere`. A counter
/// written any other way has no WAL record, so a crash resets the gate and a
/// replica never counts the call.
async fn dispatch_counter_write(
    state: &SharedState,
    tenant_id: crate::types::TenantId,
    vshard: VShardId,
    plan: PhysicalPlan,
) -> crate::Result<crate::bridge::envelope::Response> {
    crate::control::server::dispatch_utils::dispatch_durable_autocommit_write(
        state,
        crate::control::server::dispatch_utils::AutocommitWrite {
            tenant_id,
            database_id: DatabaseId::DEFAULT,
            vshard_id: vshard,
            plan,
            trace_id: TraceId::ZERO,
            event_source: crate::event::EventSource::User,
            txn_id: None,
        },
    )
    .await
}

/// Read TTL remaining for a KV key (in milliseconds).
async fn read_ttl_ms(
    state: &SharedState,
    tenant_id: crate::types::TenantId,
    vshard: VShardId,
    key: &str,
) -> u64 {
    let plan = PhysicalPlan::Kv(KvOp::GetTtl {
        collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, RATE_COLLECTION),
        key: key.as_bytes().to_vec(),
    });

    match crate::control::server::dispatch_utils::dispatch_to_data_plane(
        state,
        tenant_id,
        crate::types::DatabaseId::DEFAULT,
        vshard,
        plan,
        TraceId::ZERO,
    )
    .await
    {
        Ok(resp) if resp.status == Status::Ok => {
            let payload_text =
                crate::data::executor::response_codec::decode_payload_to_json(&resp.payload);
            sonic_rs::from_str::<serde_json::Value>(&payload_text)
                .ok()
                .and_then(|v| v.get("ttl_ms")?.as_i64())
                .map(|ttl| if ttl > 0 { ttl as u64 } else { 0 })
                .unwrap_or(0)
        }
        _ => 0,
    }
}

fn unquote(s: &str) -> String {
    let t = s.trim();
    if t.starts_with('\'') && t.ends_with('\'') && t.len() >= 2 {
        t[1..t.len() - 1].to_string()
    } else {
        t.to_string()
    }
}

fn parse_i64(s: &str, func: &str, param: &str) -> Result<i64, DdlError> {
    s.trim().parse().map_err(|_| {
        ddl_err(
            "42601",
            format!("{func}: {param} must be an integer, got '{}'", s.trim()),
        )
    })
}

fn parse_u64(s: &str, func: &str, param: &str) -> Result<u64, DdlError> {
    s.trim().parse().map_err(|_| {
        ddl_err(
            "42601",
            format!(
                "{func}: {param} must be a positive integer, got '{}'",
                s.trim()
            ),
        )
    })
}

fn ddl_err(sqlstate: &str, message: impl Into<String>) -> DdlError {
    DdlError::new(sqlstate, message)
}
