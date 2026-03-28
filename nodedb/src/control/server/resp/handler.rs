//! RESP command handlers: translate Redis commands into KvOp dispatches.

use crate::bridge::envelope::{PhysicalPlan, Status};
use crate::bridge::physical_plan::KvOp;
use crate::control::server::dispatch_utils;
use crate::control::server::wal_dispatch;
use crate::control::state::SharedState;
use crate::types::VShardId;

use super::codec::RespValue;
use super::command::RespCommand;
use super::session::RespSession;

/// Execute a RESP command and return the response.
pub async fn execute(
    cmd: &RespCommand,
    session: &mut RespSession,
    state: &SharedState,
) -> RespValue {
    match cmd.name.as_str() {
        "PING" => handle_ping(cmd),
        "ECHO" => handle_echo(cmd),
        "SELECT" => handle_select(cmd, session),
        "DBSIZE" => handle_dbsize(session, state).await,
        "GET" => super::handler_kv::handle_get(cmd, session, state).await,
        "SET" => super::handler_kv::handle_set(cmd, session, state).await,
        "DEL" => super::handler_kv::handle_del(cmd, session, state).await,
        "EXISTS" => super::handler_kv::handle_exists(cmd, session, state).await,
        "MGET" => super::handler_kv::handle_mget(cmd, session, state).await,
        "MSET" => super::handler_kv::handle_mset(cmd, session, state).await,
        "EXPIRE" => handle_expire(cmd, session, state, false).await,
        "PEXPIRE" => handle_expire(cmd, session, state, true).await,
        "TTL" => handle_ttl(cmd, session, state, false).await,
        "PTTL" => handle_ttl(cmd, session, state, true).await,
        "PERSIST" => handle_persist(cmd, session, state).await,
        "SCAN" => handle_scan(cmd, session, state).await,
        "KEYS" => handle_keys(cmd, session, state).await,
        "HGET" => super::handler_hash::handle_hget(cmd, session, state).await,
        "HMGET" => super::handler_hash::handle_hmget(cmd, session, state).await,
        "HSET" => super::handler_hash::handle_hset(cmd, session, state).await,
        "FLUSHDB" => super::handler_hash::handle_flushdb(session, state).await,
        "PUBLISH" => super::handler_pubsub::handle_publish(cmd, session, state).await,
        "INFO" => handle_info(cmd, session, state).await,
        "COMMAND" => RespValue::ok(), // Stub: redis-cli sends COMMAND on connect.
        "QUIT" => RespValue::ok(),
        _ => RespValue::err(format!("ERR unknown command '{}'", cmd.name)),
    }
}

// ---------------------------------------------------------------------------
// Simple commands
// ---------------------------------------------------------------------------

fn handle_ping(cmd: &RespCommand) -> RespValue {
    match cmd.arg(0) {
        Some(msg) => RespValue::bulk(msg.to_vec()),
        None => RespValue::SimpleString("PONG".into()),
    }
}

fn handle_echo(cmd: &RespCommand) -> RespValue {
    match cmd.arg(0) {
        Some(msg) => RespValue::bulk(msg.to_vec()),
        None => RespValue::err("ERR wrong number of arguments for 'echo' command"),
    }
}

fn handle_select(cmd: &RespCommand, session: &mut RespSession) -> RespValue {
    match cmd.arg_str(0) {
        Some(name) => {
            session.collection = name.to_string();
            RespValue::ok()
        }
        None => RespValue::err("ERR wrong number of arguments for 'select' command"),
    }
}

// ---------------------------------------------------------------------------
// TTL commands
// ---------------------------------------------------------------------------

async fn handle_expire(
    cmd: &RespCommand,
    session: &RespSession,
    state: &SharedState,
    is_pexpire: bool,
) -> RespValue {
    if cmd.argc() < 2 {
        let name = if is_pexpire { "pexpire" } else { "expire" };
        return RespValue::err(format!(
            "ERR wrong number of arguments for '{name}' command"
        ));
    }

    let key = cmd.args[0].clone();
    let ttl_ms = match cmd.arg_i64(1) {
        Some(v) if v > 0 => {
            if is_pexpire {
                v as u64
            } else {
                (v as u64) * 1000
            }
        }
        _ => return RespValue::err("ERR value is not an integer or out of range"),
    };

    let plan = PhysicalPlan::Kv(KvOp::Expire {
        collection: session.collection.clone(),
        key,
        ttl_ms,
    });

    match dispatch_kv_write(state, session, plan).await {
        Ok(resp) if resp.status == Status::Ok => RespValue::integer(1),
        Ok(_) => RespValue::integer(0),
        Err(e) => RespValue::err(format!("ERR {e}")),
    }
}

async fn handle_ttl(
    cmd: &RespCommand,
    session: &RespSession,
    state: &SharedState,
    is_pttl: bool,
) -> RespValue {
    let Some(key) = cmd.arg(0) else {
        let name = if is_pttl { "pttl" } else { "ttl" };
        return RespValue::err(format!(
            "ERR wrong number of arguments for '{name}' command"
        ));
    };

    // TTL requires getting the entry metadata — use GET to check existence.
    // The full TTL query would need the entry's expire_at_ms, which is not
    // currently returned by KvOp::Get. For now, return -1 (no TTL) if the
    // key exists, -2 if it doesn't.
    let plan = PhysicalPlan::Kv(KvOp::Get {
        collection: session.collection.clone(),
        key: key.to_vec(),
    });

    match dispatch_kv(state, session, plan).await {
        Ok(resp) if resp.status == Status::Ok && !resp.payload.is_empty() => RespValue::integer(-1),
        Ok(_) => RespValue::integer(-2),
        Err(e) => RespValue::err(format!("ERR {e}")),
    }
}

async fn handle_persist(
    cmd: &RespCommand,
    session: &RespSession,
    state: &SharedState,
) -> RespValue {
    let Some(key) = cmd.arg(0) else {
        return RespValue::err("ERR wrong number of arguments for 'persist' command");
    };

    let plan = PhysicalPlan::Kv(KvOp::Persist {
        collection: session.collection.clone(),
        key: key.to_vec(),
    });

    match dispatch_kv_write(state, session, plan).await {
        Ok(resp) if resp.status == Status::Ok => RespValue::integer(1),
        Ok(_) => RespValue::integer(0),
        Err(e) => RespValue::err(format!("ERR {e}")),
    }
}

// ---------------------------------------------------------------------------
// SCAN / KEYS
// ---------------------------------------------------------------------------

async fn handle_scan(cmd: &RespCommand, session: &RespSession, state: &SharedState) -> RespValue {
    let cursor_str = cmd.arg_str(0).unwrap_or("0");
    let cursor = if cursor_str == "0" {
        Vec::new()
    } else {
        base64::Engine::decode(&base64::engine::general_purpose::STANDARD, cursor_str)
            .unwrap_or_default()
    };

    // Parse MATCH, COUNT, and FILTER options.
    let mut match_pattern: Option<String> = None;
    let mut count: usize = 10;
    let mut filter_bytes: Vec<u8> = Vec::new();
    let mut i = 1;
    while i < cmd.argc() {
        match cmd.arg_str(i).map(|s| s.to_uppercase()) {
            Some(ref flag) if flag == "MATCH" => {
                match_pattern = cmd.arg_str(i + 1).map(|s| s.to_string());
                i += 2;
            }
            Some(ref flag) if flag == "COUNT" => {
                count = cmd.arg_i64(i + 1).unwrap_or(10) as usize;
                i += 2;
            }
            // NodeDB extension: SCAN 0 FILTER <field> = <value>
            Some(ref flag) if flag == "FILTER" => {
                // Parse simple "field = value" predicate (needs 4 args: FILTER field = value).
                if i + 4 <= cmd.argc() {
                    let field = cmd.arg_str(i + 1).unwrap_or("");
                    let _op = cmd.arg_str(i + 2).unwrap_or(""); // "=" expected
                    let value = cmd.arg_str(i + 3).unwrap_or("");
                    let scan_filter = serde_json::json!([{
                        "field": field,
                        "op": "eq",
                        "value": value,
                    }]);
                    match rmp_serde::to_vec(&scan_filter) {
                        Ok(bytes) => filter_bytes = bytes,
                        Err(_) => {
                            return RespValue::err("ERR filter serialization failed");
                        }
                    }
                    i += 4;
                } else {
                    i += 1;
                }
            }
            _ => {
                i += 1;
            }
        }
    }

    let plan = PhysicalPlan::Kv(KvOp::Scan {
        collection: session.collection.clone(),
        cursor,
        count,
        filters: filter_bytes,
        match_pattern,
    });

    match dispatch_kv(state, session, plan).await {
        Ok(resp) if resp.status == Status::Ok => {
            let json: serde_json::Value = serde_json::from_slice(&resp.payload).unwrap_or_default();

            let next_cursor = json
                .get("cursor")
                .and_then(|v| v.as_str())
                .unwrap_or("0")
                .to_string();

            let entries = json
                .get("entries")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default();

            let keys: Vec<RespValue> = entries
                .iter()
                .filter_map(|e| {
                    e.get("key").and_then(|k| k.as_str()).and_then(|b64| {
                        base64::Engine::decode(&base64::engine::general_purpose::STANDARD, b64)
                            .ok()
                            .map(RespValue::bulk)
                    })
                })
                .collect();

            RespValue::array(vec![
                RespValue::bulk_str(&next_cursor),
                RespValue::array(keys),
            ])
        }
        Ok(_) => RespValue::array(vec![RespValue::bulk_str("0"), RespValue::array(vec![])]),
        Err(e) => RespValue::err(format!("ERR {e}")),
    }
}

async fn handle_keys(cmd: &RespCommand, session: &RespSession, state: &SharedState) -> RespValue {
    let pattern = cmd.arg_str(0).unwrap_or("*");

    let plan = PhysicalPlan::Kv(KvOp::Scan {
        collection: session.collection.clone(),
        cursor: Vec::new(),
        count: 100_000,
        filters: Vec::new(),
        match_pattern: Some(pattern.to_string()),
    });

    match dispatch_kv(state, session, plan).await {
        Ok(resp) if resp.status == Status::Ok => {
            let json: serde_json::Value = serde_json::from_slice(&resp.payload).unwrap_or_default();
            let entries = json
                .get("entries")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default();

            let keys: Vec<RespValue> = entries
                .iter()
                .filter_map(|e| {
                    e.get("key").and_then(|k| k.as_str()).and_then(|b64| {
                        base64::Engine::decode(&base64::engine::general_purpose::STANDARD, b64)
                            .ok()
                            .map(RespValue::bulk)
                    })
                })
                .collect();

            RespValue::array(keys)
        }
        Ok(_) => RespValue::array(vec![]),
        Err(e) => RespValue::err(format!("ERR {e}")),
    }
}

// ---------------------------------------------------------------------------
// Info / stats
// ---------------------------------------------------------------------------

async fn handle_dbsize(session: &RespSession, state: &SharedState) -> RespValue {
    let plan = PhysicalPlan::Kv(KvOp::Scan {
        collection: session.collection.clone(),
        cursor: Vec::new(),
        count: 0,
        filters: Vec::new(),
        match_pattern: None,
    });

    match dispatch_kv(state, session, plan).await {
        Ok(resp) if resp.status == Status::Ok => {
            let json: serde_json::Value = serde_json::from_slice(&resp.payload).unwrap_or_default();
            let count = json.get("count").and_then(|v| v.as_i64()).unwrap_or(0);
            RespValue::integer(count)
        }
        _ => RespValue::integer(0),
    }
}

async fn handle_info(_cmd: &RespCommand, session: &RespSession, _state: &SharedState) -> RespValue {
    let info = format!(
        "# Server\r\nnodedb_version:0.1.0\r\n\r\n# Keyspace\r\ndb:{}\r\n",
        session.collection
    );
    RespValue::bulk(info.into_bytes())
}

// ---------------------------------------------------------------------------
// Dispatch helpers (used by handler_kv and handler_hash)
// ---------------------------------------------------------------------------

/// Dispatch a read-only KV operation to the Data Plane.
///
/// Bridge/dispatch errors are mapped to `Error::Bridge` with a "BUSY" detail
/// so the RESP handler can return `-BUSY` to the Redis client.
pub(super) async fn dispatch_kv(
    state: &SharedState,
    session: &RespSession,
    plan: PhysicalPlan,
) -> crate::Result<crate::bridge::envelope::Response> {
    let vshard = VShardId::from_collection(&session.collection);
    dispatch_utils::dispatch_to_data_plane(state, session.tenant_id, vshard, plan, 0)
        .await
        .map_err(map_busy_error)
}

/// Dispatch a KV write operation: WAL append first, then Data Plane.
pub(super) async fn dispatch_kv_write(
    state: &SharedState,
    session: &RespSession,
    plan: PhysicalPlan,
) -> crate::Result<crate::bridge::envelope::Response> {
    let vshard = VShardId::from_collection(&session.collection);
    wal_dispatch::wal_append_if_write(&state.wal, session.tenant_id, vshard, &plan)?;
    dispatch_utils::dispatch_to_data_plane(state, session.tenant_id, vshard, plan, 0)
        .await
        .map_err(map_busy_error)
}

/// Map bridge/dispatch errors to a BUSY error for Redis client compatibility.
///
/// When the SPSC ring buffer is full or the Data Plane core is overloaded,
/// the Redis client receives `-BUSY NodeDB is processing requests, retry later`
/// which Redis clients handle with automatic retry (same as Redis Cluster BUSY).
fn map_busy_error(e: crate::Error) -> crate::Error {
    match &e {
        crate::Error::Bridge { .. } | crate::Error::Dispatch { .. } => crate::Error::Bridge {
            detail: "BUSY NodeDB is processing requests, retry later".into(),
        },
        _ => e,
    }
}

/// Parse a JSON payload and extract an integer field.
pub(super) fn parse_json_field_i64(
    payload: &crate::bridge::envelope::Payload,
    field: &str,
) -> Option<i64> {
    let json: serde_json::Value = serde_json::from_slice(payload).ok()?;
    json.get(field)?.as_i64()
}
