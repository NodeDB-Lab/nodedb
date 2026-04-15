//! RESP command handlers: translate Redis commands into KvOp dispatches.

use sonic_rs;

use crate::bridge::envelope::{PhysicalPlan, Status};
use crate::bridge::physical_plan::KvOp;
use crate::control::state::SharedState;

use super::codec::RespValue;
use super::command::RespCommand;
// Re-export for sub-handlers that import via `super::handler::dispatch_kv` etc.
pub(super) use super::gateway_dispatch::{dispatch_kv, dispatch_kv_write, parse_json_field_i64};
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
        "INCR" => super::handler_kv::handle_incr(cmd, session, state, 1).await,
        "DECR" => super::handler_kv::handle_incr(cmd, session, state, -1).await,
        "INCRBY" => super::handler_kv::handle_incrby(cmd, session, state).await,
        "DECRBY" => super::handler_kv::handle_decrby(cmd, session, state).await,
        "INCRBYFLOAT" => super::handler_kv::handle_incrbyfloat(cmd, session, state).await,
        "GETSET" => super::handler_kv::handle_getset(cmd, session, state).await,
        "ZADD" => super::handler_sorted::handle_zadd(cmd, session, state).await,
        "ZREM" => super::handler_sorted::handle_zrem(cmd, session, state).await,
        "ZRANK" => super::handler_sorted::handle_zrank(cmd, session, state).await,
        "ZRANGE" => super::handler_sorted::handle_zrange(cmd, session, state).await,
        "ZCARD" => super::handler_sorted::handle_zcard(session, state).await,
        "ZSCORE" => super::handler_sorted::handle_zscore(cmd, session, state).await,
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
        "AUTH" => handle_auth(cmd, session, state),
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

/// AUTH [username] password
///
/// Redis supports two forms:
/// - `AUTH password` — authenticates with default username "admin"
/// - `AUTH username password` — authenticates with explicit username
///
/// On success, updates `session.tenant_id` from the authenticated identity.
fn handle_auth(cmd: &RespCommand, session: &mut RespSession, state: &SharedState) -> RespValue {
    let (username, password) = match cmd.argc() {
        1 => ("admin", cmd.arg_str(0).unwrap_or("")),
        2 => (
            cmd.arg_str(0).unwrap_or("admin"),
            cmd.arg_str(1).unwrap_or(""),
        ),
        _ => return RespValue::err("ERR wrong number of arguments for 'auth' command"),
    };

    // Validate credentials using the same path as native/pgwire auth.
    state.credentials.check_lockout(username).ok();

    if !state.credentials.verify_password(username, password) {
        state.credentials.record_login_failure(username);
        state.auth_metrics.record_auth_failure("resp_password");
        return RespValue::err("WRONGPASS invalid username-password pair");
    }

    state.credentials.record_login_success(username);

    // Resolve identity to get tenant_id.
    match state.credentials.to_identity(
        username,
        crate::control::security::identity::AuthMethod::CleartextPassword,
    ) {
        Some(identity) => {
            session.tenant_id = identity.tenant_id;
            state.auth_metrics.record_auth_success("resp_password");
            RespValue::ok()
        }
        None => RespValue::err("ERR user not found after authentication"),
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

    let plan = PhysicalPlan::Kv(KvOp::GetTtl {
        collection: session.collection.clone(),
        key: key.to_vec(),
    });

    match dispatch_kv(state, session, plan).await {
        Ok(resp) if resp.status == Status::Ok => {
            let ttl_ms = parse_json_field_i64(&resp.payload, "ttl_ms").unwrap_or(-2);
            if ttl_ms < 0 {
                // -1 (no TTL) or -2 (not found) — same for both TTL and PTTL.
                RespValue::integer(ttl_ms)
            } else if is_pttl {
                RespValue::integer(ttl_ms)
            } else {
                // TTL returns seconds, round up to avoid reporting 0 for sub-second TTLs.
                RespValue::integer((ttl_ms + 999) / 1000)
            }
        }
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
                    match nodedb_types::json_to_msgpack(&scan_filter) {
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
            // KV scan returns a flat msgpack array of entry maps.
            let json: serde_json::Value = match sonic_rs::from_slice(&resp.payload) {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!(error = %e, "RESP SCAN: failed to decode KV scan payload");
                    return RespValue::err(format!("ERR scan decode failed: {e}"));
                }
            };

            let entries = match json {
                serde_json::Value::Array(arr) => arr,
                _ => Vec::new(),
            };

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

            // Cursor "0" signals scan complete (no pagination in this path).
            RespValue::array(vec![RespValue::bulk_str("0"), RespValue::array(keys)])
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
            let json: serde_json::Value = match sonic_rs::from_slice(&resp.payload) {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!(error = %e, "RESP KEYS: failed to decode KV scan payload");
                    return RespValue::err(format!("ERR keys decode failed: {e}"));
                }
            };
            let entries = match json {
                serde_json::Value::Array(arr) => arr,
                _ => Vec::new(),
            };

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
            let json: serde_json::Value = match sonic_rs::from_slice(&resp.payload) {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!(error = %e, "RESP DBSIZE: failed to decode KV scan payload");
                    return RespValue::err(format!("ERR dbsize decode failed: {e}"));
                }
            };
            let count = match &json {
                serde_json::Value::Array(arr) => arr.len() as i64,
                _ => 0,
            };
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
