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
        "GET" => handle_get(cmd, session, state).await,
        "SET" => handle_set(cmd, session, state).await,
        "DEL" => handle_del(cmd, session, state).await,
        "EXISTS" => handle_exists(cmd, session, state).await,
        "MGET" => handle_mget(cmd, session, state).await,
        "MSET" => handle_mset(cmd, session, state).await,
        "EXPIRE" => handle_expire(cmd, session, state, false).await,
        "PEXPIRE" => handle_expire(cmd, session, state, true).await,
        "TTL" => handle_ttl(cmd, session, state, false).await,
        "PTTL" => handle_ttl(cmd, session, state, true).await,
        "PERSIST" => handle_persist(cmd, session, state).await,
        "SCAN" => handle_scan(cmd, session, state).await,
        "KEYS" => handle_keys(cmd, session, state).await,
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
// Core KV commands
// ---------------------------------------------------------------------------

async fn handle_get(cmd: &RespCommand, session: &RespSession, state: &SharedState) -> RespValue {
    let Some(key) = cmd.arg(0) else {
        return RespValue::err("ERR wrong number of arguments for 'get' command");
    };

    let plan = PhysicalPlan::Kv(KvOp::Get {
        collection: session.collection.clone(),
        key: key.to_vec(),
    });

    match dispatch_kv(state, session, plan).await {
        Ok(resp) if resp.status == Status::Ok && !resp.payload.is_empty() => {
            RespValue::bulk(resp.payload.to_vec())
        }
        Ok(_) => RespValue::nil(),
        Err(e) => RespValue::err(format!("ERR {e}")),
    }
}

async fn handle_set(cmd: &RespCommand, session: &RespSession, state: &SharedState) -> RespValue {
    if cmd.argc() < 2 {
        return RespValue::err("ERR wrong number of arguments for 'set' command");
    }

    let key = cmd.args[0].clone();
    let value = cmd.args[1].clone();

    // Parse optional flags: EX, PX, NX, XX.
    let mut ttl_ms: u64 = 0;
    let mut nx = false;
    let mut xx = false;
    let mut i = 2;
    while i < cmd.argc() {
        match cmd.arg_str(i).map(|s| s.to_uppercase()) {
            Some(ref flag) if flag == "EX" => {
                if let Some(secs) = cmd.arg_i64(i + 1) {
                    ttl_ms = (secs as u64) * 1000;
                    i += 2;
                } else {
                    return RespValue::err("ERR value is not an integer or out of range");
                }
            }
            Some(ref flag) if flag == "PX" => {
                if let Some(ms) = cmd.arg_i64(i + 1) {
                    ttl_ms = ms as u64;
                    i += 2;
                } else {
                    return RespValue::err("ERR value is not an integer or out of range");
                }
            }
            Some(ref flag) if flag == "NX" => {
                nx = true;
                i += 1;
            }
            Some(ref flag) if flag == "XX" => {
                xx = true;
                i += 1;
            }
            _ => {
                return RespValue::err(format!(
                    "ERR syntax error at '{}'",
                    cmd.arg_str(i).unwrap_or("?")
                ));
            }
        }
    }

    // NX/XX conditional write: check existence first.
    if nx || xx {
        let check = PhysicalPlan::Kv(KvOp::Get {
            collection: session.collection.clone(),
            key: key.clone(),
        });
        match dispatch_kv(state, session, check).await {
            Ok(resp) => {
                let exists = resp.status == Status::Ok && !resp.payload.is_empty();
                if nx && exists {
                    return RespValue::nil(); // NX: key already exists.
                }
                if xx && !exists {
                    return RespValue::nil(); // XX: key doesn't exist.
                }
            }
            Err(e) => return RespValue::err(format!("ERR {e}")),
        }
    }

    let plan = PhysicalPlan::Kv(KvOp::Put {
        collection: session.collection.clone(),
        key,
        value,
        ttl_ms,
    });

    match dispatch_kv_write(state, session, plan).await {
        Ok(_) => RespValue::ok(),
        Err(e) => RespValue::err(format!("ERR {e}")),
    }
}

async fn handle_del(cmd: &RespCommand, session: &RespSession, state: &SharedState) -> RespValue {
    if cmd.argc() < 1 {
        return RespValue::err("ERR wrong number of arguments for 'del' command");
    }

    let keys: Vec<Vec<u8>> = cmd.args.clone();
    let plan = PhysicalPlan::Kv(KvOp::Delete {
        collection: session.collection.clone(),
        keys,
    });

    match dispatch_kv_write(state, session, plan).await {
        Ok(resp) => {
            // Parse {"deleted": N} from payload.
            let count = parse_json_field_i64(&resp.payload, "deleted").unwrap_or(0);
            RespValue::integer(count)
        }
        Err(e) => RespValue::err(format!("ERR {e}")),
    }
}

async fn handle_exists(cmd: &RespCommand, session: &RespSession, state: &SharedState) -> RespValue {
    if cmd.argc() < 1 {
        return RespValue::err("ERR wrong number of arguments for 'exists' command");
    }

    let mut count = 0i64;
    for key in &cmd.args {
        let plan = PhysicalPlan::Kv(KvOp::Get {
            collection: session.collection.clone(),
            key: key.clone(),
        });
        if let Ok(resp) = dispatch_kv(state, session, plan).await
            && resp.status == Status::Ok
            && !resp.payload.is_empty()
        {
            count += 1;
        }
    }

    RespValue::integer(count)
}

async fn handle_mget(cmd: &RespCommand, session: &RespSession, state: &SharedState) -> RespValue {
    if cmd.argc() < 1 {
        return RespValue::err("ERR wrong number of arguments for 'mget' command");
    }

    let plan = PhysicalPlan::Kv(KvOp::BatchGet {
        collection: session.collection.clone(),
        keys: cmd.args.clone(),
    });

    match dispatch_kv(state, session, plan).await {
        Ok(resp) if resp.status == Status::Ok => {
            // Parse JSON array of base64-encoded values.
            let values: Vec<serde_json::Value> =
                serde_json::from_slice(&resp.payload).unwrap_or_default();
            let items: Vec<RespValue> = values
                .into_iter()
                .map(|v| match v {
                    serde_json::Value::String(b64) => {
                        match base64::Engine::decode(
                            &base64::engine::general_purpose::STANDARD,
                            &b64,
                        ) {
                            Ok(data) => RespValue::bulk(data),
                            Err(_) => RespValue::nil(),
                        }
                    }
                    _ => RespValue::nil(),
                })
                .collect();
            RespValue::array(items)
        }
        Ok(_) => RespValue::nil_array(),
        Err(e) => RespValue::err(format!("ERR {e}")),
    }
}

async fn handle_mset(cmd: &RespCommand, session: &RespSession, state: &SharedState) -> RespValue {
    if cmd.argc() < 2 || !cmd.argc().is_multiple_of(2) {
        return RespValue::err("ERR wrong number of arguments for 'mset' command");
    }

    let entries: Vec<(Vec<u8>, Vec<u8>)> = cmd
        .args
        .chunks(2)
        .map(|pair| (pair[0].clone(), pair[1].clone()))
        .collect();

    let plan = PhysicalPlan::Kv(KvOp::BatchPut {
        collection: session.collection.clone(),
        entries,
        ttl_ms: 0,
    });

    match dispatch_kv_write(state, session, plan).await {
        Ok(_) => RespValue::ok(),
        Err(e) => RespValue::err(format!("ERR {e}")),
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
        Ok(_) => RespValue::integer(0), // Key not found.
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
    // Note: the full TTL query would need the entry's expire_at_ms, which
    // is not currently returned by KvOp::Get. For now, we return -1 (no TTL)
    // if the key exists, -2 if it doesn't. Full TTL reporting needs a dedicated
    // KvOp variant (future enhancement).
    let plan = PhysicalPlan::Kv(KvOp::Get {
        collection: session.collection.clone(),
        key: key.to_vec(),
    });

    match dispatch_kv(state, session, plan).await {
        Ok(resp) if resp.status == Status::Ok && !resp.payload.is_empty() => {
            // Key exists. We don't have the expire_at_ms in the response,
            // so report -1 (no TTL) as a safe default.
            RespValue::integer(-1)
        }
        Ok(_) => RespValue::integer(-2), // Key doesn't exist.
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
// SCAN
// ---------------------------------------------------------------------------

async fn handle_scan(cmd: &RespCommand, session: &RespSession, state: &SharedState) -> RespValue {
    let cursor_str = cmd.arg_str(0).unwrap_or("0");
    let cursor = if cursor_str == "0" {
        Vec::new()
    } else {
        base64::Engine::decode(&base64::engine::general_purpose::STANDARD, cursor_str)
            .unwrap_or_default()
    };

    // Parse MATCH and COUNT options.
    let mut match_pattern: Option<String> = None;
    let mut count: usize = 10;
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
            _ => {
                i += 1;
            }
        }
    }

    let plan = PhysicalPlan::Kv(KvOp::Scan {
        collection: session.collection.clone(),
        cursor,
        count,
        filters: Vec::new(),
        match_pattern,
    });

    match dispatch_kv(state, session, plan).await {
        Ok(resp) if resp.status == Status::Ok => {
            // Parse JSON response: {"cursor": "...", "entries": [{"key": "...", "value": "..."}]}
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

            // SCAN returns: [cursor, [key1, key2, ...]]
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

    // KEYS is a full scan — warn-worthy for large datasets.
    let plan = PhysicalPlan::Kv(KvOp::Scan {
        collection: session.collection.clone(),
        cursor: Vec::new(),
        count: 100_000, // Large limit — KEYS scans everything.
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
    // Use SCAN with count=0 to get metadata — or dispatch a custom stats request.
    // For now, return 0 as a placeholder (full stats need a dedicated KvOp).
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
// Dispatch helpers
// ---------------------------------------------------------------------------

/// Dispatch a read-only KV operation to the Data Plane.
async fn dispatch_kv(
    state: &SharedState,
    session: &RespSession,
    plan: PhysicalPlan,
) -> crate::Result<crate::bridge::envelope::Response> {
    let vshard = VShardId::from_collection(&session.collection);
    dispatch_utils::dispatch_to_data_plane(state, session.tenant_id, vshard, plan, 0).await
}

/// Dispatch a KV write operation: WAL append first, then Data Plane.
async fn dispatch_kv_write(
    state: &SharedState,
    session: &RespSession,
    plan: PhysicalPlan,
) -> crate::Result<crate::bridge::envelope::Response> {
    let vshard = VShardId::from_collection(&session.collection);

    // WAL append before execution (durability guarantee).
    wal_dispatch::wal_append_if_write(&state.wal, session.tenant_id, vshard, &plan)?;

    dispatch_utils::dispatch_to_data_plane(state, session.tenant_id, vshard, plan, 0).await
}

/// Parse a JSON payload and extract an integer field.
fn parse_json_field_i64(payload: &crate::bridge::envelope::Payload, field: &str) -> Option<i64> {
    let json: serde_json::Value = serde_json::from_slice(payload).ok()?;
    json.get(field)?.as_i64()
}
