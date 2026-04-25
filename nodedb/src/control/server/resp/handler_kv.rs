//! Core KV RESP command handlers: GET, SET, DEL, EXISTS, MGET, MSET, INCR, DECR, GETSET.

use sonic_rs;

use crate::bridge::envelope::{PhysicalPlan, Status};
use crate::bridge::physical_plan::KvOp;
use crate::control::state::SharedState;

use super::codec::RespValue;
use super::command::RespCommand;
use super::handler::{dispatch_kv, dispatch_kv_write, parse_json_field_i64};
use super::session::RespSession;

pub(super) async fn handle_get(
    cmd: &RespCommand,
    session: &RespSession,
    state: &SharedState,
) -> RespValue {
    let Some(key) = cmd.arg(0) else {
        return RespValue::err("ERR wrong number of arguments for 'get' command");
    };

    let plan = PhysicalPlan::Kv(KvOp::Get {
        collection: session.collection.clone(),
        key: key.to_vec(),
        rls_filters: Vec::new(),
    });

    match dispatch_kv(state, session, plan).await {
        Ok(resp) if resp.status == Status::Ok && !resp.payload.is_empty() => {
            RespValue::bulk(resp.payload.to_vec())
        }
        Ok(_) => RespValue::nil(),
        Err(e) => RespValue::err(format!("ERR {e}")),
    }
}

pub(super) async fn handle_set(
    cmd: &RespCommand,
    session: &RespSession,
    state: &SharedState,
) -> RespValue {
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
            rls_filters: Vec::new(),
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

    let surrogate = match state.surrogate_assigner.assign(&session.collection, &key) {
        Ok(s) => s,
        Err(e) => return RespValue::err(format!("ERR {e}")),
    };
    let plan = PhysicalPlan::Kv(KvOp::Put {
        collection: session.collection.clone(),
        key,
        value,
        ttl_ms,
        surrogate,
    });

    match dispatch_kv_write(state, session, plan).await {
        Ok(_) => RespValue::ok(),
        Err(e) => RespValue::err(format!("ERR {e}")),
    }
}

pub(super) async fn handle_del(
    cmd: &RespCommand,
    session: &RespSession,
    state: &SharedState,
) -> RespValue {
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
            let count = parse_json_field_i64(&resp.payload, "deleted").unwrap_or(0);
            RespValue::integer(count)
        }
        Err(e) => RespValue::err(format!("ERR {e}")),
    }
}

pub(super) async fn handle_exists(
    cmd: &RespCommand,
    session: &RespSession,
    state: &SharedState,
) -> RespValue {
    if cmd.argc() < 1 {
        return RespValue::err("ERR wrong number of arguments for 'exists' command");
    }

    let mut count = 0i64;
    for key in &cmd.args {
        let plan = PhysicalPlan::Kv(KvOp::Get {
            collection: session.collection.clone(),
            key: key.clone(),
            rls_filters: Vec::new(),
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

pub(super) async fn handle_mget(
    cmd: &RespCommand,
    session: &RespSession,
    state: &SharedState,
) -> RespValue {
    if cmd.argc() < 1 {
        return RespValue::err("ERR wrong number of arguments for 'mget' command");
    }

    let plan = PhysicalPlan::Kv(KvOp::BatchGet {
        collection: session.collection.clone(),
        keys: cmd.args.clone(),
    });

    match dispatch_kv(state, session, plan).await {
        Ok(resp) if resp.status == Status::Ok => {
            let values: Vec<serde_json::Value> =
                sonic_rs::from_slice(&resp.payload).unwrap_or_default();
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

pub(super) async fn handle_mset(
    cmd: &RespCommand,
    session: &RespSession,
    state: &SharedState,
) -> RespValue {
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

/// INCR key / DECR key — increment/decrement by 1.
///
/// `default_delta` is +1 for INCR, -1 for DECR.
pub(super) async fn handle_incr(
    cmd: &RespCommand,
    session: &RespSession,
    state: &SharedState,
    default_delta: i64,
) -> RespValue {
    let Some(key) = cmd.arg(0) else {
        return RespValue::err("ERR wrong number of arguments for 'incr' command");
    };

    let plan = PhysicalPlan::Kv(KvOp::Incr {
        collection: session.collection.clone(),
        key: key.to_vec(),
        delta: default_delta,
        ttl_ms: 0,
    });

    match dispatch_kv_write(state, session, plan).await {
        Ok(resp) => {
            let new_val = parse_json_field_i64(&resp.payload, "value").unwrap_or(0);
            RespValue::integer(new_val)
        }
        Err(e) => RespValue::err(format!("ERR {e}")),
    }
}

/// INCRBY key delta
pub(super) async fn handle_incrby(
    cmd: &RespCommand,
    session: &RespSession,
    state: &SharedState,
) -> RespValue {
    if cmd.argc() < 2 {
        return RespValue::err("ERR wrong number of arguments for 'incrby' command");
    }

    let key = cmd.args[0].clone();
    let Some(delta) = cmd.arg_i64(1) else {
        return RespValue::err("ERR value is not an integer or out of range");
    };

    let plan = PhysicalPlan::Kv(KvOp::Incr {
        collection: session.collection.clone(),
        key,
        delta,
        ttl_ms: 0,
    });

    match dispatch_kv_write(state, session, plan).await {
        Ok(resp) => {
            let new_val = parse_json_field_i64(&resp.payload, "value").unwrap_or(0);
            RespValue::integer(new_val)
        }
        Err(e) => RespValue::err(format!("ERR {e}")),
    }
}

/// DECRBY key delta
pub(super) async fn handle_decrby(
    cmd: &RespCommand,
    session: &RespSession,
    state: &SharedState,
) -> RespValue {
    if cmd.argc() < 2 {
        return RespValue::err("ERR wrong number of arguments for 'decrby' command");
    }

    let key = cmd.args[0].clone();
    let Some(delta) = cmd.arg_i64(1) else {
        return RespValue::err("ERR value is not an integer or out of range");
    };
    let Some(neg_delta) = delta.checked_neg() else {
        return RespValue::err("ERR value is not an integer or out of range");
    };

    let plan = PhysicalPlan::Kv(KvOp::Incr {
        collection: session.collection.clone(),
        key,
        delta: neg_delta,
        ttl_ms: 0,
    });

    match dispatch_kv_write(state, session, plan).await {
        Ok(resp) => {
            let new_val = parse_json_field_i64(&resp.payload, "value").unwrap_or(0);
            RespValue::integer(new_val)
        }
        Err(e) => RespValue::err(format!("ERR {e}")),
    }
}

/// INCRBYFLOAT key delta
pub(super) async fn handle_incrbyfloat(
    cmd: &RespCommand,
    session: &RespSession,
    state: &SharedState,
) -> RespValue {
    if cmd.argc() < 2 {
        return RespValue::err("ERR wrong number of arguments for 'incrbyfloat' command");
    }

    let key = cmd.args[0].clone();
    let delta_str = match cmd.arg_str(1) {
        Some(s) => s,
        None => return RespValue::err("ERR value is not a valid float"),
    };
    let delta: f64 = match delta_str.parse() {
        Ok(v) => v,
        Err(_) => return RespValue::err("ERR value is not a valid float"),
    };

    let plan = PhysicalPlan::Kv(KvOp::IncrFloat {
        collection: session.collection.clone(),
        key,
        delta,
    });

    match dispatch_kv_write(state, session, plan).await {
        Ok(resp) => {
            // Return the new value as a bulk string (Redis convention).
            let payload_text =
                crate::data::executor::response_codec::decode_payload_to_json(&resp.payload);
            if let Ok(json) = sonic_rs::from_str::<serde_json::Value>(&payload_text)
                && let Some(v) = json.get("value")
            {
                return RespValue::bulk(v.to_string().into_bytes());
            }
            RespValue::bulk(payload_text.into_bytes())
        }
        Err(e) => RespValue::err(format!("ERR {e}")),
    }
}

/// GETSET key value — atomically set new value and return old.
pub(super) async fn handle_getset(
    cmd: &RespCommand,
    session: &RespSession,
    state: &SharedState,
) -> RespValue {
    if cmd.argc() < 2 {
        return RespValue::err("ERR wrong number of arguments for 'getset' command");
    }

    let key = cmd.args[0].clone();
    let new_value = cmd.args[1].clone();

    let plan = PhysicalPlan::Kv(KvOp::GetSet {
        collection: session.collection.clone(),
        key,
        new_value,
    });

    match dispatch_kv_write(state, session, plan).await {
        Ok(resp) => {
            let payload_text =
                crate::data::executor::response_codec::decode_payload_to_json(&resp.payload);
            if let Ok(json) = sonic_rs::from_str::<serde_json::Value>(&payload_text)
                && let Some(serde_json::Value::String(b64)) = json.get("old_value")
                && let Ok(data) =
                    base64::Engine::decode(&base64::engine::general_purpose::STANDARD, b64)
            {
                return RespValue::bulk(data);
            }
            RespValue::nil()
        }
        Err(e) => RespValue::err(format!("ERR {e}")),
    }
}
