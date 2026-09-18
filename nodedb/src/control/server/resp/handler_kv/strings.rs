// SPDX-License-Identifier: BUSL-1.1

//! Single-key string commands: GET, SET, DEL, EXISTS, GETSET.

use crate::bridge::envelope::{PhysicalPlan, Status};
use crate::control::state::SharedState;
use nodedb_physical::physical_plan::KvOp;
use nodedb_types::{DatabaseId, QualifiedCollection};

use super::super::codec::RespValue;
use super::super::command::RespCommand;
use super::super::handler::{dispatch_kv, dispatch_kv_write};
use super::super::payload::{payload_field_i64, payload_json};
use super::super::redaction::resp_redaction;
use super::super::session::RespSession;
use super::surrogate::resp_kv_surrogate;
use crate::control::server::response_shape::redaction::redact_stored_value_bytes;

pub(in crate::control::server::resp) async fn handle_get(
    cmd: &RespCommand,
    session: &RespSession,
    state: &SharedState,
) -> RespValue {
    let Some(key) = cmd.arg(0) else {
        return RespValue::err("ERR wrong number of arguments for 'get' command");
    };

    // Resolved once for this command, before the dispatch whose value it covers.
    let redaction = resp_redaction(state, session);

    let plan = PhysicalPlan::Kv(KvOp::Get {
        collection: QualifiedCollection::new(DatabaseId::DEFAULT, &session.collection),
        key: key.to_vec(),
        rls_filters: Vec::new(),
        surrogate_ceiling: None,
    });

    match dispatch_kv(state, session, plan).await {
        Ok(resp) if resp.status == Status::Ok && !resp.payload.is_empty() => {
            // The Data Plane returns the stored bytes verbatim (`kv get` is a
            // raw passthrough), so the masking hook is applied to those bytes
            // rather than to a decoded row map.
            let mut value = resp.payload.to_vec();
            redact_stored_value_bytes(redaction.as_ref(), &state.redaction, &mut value);
            if value.is_empty() {
                RespValue::nil()
            } else {
                RespValue::bulk(value)
            }
        }
        Ok(_) => RespValue::nil(),
        Err(e) => RespValue::from_error(&e),
    }
}

pub(in crate::control::server::resp) async fn handle_set(
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
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, &session.collection),
            key: key.clone(),
            rls_filters: Vec::new(),
            surrogate_ceiling: None,
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
            Err(e) => return RespValue::from_error(&e),
        }
    }

    let surrogate = match resp_kv_surrogate(state, session, &key) {
        Ok(s) => s,
        Err(e) => return e,
    };
    let plan = PhysicalPlan::Kv(KvOp::Put {
        collection: QualifiedCollection::new(DatabaseId::DEFAULT, &session.collection),
        key,
        value,
        ttl_ms,
        surrogate,
        returning: None,
        rls_filters: Vec::new(),
    });

    match dispatch_kv_write(state, session, plan).await {
        Ok(_) => RespValue::ok(),
        Err(e) => RespValue::from_error(&e),
    }
}

pub(in crate::control::server::resp) async fn handle_del(
    cmd: &RespCommand,
    session: &RespSession,
    state: &SharedState,
) -> RespValue {
    if cmd.argc() < 1 {
        return RespValue::err("ERR wrong number of arguments for 'del' command");
    }

    let keys: Vec<Vec<u8>> = cmd.args.clone();
    let plan = PhysicalPlan::Kv(KvOp::Delete {
        collection: QualifiedCollection::new(DatabaseId::DEFAULT, &session.collection),
        keys,
        // Filled by the RLS injection pass `dispatch_kv_write` runs.
        rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
        // RESP has no RETURNING clause.
        returning: None,
        rls_filters: Vec::new(),
    });

    match dispatch_kv_write(state, session, plan).await {
        Ok(resp) => {
            let count = payload_field_i64(&resp.payload, "deleted").unwrap_or(0);
            RespValue::integer(count)
        }
        Err(e) => RespValue::from_error(&e),
    }
}

pub(in crate::control::server::resp) async fn handle_exists(
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
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, &session.collection),
            key: key.clone(),
            rls_filters: Vec::new(),
            surrogate_ceiling: None,
        });
        match dispatch_kv(state, session, plan).await {
            Ok(resp) if resp.status == Status::Ok && !resp.payload.is_empty() => count += 1,
            Ok(_) => {}
            // A policy refusal is not an absent key: reporting it as one would
            // let EXISTS answer a question the caller is not allowed to ask.
            Err(e) => return RespValue::from_error(&e),
        }
    }

    RespValue::integer(count)
}

/// GETSET key value — atomically set new value and return old.
pub(in crate::control::server::resp) async fn handle_getset(
    cmd: &RespCommand,
    session: &RespSession,
    state: &SharedState,
) -> RespValue {
    if cmd.argc() < 2 {
        return RespValue::err("ERR wrong number of arguments for 'getset' command");
    }

    let key = cmd.args[0].clone();
    let new_value = cmd.args[1].clone();

    // Resolved once for this command: GETSET returns the row's PREVIOUS stored
    // value, which carries exactly the disclosure a GET of it would.
    let redaction = resp_redaction(state, session);

    let surrogate = match resp_kv_surrogate(state, session, &key) {
        Ok(s) => s,
        Err(e) => return e,
    };
    let plan = PhysicalPlan::Kv(KvOp::GetSet {
        collection: QualifiedCollection::new(DatabaseId::DEFAULT, &session.collection),
        key,
        new_value,
        surrogate,
        // Both filled by the RLS injection pass `dispatch_kv_write` runs: the
        // read half gates the old value this returns, the write half the value
        // it stores.
        rls_filters: Vec::new(),
        rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
    });

    match dispatch_kv_write(state, session, plan).await {
        Ok(resp) => {
            if let Some(serde_json::Value::String(b64)) =
                payload_json(&resp.payload).get("old_value")
                && let Ok(mut data) =
                    base64::Engine::decode(&base64::engine::general_purpose::STANDARD, b64)
            {
                // `old_value` is the stored bytes, base64-framed for transport
                // only — redaction applies to the bytes inside the frame. An
                // already-empty previous value still answers as it always did;
                // only a value a rule emptied degrades to nil.
                let was_empty = data.is_empty();
                redact_stored_value_bytes(redaction.as_ref(), &state.redaction, &mut data);
                if was_empty || !data.is_empty() {
                    return RespValue::bulk(data);
                }
            }
            RespValue::nil()
        }
        Err(e) => RespValue::from_error(&e),
    }
}
