// SPDX-License-Identifier: BUSL-1.1

//! Atomic counter commands: INCR, DECR, INCRBY, DECRBY, INCRBYFLOAT.

use crate::bridge::envelope::PhysicalPlan;
use crate::control::state::SharedState;
use nodedb_physical::physical_plan::{KvCounterShape, KvOp};
use nodedb_types::{DatabaseId, QualifiedCollection};

use super::super::codec::RespValue;
use super::super::command::RespCommand;
use super::super::handler::dispatch_kv_write;
use super::super::payload::{payload_field_i64, payload_json};
use super::super::redaction::resp_redaction;
use super::super::session::RespSession;
use super::surrogate::resp_kv_surrogate;

/// Refuse a counter command whose result a redaction rule covers.
///
/// `INCR` / `DECR` / `INCRBY` / `DECRBY` / `INCRBYFLOAT` answer with the row's
/// new stored value, which the KV engine holds as the single-value form — the
/// column every SQL-side read of that row calls `value`. Masking the answer
/// would report a number the key does not hold, so the command is refused
/// instead, on the same fail-closed principle the planner applies to an
/// aggregate over a redacted column. The refusal happens BEFORE dispatch, so
/// the increment the caller could not observe is never performed either.
fn refuse_if_counter_is_redacted(state: &SharedState, session: &RespSession) -> Option<RespValue> {
    let redaction = resp_redaction(state, session)?;
    redaction
        .field_has_rule(&state.redaction, "value")
        .then(|| RespValue::err("ERR the counter value is redacted for this role"))
}

/// INCR key / DECR key — increment/decrement by 1.
///
/// `default_delta` is +1 for INCR, -1 for DECR.
pub(in crate::control::server::resp) async fn handle_incr(
    cmd: &RespCommand,
    session: &RespSession,
    state: &SharedState,
    default_delta: i64,
) -> RespValue {
    let Some(key) = cmd.arg(0) else {
        return RespValue::err("ERR wrong number of arguments for 'incr' command");
    };
    dispatch_incr(state, session, key.to_vec(), default_delta).await
}

/// INCRBY key delta
pub(in crate::control::server::resp) async fn handle_incrby(
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
    dispatch_incr(state, session, key, delta).await
}

/// DECRBY key delta
pub(in crate::control::server::resp) async fn handle_decrby(
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
    dispatch_incr(state, session, key, neg_delta).await
}

/// Shared body for every integer counter command.
async fn dispatch_incr(
    state: &SharedState,
    session: &RespSession,
    key: Vec<u8>,
    delta: i64,
) -> RespValue {
    if let Some(refusal) = refuse_if_counter_is_redacted(state, session) {
        return refusal;
    }
    let surrogate = match resp_kv_surrogate(state, session, &key) {
        Ok(s) => s,
        Err(e) => return e,
    };
    let plan = PhysicalPlan::Kv(KvOp::Incr {
        collection: QualifiedCollection::new(DatabaseId::DEFAULT, &session.collection),
        key,
        delta,
        ttl_ms: 0,
        surrogate,
        // Filled by the RLS injection pass `dispatch_kv_write` runs.
        rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
        // RESP treats every value as a byte string, as `SET` and `GET` do, so
        // an absent key starts as decimal text in any collection.
        shape: KvCounterShape::Raw,
    });

    match dispatch_counter(state, session, plan).await {
        Ok(resp) => match payload_field_i64(&resp.payload, "value") {
            Some(new_val) => RespValue::integer(new_val),
            // The counter did change; a response we cannot read means we do
            // not know its new value, and echoing 0 would report a value the
            // key does not hold.
            None => RespValue::err("ERR counter response could not be decoded"),
        },
        Err(e) => RespValue::from_error(&e),
    }
}

/// INCRBYFLOAT key delta
pub(in crate::control::server::resp) async fn handle_incrbyfloat(
    cmd: &RespCommand,
    session: &RespSession,
    state: &SharedState,
) -> RespValue {
    if cmd.argc() < 2 {
        return RespValue::err("ERR wrong number of arguments for 'incrbyfloat' command");
    }

    let key = cmd.args[0].clone();
    // The delta stays the client's decimal text, so the engine adds every
    // digit the client sent.
    let delta = match cmd.arg_str(1) {
        Some(s) if nodedb_physical::kv_atomic::float_text::is_decimal_number(s) => s.to_string(),
        _ => return RespValue::err("ERR value is not a valid float"),
    };

    if let Some(refusal) = refuse_if_counter_is_redacted(state, session) {
        return refusal;
    }

    let surrogate = match resp_kv_surrogate(state, session, &key) {
        Ok(s) => s,
        Err(e) => return e,
    };
    let plan = PhysicalPlan::Kv(KvOp::IncrFloat {
        collection: QualifiedCollection::new(DatabaseId::DEFAULT, &session.collection),
        key,
        delta,
        surrogate,
        // Filled by the RLS injection pass `dispatch_kv_write` runs.
        rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
        // See `dispatch_incr`.
        shape: KvCounterShape::Raw,
    });

    match dispatch_counter(state, session, plan).await {
        // The reply is a bulk string. A raw body answers with the exact text
        // it stored, the bytes `GET` returns. A typed row answers with the
        // column's new number.
        Ok(resp) => {
            let reply = payload_json(&resp.payload);
            let text = match reply.get("text").and_then(serde_json::Value::as_str) {
                Some(text) => Some(text.to_string()),
                None => reply
                    .get("value")
                    .and_then(serde_json::Value::as_f64)
                    .map(|v| v.to_string()),
            };
            match text {
                Some(text) => RespValue::bulk(text.into_bytes()),
                None => RespValue::err("ERR counter response could not be decoded"),
            }
        }
        Err(e) => RespValue::from_error(&e),
    }
}

/// Dispatch a counter write and turn a Data-Plane error status into a typed
/// error, so a counter fault reaches the client as its own message.
async fn dispatch_counter(
    state: &SharedState,
    session: &RespSession,
    plan: PhysicalPlan,
) -> crate::Result<crate::bridge::envelope::Response> {
    let resp = dispatch_kv_write(state, session, plan).await?;
    crate::control::server::dispatch_utils::reject_data_plane_error(&resp)?;
    Ok(resp)
}
