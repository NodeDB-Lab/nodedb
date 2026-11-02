// SPDX-License-Identifier: BUSL-1.1

//! Multi-key commands: MGET, MSET.

use crate::bridge::envelope::{PhysicalPlan, Status};
use crate::control::state::SharedState;
use nodedb_physical::physical_plan::KvOp;
use nodedb_types::{DatabaseId, QualifiedCollection};

use super::super::codec::RespValue;
use super::super::command::RespCommand;
use super::super::handler::{dispatch_kv, dispatch_kv_write};
use super::super::payload::payload_json;
use super::super::redaction::resp_redaction;
use super::super::session::RespSession;
use super::surrogate::resp_kv_surrogates;
use crate::control::server::response_shape::redaction::redact_stored_value_bytes;

pub(in crate::control::server::resp) async fn handle_mget(
    cmd: &RespCommand,
    session: &RespSession,
    state: &SharedState,
) -> RespValue {
    if cmd.argc() < 1 {
        return RespValue::err("ERR wrong number of arguments for 'mget' command");
    }

    // Resolved once for the whole command, not once per returned value.
    let redaction = match resp_redaction(state, session) {
        Ok(redaction) => redaction,
        Err(e) => return RespValue::from_error(&e),
    };

    let plan = PhysicalPlan::Kv(KvOp::BatchGet {
        collection: QualifiedCollection::new(DatabaseId::DEFAULT, &session.collection),
        keys: cmd.args.clone(),
        rls_filters: Vec::new(),
        system_time: nodedb_types::SystemTimeScope::Current,
    });

    match dispatch_kv(state, session, plan).await {
        Ok(resp) if resp.status == Status::Ok => {
            let values = match payload_json(&resp.payload) {
                serde_json::Value::Array(values) => values,
                _ => Vec::new(),
            };
            let items: Vec<RespValue> = values
                .into_iter()
                .map(|v| match v {
                    serde_json::Value::String(b64) => {
                        match base64::Engine::decode(
                            &base64::engine::general_purpose::STANDARD,
                            &b64,
                        ) {
                            // Each entry is one row's stored bytes, base64-framed
                            // for transport only — the same masking GET applies
                            // to a single row applies to each of these.
                            Ok(mut data) => {
                                let was_empty = data.is_empty();
                                redact_stored_value_bytes(
                                    redaction.as_ref(),
                                    &state.redaction,
                                    &mut data,
                                );
                                if was_empty || !data.is_empty() {
                                    RespValue::bulk(data)
                                } else {
                                    RespValue::nil()
                                }
                            }
                            Err(_) => RespValue::nil(),
                        }
                    }
                    _ => RespValue::nil(),
                })
                .collect();
            RespValue::array(items)
        }
        Ok(_) => RespValue::nil_array(),
        Err(e) => RespValue::from_error(&e),
    }
}

pub(in crate::control::server::resp) async fn handle_mset(
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

    // Every entry's stable cross-engine surrogate, resolved in one batch the
    // way SET (`handle_set`) resolves one key. A row left at
    // `Surrogate::ZERO` is invisible to any surrogate-keyed cross-engine read
    // or join.
    let keys: Vec<&[u8]> = entries.iter().map(|(key, _value)| key.as_slice()).collect();
    let surrogates = match resp_kv_surrogates(state, session, &keys).await {
        Ok(s) => s,
        Err(e) => return e,
    };

    let plan = PhysicalPlan::Kv(KvOp::BatchPut {
        collection: QualifiedCollection::new(DatabaseId::DEFAULT, &session.collection),
        entries,
        ttl_ms: 0,
        intent: nodedb_physical::physical_plan::KvBatchIntent::Put,
        surrogates,
        returning: None,
        rls_filters: Vec::new(),
    });

    match dispatch_kv_write(state, session, plan).await {
        Ok(_) => RespValue::ok(),
        Err(e) => RespValue::from_error(&e),
    }
}
