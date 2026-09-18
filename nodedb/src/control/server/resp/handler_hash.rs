// SPDX-License-Identifier: BUSL-1.1

//! Hash field RESP command handlers: HGET, HMGET, HSET, FLUSHDB.

use sonic_rs;

use crate::bridge::envelope::{PhysicalPlan, Status};
use crate::control::state::SharedState;
use nodedb_physical::physical_plan::KvOp;
use nodedb_types::QualifiedCollection;

use super::codec::RespValue;
use super::command::RespCommand;
use super::handler::{dispatch_kv, dispatch_kv_write};
use super::payload::{payload_field_i64, payload_json};
use super::redaction::resp_redaction;
use super::session::RespSession;
use crate::control::server::response_shape::redaction::redact_envelope_row;

pub(super) async fn handle_hget(
    cmd: &RespCommand,
    session: &RespSession,
    state: &SharedState,
) -> RespValue {
    if cmd.argc() < 2 {
        return RespValue::err("ERR wrong number of arguments for 'hget' command");
    }

    let key = cmd.args[0].clone();
    let field = cmd.arg_str(1).unwrap_or("").to_string();

    // Resolved once for this command, before the dispatch whose field it covers.
    let redaction = resp_redaction(state, session);

    let plan = PhysicalPlan::Kv(KvOp::FieldGet {
        collection: QualifiedCollection::new(
            nodedb_types::DatabaseId::DEFAULT,
            &session.collection,
        ),
        key,
        fields: vec![field.clone()],
        rls_filters: Vec::new(),
    });

    match dispatch_kv(state, session, plan).await {
        Ok(resp) if resp.status == Status::Ok => {
            // A field-get payload is already a map keyed by the stored field
            // names the rules name, so it is redacted exactly as a result row.
            let mut json = payload_json(&resp.payload);
            redact_envelope_row(redaction.as_ref(), &state.redaction, &mut json);
            match json.get(&field) {
                Some(serde_json::Value::Null) | None => RespValue::nil(),
                Some(serde_json::Value::String(s)) => RespValue::bulk_str(s),
                Some(v) => RespValue::bulk(v.to_string().into_bytes()),
            }
        }
        Ok(_) => RespValue::nil(),
        Err(e) => RespValue::from_error(&e),
    }
}

pub(super) async fn handle_hmget(
    cmd: &RespCommand,
    session: &RespSession,
    state: &SharedState,
) -> RespValue {
    if cmd.argc() < 2 {
        return RespValue::err("ERR wrong number of arguments for 'hmget' command");
    }

    let key = cmd.args[0].clone();
    let fields: Vec<String> = cmd.args[1..]
        .iter()
        .filter_map(|a| std::str::from_utf8(a).ok().map(|s| s.to_string()))
        .collect();

    // Resolved once for the whole command, not once per requested field.
    let redaction = resp_redaction(state, session);

    let plan = PhysicalPlan::Kv(KvOp::FieldGet {
        collection: QualifiedCollection::new(
            nodedb_types::DatabaseId::DEFAULT,
            &session.collection,
        ),
        key,
        fields: fields.clone(),
        rls_filters: Vec::new(),
    });

    match dispatch_kv(state, session, plan).await {
        Ok(resp) if resp.status == Status::Ok => {
            // See `handle_hget`: the payload is a stored-field-keyed map.
            let mut json = payload_json(&resp.payload);
            redact_envelope_row(redaction.as_ref(), &state.redaction, &mut json);
            let items: Vec<RespValue> = fields
                .iter()
                .map(|f| match json.get(f) {
                    Some(serde_json::Value::Null) | None => RespValue::nil(),
                    Some(serde_json::Value::String(s)) => RespValue::bulk_str(s),
                    Some(v) => RespValue::bulk(v.to_string().into_bytes()),
                })
                .collect();
            RespValue::array(items)
        }
        Ok(_) => RespValue::nil_array(),
        Err(e) => RespValue::from_error(&e),
    }
}

pub(super) async fn handle_hset(
    cmd: &RespCommand,
    session: &RespSession,
    state: &SharedState,
) -> RespValue {
    if cmd.argc() < 3 || !(cmd.argc() - 1).is_multiple_of(2) {
        return RespValue::err("ERR wrong number of arguments for 'hset' command");
    }

    let key = cmd.args[0].clone();
    let updates: Vec<(String, Vec<u8>)> = cmd.args[1..]
        .chunks(2)
        .filter_map(|pair| {
            let field = std::str::from_utf8(&pair[0]).ok()?.to_string();
            let json_value =
                serde_json::Value::String(String::from_utf8_lossy(&pair[1]).into_owned());
            Some((field, sonic_rs::to_vec(&json_value).ok()?))
        })
        .collect();

    // Content-addressed cross-engine identity so the merged row keeps the
    // surrogate its original insert assigned.
    let surrogate = match state.surrogate_assigner.assign(
        nodedb_types::DatabaseId::DEFAULT,
        session.tenant_id,
        &session.collection,
        &key,
    ) {
        Ok(s) => s,
        Err(e) => return RespValue::from_error(&e),
    };

    let plan = PhysicalPlan::Kv(KvOp::FieldSet {
        collection: QualifiedCollection::new(
            nodedb_types::DatabaseId::DEFAULT,
            &session.collection,
        ),
        key,
        updates,
        surrogate,
        // Filled by the RLS injection pass `dispatch_kv_write` runs.
        rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
        // RESP has no RETURNING clause.
        returning: None,
        rls_filters: Vec::new(),
    });

    match dispatch_kv_write(state, session, plan).await {
        Ok(resp) if resp.status == Status::Ok => {
            let added = payload_field_i64(&resp.payload, "fields_added").unwrap_or(0);
            RespValue::integer(added)
        }
        Ok(_) => RespValue::integer(0),
        Err(e) => RespValue::from_error(&e),
    }
}

pub(super) async fn handle_flushdb(session: &RespSession, state: &SharedState) -> RespValue {
    let plan = PhysicalPlan::Kv(KvOp::Truncate {
        collection: QualifiedCollection::new(
            nodedb_types::DatabaseId::DEFAULT,
            &session.collection,
        ),
    });

    match dispatch_kv_write(state, session, plan).await {
        Ok(_) => RespValue::ok(),
        Err(e) => RespValue::from_error(&e),
    }
}
