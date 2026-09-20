// SPDX-License-Identifier: BUSL-1.1

//! Session parameter operations: SET, SHOW, RESET (opcode form).
//!
//! The native protocol applies the same session-parameter contract pgwire
//! applies: the shared allowlist, the shared per-parameter value grammar, and
//! an explicit refusal for identity keys this protocol does not carry. A name
//! or value one protocol refuses and the other stores would give a client two
//! different servers.
//!
//! The SQL form of these commands (`handle_set_sql`, `handle_show_sql`) parses
//! the statement and then calls the same functions here.

use nodedb_types::error::sqlstate;
use nodedb_types::protocol::NativeResponse;
use nodedb_types::value::Value;

use crate::control::server::native::sqlstate_code::sqlstate_error;
use crate::control::server::shared::session::{
    validate_reset_parameter, validate_set_parameter, validate_show_parameter,
};

use super::DispatchCtx;

/// Store one session parameter after the shared contract accepts it.
pub(crate) fn handle_set(
    ctx: &DispatchCtx<'_>,
    seq: u64,
    key: &str,
    value: &str,
) -> NativeResponse {
    let key = key.to_lowercase();
    if let Some(refusal) = identity_key_refusal(seq, &key, "set") {
        return refusal;
    }
    if let Err(error) = validate_set_parameter(&key, value) {
        return sqlstate_error(seq, error.sqlstate(), error.to_string());
    }
    ctx.sessions
        .set_parameter(ctx.peer_addr, key, value.to_string());
    NativeResponse::status_row(seq, "SET")
}

/// Read one session parameter back.
///
/// A name the session never set resolves to the empty setting only when the
/// server carries that parameter. Any other name is reported as unknown, so a
/// client never reads a blank row as a real value.
pub(crate) fn handle_show(ctx: &DispatchCtx<'_>, seq: u64, key: &str) -> NativeResponse {
    let key = key.to_lowercase();
    let value = match ctx.sessions.get_parameter(ctx.peer_addr, &key) {
        Some(value) => value,
        None => match validate_show_parameter(&key) {
            Ok(()) => String::new(),
            Err(error) => return sqlstate_error(seq, error.sqlstate(), error.to_string()),
        },
    };
    setting_row(seq, value)
}

/// Restore one session parameter to its connection default.
pub(crate) fn handle_reset(ctx: &DispatchCtx<'_>, seq: u64, key: &str) -> NativeResponse {
    let key = key.to_lowercase();
    if key == "all" {
        ctx.sessions.reset_all_parameters(ctx.peer_addr);
        return NativeResponse::status_row(seq, "RESET");
    }
    if let Some(refusal) = identity_key_refusal(seq, &key, "reset") {
        return refusal;
    }
    if let Err(error) = validate_reset_parameter(&key) {
        return sqlstate_error(seq, error.sqlstate(), error.to_string());
    }
    ctx.sessions.reset_parameter(ctx.peer_addr, &key);
    NativeResponse::status_row(seq, "RESET")
}

/// Every session parameter, one row per name.
pub(crate) fn show_all(ctx: &DispatchCtx<'_>, seq: u64) -> NativeResponse {
    let rows = ctx
        .sessions
        .all_parameters(ctx.peer_addr)
        .into_iter()
        .map(|(name, value)| vec![Value::String(name), Value::String(value)])
        .collect();
    NativeResponse {
        seq,
        status: nodedb_types::protocol::ResponseStatus::Ok,
        columns: Some(vec!["name".into(), "setting".into()]),
        rows: Some(rows),
        rows_affected: None,
        command: None,
        watermark_lsn: 0,
        error: None,
        auth: None,
        warnings: Vec::new(),
    }
}

/// One `setting` column holding `value`.
fn setting_row(seq: u64, value: String) -> NativeResponse {
    NativeResponse {
        seq,
        status: nodedb_types::protocol::ResponseStatus::Ok,
        columns: Some(vec!["setting".into()]),
        rows: Some(vec![vec![Value::String(value)]]),
        rows_affected: None,
        command: None,
        watermark_lsn: 0,
        error: None,
        auth: None,
        warnings: Vec::new(),
    }
}

/// The refusal for an identity or security key, or `None` for every other
/// name.
///
/// These keys are on the shared allowlist because pgwire claims them in its
/// own dispatch branches and enforces them there. The native protocol binds
/// identity at connect time and reads none of them back out of the parameter
/// bag, so storing one would report success for a switch that never happens.
/// `verb` names the command the client sent.
///
/// Each refusal states what to do instead, because a client that sent one of
/// these wanted an identity change and gets no other signal that it did not
/// happen.
fn identity_key_refusal(seq: u64, key: &str, verb: &str) -> Option<NativeResponse> {
    let detail = match key {
        "tenant" | "nodedb.tenant_id" => {
            "the native protocol binds a session's tenant at connect time. \
             Reconnect against the target tenant."
        }
        "role" => {
            "a session's role set is identity-bound at CREATE USER time. Use \
             GRANT/REVOKE ROLE TO <user> to change a user's roles, or reconnect \
             with a different user."
        }
        "session_authorization" => {
            "identity is bound at connection time. Reconnect as the target user."
        }
        "nodedb.auth_session" => {
            "pooled session handles are a pgwire path. A native connection \
             authenticates with its own Auth frame."
        }
        _ => return None,
    };
    Some(sqlstate_error(
        seq,
        sqlstate::FEATURE_NOT_SUPPORTED,
        format!("cannot {verb} {key}: {detail}"),
    ))
}
