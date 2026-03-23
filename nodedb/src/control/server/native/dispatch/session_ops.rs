//! Session parameter operations: SET, SHOW, RESET (opcode form).

use nodedb_types::protocol::NativeResponse;
use nodedb_types::value::Value;

use super::DispatchCtx;

pub(crate) fn handle_set(
    ctx: &DispatchCtx<'_>,
    seq: u64,
    key: &str,
    value: &str,
) -> NativeResponse {
    ctx.sessions
        .set_parameter(ctx.peer_addr, key.to_lowercase(), value.to_string());
    NativeResponse::status_row(seq, "SET")
}

pub(crate) fn handle_show(ctx: &DispatchCtx<'_>, seq: u64, key: &str) -> NativeResponse {
    let value = ctx
        .sessions
        .get_parameter(ctx.peer_addr, &key.to_lowercase())
        .unwrap_or_default();
    NativeResponse {
        seq,
        status: nodedb_types::protocol::ResponseStatus::Ok,
        columns: Some(vec!["setting".into()]),
        rows: Some(vec![vec![Value::String(value)]]),
        rows_affected: None,
        watermark_lsn: 0,
        error: None,
        auth: None,
    }
}

pub(crate) fn handle_reset(ctx: &DispatchCtx<'_>, seq: u64, key: &str) -> NativeResponse {
    ctx.sessions
        .set_parameter(ctx.peer_addr, key.to_lowercase(), String::new());
    NativeResponse::status_row(seq, "RESET")
}
