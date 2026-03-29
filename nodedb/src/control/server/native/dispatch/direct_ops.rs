//! Direct Data Plane operation dispatch (PointGet, VectorSearch, Graph, etc.).

use nodedb_types::protocol::{NativeResponse, OpCode, TextFields};

use crate::bridge::envelope::{Response, Status};
use crate::data::executor::response_codec;

use super::super::super::dispatch_utils;
use super::{DispatchCtx, error_to_native};

/// Dispatch a direct Data Plane operation by opcode.
pub(crate) async fn handle_direct_op(
    ctx: &DispatchCtx<'_>,
    seq: u64,
    op: OpCode,
    fields: &TextFields,
) -> NativeResponse {
    let collection = fields
        .collection
        .as_deref()
        .unwrap_or("default")
        .to_lowercase();
    let vshard_key = fields.document_id.as_deref().unwrap_or(&collection);
    let vshard_id = ctx.vshard_for_key(vshard_key);
    let tenant_id = ctx.tenant_id();

    let mut plan = match super::plan_builder::build_plan(ctx, op, fields, &collection) {
        Ok(p) => p,
        Err(e) => return NativeResponse::error(seq, "42601", e.to_string()),
    };

    // Inject RLS filters from auth context (same as pgwire planner).
    if let Err(e) = crate::control::planner::rls_injection::inject_rls_for_single_plan(
        tenant_id.as_u32(),
        &mut plan,
        &ctx.state.rls,
        ctx.auth_context,
    ) {
        return NativeResponse::error(seq, "42501", e.to_string());
    }

    // WAL append for writes.
    if let Err(e) = dispatch_utils::wal_append_if_write(&ctx.state.wal, tenant_id, vshard_id, &plan)
    {
        return error_to_native(seq, &e);
    }

    match dispatch_utils::dispatch_to_data_plane(ctx.state, tenant_id, vshard_id, plan, 0).await {
        Ok(resp) => data_plane_response_to_native(seq, &resp),
        Err(e) => error_to_native(seq, &e),
    }
}

fn data_plane_response_to_native(seq: u64, resp: &Response) -> NativeResponse {
    if resp.status == Status::Error {
        let msg = if resp.payload.is_empty() {
            resp.error_code
                .as_ref()
                .map(|c| format!("{c:?}"))
                .unwrap_or_else(|| "unknown error".into())
        } else {
            String::from_utf8_lossy(&resp.payload).into_owned()
        };
        return NativeResponse::error(seq, "XX000", msg);
    }

    if resp.payload.is_empty() {
        let mut r = NativeResponse::ok(seq);
        r.watermark_lsn = resp.watermark_lsn.as_u64();
        return r;
    }

    let json_text = response_codec::decode_payload_to_json(&resp.payload);
    let (columns, rows) = super::parse_json_to_columns_rows(&json_text);
    NativeResponse {
        seq,
        status: nodedb_types::protocol::ResponseStatus::Ok,
        columns: Some(columns),
        rows: Some(rows),
        rows_affected: None,
        watermark_lsn: resp.watermark_lsn.as_u64(),
        error: None,
        auth: None,
    }
}
