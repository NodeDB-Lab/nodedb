// SPDX-License-Identifier: BUSL-1.1

//! Native transport for `EdgePutBatch` / `EdgeDeleteBatch`: decode the
//! edges and hand them to the same handlers `GRAPH INSERT EDGE` and
//! `GRAPH DELETE EDGE` run. A request is one unit, as one statement is: the
//! same edge cap, write grant and per-edge write policy apply, and the
//! answer counts the edges written. As for the single-edge opcodes, the
//! collection name is lower-cased and the tenant quota is checked first. A
//! configured `max_batch_size` caps the edges of one request, as it caps
//! the vectors and documents of the other batch opcodes.

use nodedb_types::protocol::{EdgeDeleteBatch, EdgePutBatch, NativeResponse, TextFields};

use crate::control::server::native::sqlstate_code::sqlstate_error;
use crate::control::server::shared::ddl::neutral::graph_ops::{edge_delete, edge_insert};
use crate::control::server::shared::session::DmlTxnCtx;

use super::{DispatchCtx, ddl_result_to_native, error_to_native, error_to_native_with_sqlstate};

/// Insert the request's edges into the collection it names.
pub(crate) async fn handle_edge_put_batch(
    ctx: &DispatchCtx<'_>,
    seq: u64,
    fields: &TextFields,
) -> NativeResponse {
    let (collection, batch) = match decode::<EdgePutBatch>(ctx, "EdgePutBatch", seq, fields) {
        Ok(decoded) => decoded,
        Err(response) => return *response,
    };
    if let Some(refusal) = batch_size_refusal(ctx, seq, batch.edges.len()) {
        return refusal;
    }
    let txn_ctx = DmlTxnCtx {
        sessions: ctx.sessions,
        session_id: ctx.peer_addr.into(),
    };
    let _request = ctx.state.tenant_request_guard(ctx.tenant_id());
    let result = match edge_insert::edge_inserts_from_writes(batch.edges) {
        Ok(edges) => {
            edge_insert::insert_edges(
                ctx.state,
                ctx.identity,
                ctx.database_id(),
                collection,
                edges,
                &txn_ctx,
            )
            .await
        }
        Err(e) => Err(e),
    };
    ddl_result_to_native(seq, result)
}

/// Delete the request's edges from the collection it names.
pub(crate) async fn handle_edge_delete_batch(
    ctx: &DispatchCtx<'_>,
    seq: u64,
    fields: &TextFields,
) -> NativeResponse {
    let (collection, batch) = match decode::<EdgeDeleteBatch>(ctx, "EdgeDeleteBatch", seq, fields) {
        Ok(decoded) => decoded,
        Err(response) => return *response,
    };
    if let Some(refusal) = batch_size_refusal(ctx, seq, batch.edges.len()) {
        return refusal;
    }
    let txn_ctx = DmlTxnCtx {
        sessions: ctx.sessions,
        session_id: ctx.peer_addr.into(),
    };
    let _request = ctx.state.tenant_request_guard(ctx.tenant_id());
    let result = edge_delete::delete_edges(
        ctx.state,
        ctx.identity,
        ctx.database_id(),
        collection,
        batch.edges.into_iter().map(Into::into).collect(),
        &txn_ctx,
    )
    .await;
    ddl_result_to_native(seq, result)
}

/// The request's lower-cased `collection` and its `payload` decoded as `B`,
/// after the tenant quota check. A missing field, a refused quota or an
/// undecodable payload is the boxed error response to send.
fn decode<B>(
    ctx: &DispatchCtx<'_>,
    op: &str,
    seq: u64,
    fields: &TextFields,
) -> Result<(String, B), Box<NativeResponse>>
where
    B: for<'a> zerompk::FromMessagePack<'a>,
{
    let Some(collection) = fields.collection.as_deref() else {
        return Err(Box::new(sqlstate_error(
            seq,
            "42601",
            format!("{op}: missing 'collection' field"),
        )));
    };
    let Some(payload) = fields.payload.as_deref() else {
        return Err(Box::new(sqlstate_error(
            seq,
            "42601",
            format!("{op}: missing 'payload' field"),
        )));
    };
    if let Err(e) = ctx.state.check_tenant_quota(ctx.tenant_id()) {
        return Err(Box::new(error_to_native(seq, &e)));
    }
    let batch = zerompk::from_msgpack(payload).map_err(|e| {
        Box::new(sqlstate_error(
            seq,
            "22P02",
            format!("{op}: payload is not an {op} payload: {e}"),
        ))
    })?;
    Ok((collection.to_lowercase(), batch))
}

/// The refusal for a request of `count` edges over the configured
/// `max_batch_size`, with the error the other batch opcodes return. `None`
/// when no cap is configured or `count` is within it.
fn batch_size_refusal(ctx: &DispatchCtx<'_>, seq: u64, count: usize) -> Option<NativeResponse> {
    let max = u64::from(ctx.state.limits.max_batch_size?);
    let value = count as u64;
    if value <= max {
        return None;
    }
    let error = crate::Error::LimitExceeded {
        limit_name: "max_batch_size(edges)",
        value,
        max,
    };
    Some(error_to_native_with_sqlstate(seq, "0A000", &error))
}
