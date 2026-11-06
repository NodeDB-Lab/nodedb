// SPDX-License-Identifier: BUSL-1.1

//! Native transport for `ArrayPutBatch`: decode the columnar cells and hand
//! them to the shared array bulk-load core, which checks, chunks and
//! dispatches them as `COPY <array> FROM STDIN` does. The frame cap bounds
//! one request.
//!
//! Outside a transaction block each chunk of the request commits on its
//! own, and a bad cell stops the request with the committed row count.
//! Inside a block the request stages whole or aborts the block.
//!
//! As for the edge batch opcodes, the tenant quota is checked first, and a
//! configured `max_batch_size` caps the cells of one request.

use nodedb_types::protocol::{ArrayPutBatch, NativeResponse, TextFields};

use crate::control::server::shared::ddl::DdlResult;
use crate::control::server::shared::ddl::neutral::collection::copy_from::{
    ArrayLoadCtx, ArrayLoader, LoadInput,
};
use crate::control::server::shared::session::DmlTxnCtx;

use super::batch_admission::{batch_size_refusal, decode_batch};
use super::{DispatchCtx, ddl_result_to_native};

/// The cells of `batch`: the length of its longest column. A batch whose
/// columns differ in length counts its longest, and the load refuses it.
fn cell_count(batch: &ArrayPutBatch) -> usize {
    batch
        .dims
        .iter()
        .chain(&batch.attrs)
        .map(Vec::len)
        .max()
        .unwrap_or(0)
}

/// Load the request's cells into the array `collection` names. Answers the
/// row count as a `COPY` status.
pub(crate) async fn handle_array_put_batch(
    ctx: &DispatchCtx<'_>,
    seq: u64,
    fields: &TextFields,
) -> NativeResponse {
    let (array, batch) = match decode_batch::<ArrayPutBatch>(ctx, "ArrayPutBatch", seq, fields) {
        Ok(decoded) => decoded,
        Err(response) => return *response,
    };
    if let Some(refusal) = batch_size_refusal(ctx, seq, "max_batch_size(cells)", cell_count(&batch))
    {
        return refusal;
    }
    let txn_ctx = DmlTxnCtx {
        sessions: ctx.sessions,
        session_id: ctx.peer_addr.into(),
    };
    let load = ArrayLoadCtx {
        state: ctx.state,
        identity: ctx.identity,
        database_id: ctx.database_id(),
        txn_ctx: &txn_ctx,
    };
    let _request = ctx.state.tenant_request_guard(ctx.tenant_id());
    let result = async {
        let mut loader = ArrayLoader::open(&load, array, LoadInput::Cells)?;
        loader.feed_cells(&load, &batch.dims, &batch.attrs).await?;
        loader.finish(&load).await
    }
    .await
    .map(|rows| {
        vec![DdlResult::Status {
            command: "COPY".to_string(),
            rows_affected: Some(rows),
        }]
    });
    ddl_result_to_native(seq, result)
}

#[cfg(test)]
mod tests {
    use nodedb_types::Value;

    use super::*;

    fn column(len: usize) -> Vec<Value> {
        vec![Value::Integer(0); len]
    }

    #[test]
    fn a_batch_counts_the_cells_of_its_columns() {
        let batch = ArrayPutBatch {
            dims: vec![column(3), column(3)],
            attrs: vec![column(3)],
        };
        assert_eq!(cell_count(&batch), 3);
    }

    #[test]
    fn a_short_first_column_cannot_hide_a_long_one_from_the_cap() {
        let batch = ArrayPutBatch {
            dims: vec![column(1)],
            attrs: vec![column(500)],
        };
        assert_eq!(cell_count(&batch), 500);
    }

    #[test]
    fn a_batch_without_columns_counts_no_cells() {
        let batch = ArrayPutBatch {
            dims: vec![],
            attrs: vec![],
        };
        assert_eq!(cell_count(&batch), 0);
    }
}
