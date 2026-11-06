// SPDX-License-Identifier: BUSL-1.1

//! Chunk dispatch and the refusals of an array bulk load.

use nodedb_mem::MemError;
use nodedb_sql::SqlError;
use nodedb_sql::catalog::SqlCatalog;
use nodedb_sql::engine_rules::{BulkLoadParams, BulkLoadPlan};
use nodedb_wal::record::MAX_WAL_PAYLOAD_SIZE;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::ddl::neutral::collection::dml::{
    PlanBuilder, plan_and_dispatch_built,
};
use crate::control::server::shared::ddl::result::DdlError;
use crate::control::server::shared::session::DmlTxnCtx;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::super::entry::wrap_row_error;
use super::chunker::Chunk;
use super::decode::RowError;

/// Upper bound of the redo bytes one chunk's sub-record adds beyond its
/// cells and the array name: the record type, the version byte, the
/// payload's array headers, the array id's ids, and the provenance.
const REDO_CHUNK_OVERHEAD: usize = 256;

/// Who loads, into which database, in which transaction scope.
pub(crate) struct ArrayLoadCtx<'a> {
    pub state: &'a SharedState,
    pub identity: &'a AuthenticatedIdentity,
    pub database_id: DatabaseId,
    pub txn_ctx: &'a DmlTxnCtx<'a>,
}

/// Dispatch one chunk through the write funnel.
pub(super) async fn dispatch_chunk(
    ctx: &ArrayLoadCtx<'_>,
    array: &str,
    chunk: Chunk,
    label: &str,
    committed: u64,
) -> Result<(), DdlError> {
    let Chunk {
        rows, first_row, ..
    } = chunk;
    let collection = array.to_string();
    let build: PlanBuilder<'_> = Box::new(move |catalog: &dyn SqlCatalog| {
        let params = BulkLoadParams {
            collection: collection.clone(),
            cells: rows,
        };
        match nodedb_sql::planner::dml::plan_bulk_load(params, catalog)? {
            BulkLoadPlan::CellChunks(plans) => Ok(plans),
            BulkLoadPlan::RowInserts => Err(SqlError::Unsupported {
                detail: format!("COPY: '{collection}' is no longer an array"),
            }),
        }
    });
    plan_and_dispatch_built(
        ctx.state,
        ctx.identity,
        ctx.identity.tenant_id,
        ctx.database_id,
        build,
        ctx.txn_ctx,
        true,
    )
    .await
    .map_err(|e| refuse(e, first_row, label, committed))?;
    Ok(())
}

/// The `54000` refusal of an incomplete record of `len` bytes that the memory
/// governor refuses to hold.
pub(super) fn record_over_cap(
    len: usize,
    refusal: &MemError,
    row: usize,
    label: &str,
    committed: u64,
) -> DdlError {
    let e = DdlError::new(
        "54000",
        format!(
            "{len} bytes hold no record end, and the memory governor refuses to hold them: \
             {refusal}; check the input's line breaks and quotes"
        ),
    );
    refuse(e, row, label, committed)
}

/// Inside a block, every staged chunk's redo joins the one commit record.
/// Refuse the chunk that would take the staged redo of `chunks` chunks and
/// `cell_bytes` cell bytes past the WAL record cap, before it stages.
pub(super) fn check_redo_cap(
    array: &str,
    rows: u64,
    cell_bytes: usize,
    chunks: usize,
) -> Result<(), DdlError> {
    let redo = cell_bytes + chunks * (REDO_CHUNK_OVERHEAD + array.len());
    if redo <= MAX_WAL_PAYLOAD_SIZE {
        return Ok(());
    }
    Err(DdlError::new(
        "54000",
        format!(
            "COPY: {rows} rows need up to {redo} bytes of transaction redo, over the \
             {MAX_WAL_PAYLOAD_SIZE}-byte WAL record cap; load them outside a transaction \
             block, where each chunk commits on its own (0 rows committed)"
        ),
    ))
}

/// A refused row. Row 0 is the input as a whole, such as a bad delimiter,
/// so its message names no row.
pub(super) fn refuse_row(e: RowError, label: &str, committed: u64) -> DdlError {
    if e.row == 0 {
        return DdlError::new(
            e.sqlstate,
            format!("COPY: {label}: {} ({committed} rows committed)", e.message),
        );
    }
    let row = e.row;
    refuse(DdlError::new(e.sqlstate, e.message), row, label, committed)
}

/// `COPY: <label> row <n>: <message> (<committed> rows committed)`.
pub(super) fn refuse(e: DdlError, row: usize, label: &str, committed: u64) -> DdlError {
    let mut e = wrap_row_error(e, row, label);
    e.message = format!("{} ({committed} rows committed)", e.message);
    e
}

#[cfg(test)]
mod tests {
    use super::super::chunker::ARRAY_COPY_CHUNK_BYTES as CHUNK;
    use super::*;

    #[test]
    fn a_small_load_fits_the_redo_cap() {
        assert!(check_redo_cap("scene", 10, 10 * 1024, 1).is_ok());
    }

    #[test]
    fn a_load_past_the_redo_cap_is_refused_with_54000() {
        let chunks = MAX_WAL_PAYLOAD_SIZE / CHUNK + 1;
        let err =
            check_redo_cap("scene", 1_000_000, MAX_WAL_PAYLOAD_SIZE, chunks).expect_err("over");
        assert_eq!(err.sqlstate, "54000");
    }

    #[test]
    fn a_row_refusal_names_the_row_and_the_committed_count() {
        let e = refuse_row(RowError::new(1000, "22P02", "bad value".into()), "CSV", 65);
        assert_eq!(e.sqlstate, "22P02");
        assert_eq!(
            e.message,
            "COPY: CSV row 1000: bad value (65 rows committed)"
        );
    }

    #[test]
    fn an_endless_record_is_refused_with_54000() {
        let refusal = MemError::BudgetExhausted {
            engine: nodedb_mem::EngineId::Array,
            requested: 300,
            available: 200,
            limit: 1000,
        };
        let e = record_over_cap(300, &refusal, 7, "CSV", 0);
        assert_eq!(e.sqlstate, "54000");
        assert!(
            e.message.starts_with("COPY: CSV row 7: 300 bytes"),
            "{}",
            e.message
        );
        assert!(e.message.contains("limit: 1000 bytes"), "{}", e.message);
    }
}
