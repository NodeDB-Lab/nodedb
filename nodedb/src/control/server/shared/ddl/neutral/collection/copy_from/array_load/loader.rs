// SPDX-License-Identifier: BUSL-1.1

//! The shared core of an array bulk load. pgwire `COPY <array> FROM STDIN`,
//! `COPY <array> FROM '<path>'` (CSV or GeoTIFF) and the native
//! `ArrayPutBatch` opcode are thin transports over it.
//!
//! A load streams. Each slice of input decodes and checks row by row, and
//! each full chunk dispatches through the write funnel and is awaited
//! before the next row reads. A load holds one incomplete record and a few
//! chunks, whatever the input size.
//!
//! Outside a transaction block each chunk commits on its own, as each row of
//! a document `COPY` does. A bad row or a refused chunk stops the load, and
//! the chunks before it stay committed. The refusal names the row and the
//! committed row count. Inside a block every chunk stages into the
//! transaction, and a refusal aborts the block, so the load commits or rolls
//! back whole. A chunk that takes the staged redo past the WAL record cap is
//! refused with `54000` before it stages.

use std::sync::Arc;

use nodedb_mem::{EngineId, ScopedMemory};
use nodedb_sql::catalog::ArrayCatalogView;
use nodedb_sql::types_array::ArrayInsertRow;
use nodedb_types::Value;

use crate::control::planner::context::{BulkLoadRoute, QueryContext};
use crate::control::server::shared::ddl::neutral::collection::dml::authorize_write_target;
use crate::control::server::shared::ddl::result::DdlError;
use crate::control::trigger::statement_txn::in_block;

use super::super::entry::bulk_load_refusal;
use super::chunker::{ARRAY_COPY_CHUNK_BYTES, ArrayChunker, Chunk};
use super::decode::{RowError, wire_bound};
use super::dispatch::{
    ArrayLoadCtx, check_redo_cap, dispatch_chunk, record_over_cap, refuse, refuse_row,
};
use super::records::{RecordBoundary, check_delimiter};
use super::router::CellRouter;
use super::source::{RowSource, TextFormat, TextSlice};
use super::tail::RecordTail;

/// What a load reads its rows from.
pub(crate) enum LoadInput {
    /// A COPY stream, fed with [`ArrayLoader::feed_text`].
    Text(TextFormat),
    /// Native request columns, fed with [`ArrayLoader::feed_cells`].
    Cells,
    /// Decoded GeoTIFF pixels, fed with [`ArrayLoader::feed_rows`].
    Raster,
}

/// One open array bulk load. Errors carry their SQLSTATE: `22P02` for a
/// wrong column count or an unparsable value, `23502` for a NULL in a NOT
/// NULL column, `22003` for a coordinate outside its dim domain, `42703`
/// for a header that does not name the schema's columns, `54000` for an
/// incomplete record the memory governor refuses to hold or a block's redo
/// over the WAL record cap, and the Data Plane's own class, such as `53200`
/// under memory pressure. A row error names the row and how many rows
/// committed.
pub(crate) struct ArrayLoader {
    target: LoadTarget,
    progress: LoadProgress,
    text: Option<TextStream>,
}

/// The array a load writes.
struct LoadTarget {
    array: String,
    view: ArrayCatalogView,
    /// Whether the load runs inside a transaction block.
    in_txn: bool,
    label: &'static str,
}

/// The rows a load has read, routed, chunked and dispatched.
struct LoadProgress {
    /// How each row routes. It keeps the last tile key across rows.
    router: CellRouter,
    chunker: ArrayChunker,
    /// Rows read and checked.
    rows: u64,
    /// Rows of the committed chunks. Outside a block only.
    committed: u64,
    /// Cell bytes and chunks staged into the block's transaction.
    staged_bytes: usize,
    staged_chunks: usize,
}

/// A COPY stream's carried state between slices.
struct TextStream {
    format: TextFormat,
    boundary: RecordBoundary,
    /// The bytes of the record the last slice left incomplete.
    tail: RecordTail,
    /// The 1-based number of the next record.
    next_record_no: usize,
    header_pending: bool,
    /// Whether the stream met the `\.` end-of-data line.
    ended: bool,
}

impl ArrayLoader {
    /// Open a load into `array`: check the write privilege and resolve the
    /// array and its routing.
    pub(crate) fn open(
        ctx: &ArrayLoadCtx<'_>,
        array: &str,
        input: LoadInput,
    ) -> Result<Self, DdlError> {
        let tenant_id = ctx.identity.tenant_id;
        authorize_write_target(ctx.state, ctx.identity, ctx.database_id, array)?;
        let view = match QueryContext::for_state(ctx.state)
            .bulk_load_route(array, tenant_id, ctx.database_id)
            .map_err(|e| bulk_load_refusal(e, tenant_id))?
        {
            BulkLoadRoute::ArrayCells(view) => *view,
            BulkLoadRoute::RowInserts => {
                return Err(DdlError::new(
                    "42P01",
                    format!("COPY: array '{array}' does not exist"),
                ));
            }
        };
        let router = CellRouter::for_array(ctx, array)?;
        let (label, text) = match input {
            LoadInput::Text(format) => {
                check_delimiter(format.delimiter())
                    .map_err(|e| refuse_row(e, format.label(), 0))?;
                let stream = TextStream {
                    format,
                    boundary: RecordBoundary::new(format.format, format.delimiter()),
                    tail: RecordTail::new(ScopedMemory::new(
                        Arc::clone(&ctx.state.governor),
                        ctx.database_id,
                        tenant_id,
                        EngineId::Array,
                    )),
                    next_record_no: 1,
                    header_pending: format.header,
                    ended: false,
                };
                (format.label(), Some(stream))
            }
            LoadInput::Cells => ("batch", None),
            LoadInput::Raster => ("GeoTIFF", None),
        };
        Ok(Self {
            target: LoadTarget {
                array: array.to_string(),
                view,
                in_txn: in_block(ctx.txn_ctx),
                label,
            },
            progress: LoadProgress {
                router,
                chunker: ArrayChunker::new(ARRAY_COPY_CHUNK_BYTES),
                rows: 0,
                committed: 0,
                staged_bytes: 0,
                staged_chunks: 0,
            },
            text,
        })
    }

    /// Load the whole records of the next slice of a COPY stream. A record
    /// the slice leaves incomplete carries to the next slice.
    pub(crate) async fn feed_text(
        &mut self,
        ctx: &ArrayLoadCtx<'_>,
        bytes: &[u8],
    ) -> Result<(), DdlError> {
        let Self {
            target,
            progress,
            text,
        } = self;
        let stream = text
            .as_mut()
            .ok_or_else(|| DdlError::internal("COPY: text input fed to a cell load"))?;
        if stream.ended {
            return Ok(());
        }
        let mut rest = bytes;
        if !stream.tail.is_empty() {
            let Some(end) = stream.boundary.scan(rest, true) else {
                return stream.carry(rest, target.label, progress.committed);
            };
            stream.carry(&rest[..end], target.label, progress.committed)?;
            rest = &rest[end..];
            let carried = stream.tail.take();
            progress.take_records(ctx, target, stream, &carried).await?;
            stream.tail.reuse(carried);
            if stream.ended {
                return Ok(());
            }
        }
        let left = match stream.boundary.scan(rest, false) {
            Some(end) => {
                progress
                    .take_records(ctx, target, stream, &rest[..end])
                    .await?;
                if stream.ended {
                    return Ok(());
                }
                &rest[end..]
            }
            None => rest,
        };
        stream.carry(left, target.label, progress.committed)
    }

    /// Load one native request's columns: one per dim, then one per attr,
    /// in schema order, all of one length.
    pub(crate) async fn feed_cells(
        &mut self,
        ctx: &ArrayLoadCtx<'_>,
        dims: &[Vec<Value>],
        attrs: &[Vec<Value>],
    ) -> Result<(), DdlError> {
        let Self {
            target, progress, ..
        } = self;
        let mut source = RowSource::cells(dims, attrs, &target.view)
            .map_err(|e| refuse_row(e, target.label, progress.committed))?;
        progress.take_source(ctx, target, &mut source).await
    }

    /// Load checked rows in input order. Each item is a row and its 1-based
    /// number, or the refusal of that row.
    pub(super) async fn feed_rows<I>(
        &mut self,
        ctx: &ArrayLoadCtx<'_>,
        rows: I,
    ) -> Result<(), DdlError>
    where
        I: Iterator<Item = Result<(usize, ArrayInsertRow), RowError>>,
    {
        let Self {
            target, progress, ..
        } = self;
        for next in rows {
            let (row_no, row) =
                next.map_err(|e| refuse_row(e, target.label, progress.committed))?;
            progress.take_row(ctx, target, row_no, row).await?;
        }
        Ok(())
    }

    /// The array the load writes.
    pub(crate) fn view(&self) -> &ArrayCatalogView {
        &self.target.view
    }

    /// `e` as this load's refusal, naming its input and committed row count.
    pub(super) fn refuse(&self, e: RowError) -> DdlError {
        refuse_row(e, self.target.label, self.progress.committed)
    }

    /// Load the stream's last record and every chunk still pending. Returns
    /// the number of rows loaded.
    pub(crate) async fn finish(mut self, ctx: &ArrayLoadCtx<'_>) -> Result<u64, DdlError> {
        let Self {
            target,
            progress,
            text,
        } = &mut self;
        if let Some(stream) = text.as_mut()
            && !stream.ended
            && !stream.tail.is_empty()
        {
            let carried = stream.tail.take();
            progress.take_records(ctx, target, stream, &carried).await?;
        }
        for chunk in progress.chunker.finish() {
            progress.dispatch(ctx, target, chunk).await?;
        }
        Ok(progress.rows)
    }
}

impl TextStream {
    /// Carry `bytes` of an incomplete record to the next slice. A record the
    /// memory governor refuses to hold stops the load.
    fn carry(&mut self, bytes: &[u8], label: &str, committed: u64) -> Result<(), DdlError> {
        self.tail.extend(bytes).map_err(|refusal| {
            record_over_cap(
                self.tail.len().saturating_add(bytes.len()),
                &refusal,
                self.next_record_no,
                label,
                committed,
            )
        })
    }

    /// The 1-based number of the record that holds byte `offset` of a run
    /// of records starting at the next record.
    fn record_at(&self, bytes: &[u8], offset: usize) -> usize {
        let mut boundary = RecordBoundary::new(self.format.format, self.format.delimiter());
        let mut before = &bytes[..offset.min(bytes.len())];
        let mut row = self.next_record_no;
        while let Some(end) = boundary.scan(before, true) {
            row += 1;
            before = &before[end..];
        }
        row
    }
}

impl LoadProgress {
    /// Load a run of whole records of the stream.
    async fn take_records(
        &mut self,
        ctx: &ArrayLoadCtx<'_>,
        target: &LoadTarget,
        stream: &mut TextStream,
        bytes: &[u8],
    ) -> Result<(), DdlError> {
        let src = std::str::from_utf8(bytes).map_err(|e| {
            let row = stream.record_at(bytes, e.valid_up_to());
            let e = RowError::new(row, "22021", format!("input is not valid UTF-8: {e}"));
            refuse_row(e, target.label, self.committed)
        })?;
        let slice = TextSlice {
            src,
            format: stream.format,
            first_record_no: stream.next_record_no,
            header_pending: &mut stream.header_pending,
        };
        let mut source = RowSource::text(slice, &target.view)
            .map_err(|e| refuse_row(e, target.label, self.committed))?;
        self.take_source(ctx, target, &mut source).await?;
        stream.next_record_no = source.next_record_no();
        stream.ended = source.at_end_marker();
        Ok(())
    }

    /// Load every row of `source`.
    async fn take_source(
        &mut self,
        ctx: &ArrayLoadCtx<'_>,
        target: &LoadTarget,
        source: &mut RowSource<'_, '_>,
    ) -> Result<(), DdlError> {
        while let Some(next) = source.next_row() {
            let (row_no, row) = next.map_err(|e| refuse_row(e, target.label, self.committed))?;
            self.take_row(ctx, target, row_no, row).await?;
        }
        Ok(())
    }

    /// Chunk one checked row. Dispatches the chunk it fills.
    async fn take_row(
        &mut self,
        ctx: &ArrayLoadCtx<'_>,
        target: &LoadTarget,
        row_no: usize,
        row: ArrayInsertRow,
    ) -> Result<(), DdlError> {
        let vshard = self.router.vshard(&row.coords).map_err(|e| {
            refuse(
                DdlError::from_error(&e),
                row_no,
                target.label,
                self.committed,
            )
        })?;
        self.rows += 1;
        let bound = wire_bound(&row);
        if let Some(chunk) = self.chunker.push(vshard, row_no, row, bound) {
            self.dispatch(ctx, target, chunk).await?;
        }
        Ok(())
    }

    /// Dispatch one chunk and await it. Outside a block it commits.
    async fn dispatch(
        &mut self,
        ctx: &ArrayLoadCtx<'_>,
        target: &LoadTarget,
        chunk: Chunk,
    ) -> Result<(), DdlError> {
        let count = chunk.rows.len() as u64;
        if target.in_txn {
            self.staged_bytes += chunk.bytes;
            self.staged_chunks += 1;
            check_redo_cap(
                &target.array,
                self.rows,
                self.staged_bytes,
                self.staged_chunks,
            )?;
        }
        dispatch_chunk(ctx, &target.array, chunk, target.label, self.committed).await?;
        if !target.in_txn {
            self.committed += count;
        }
        Ok(())
    }
}
