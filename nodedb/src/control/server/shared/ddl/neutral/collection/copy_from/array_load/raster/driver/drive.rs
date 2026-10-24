// SPDX-License-Identifier: BUSL-1.1

//! `drive_raster`: the one async driver of a raster load, whatever the
//! transport.
//!
//! 1. Open every source in shared rounds ([`open_sources`]).
//! 2. Check that the sources share a grid and block layout, and run every
//!    open-time check of the plan. A refused input commits no row.
//! 3. With `ADOPT_METADATA true`, write the files' attr metadata.
//! 4. A producer task reads the blocks the plan wants, one block index from
//!    every source at a time, into a queue of two. The load turns each set
//!    into rows, every source's bands in one row, and feeds the loader.
//!
//! Network reads run on the Control Plane's Tokio runtime and decoding on
//! its blocking pool. The Data Plane receives only the array writes.

use std::sync::Arc;

use nodedb_raster::{ObjectId, RasterLimits, RasterReader, TiffOpener};
use nodedb_raster_load::{
    BlockRows, LayoutError, RasterPlan, SourceHeader, adopted_metadata, check_same_layout,
};
use nodedb_sql::ddl_ast::statement::CopyRasterOptions;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::control::server::shared::ddl::result::DdlError;

use super::super::super::decode::RowError;
use super::super::super::dispatch::{ArrayLoadCtx, input_cap};
use super::super::super::loader::{ArrayLoader, LoadInput};
use super::super::adopt::adopt_attr_metadata;
use super::super::fetch::RangeFetch;
use super::super::refusal::{drive_refusal, raster_refusal};
use super::array_view::{array_view, insert_row, meta_alter, view_georef};
use super::blocks::{BLOCK_QUEUE, produce_blocks};
use super::error::DriveError;
use super::load_options::load_options;
use super::open::open_sources;

/// The most bytes one block decodes to, before the memory headroom cap.
const MAX_BLOCK_BYTES: usize = 64 * 1024 * 1024;

/// Aborts the producer task when the load ends early.
struct AbortOnDrop(Option<JoinHandle<()>>);

impl AbortOnDrop {
    /// The task, which the guard no longer aborts.
    fn take(&mut self) -> Option<JoinHandle<()>> {
        self.0.take()
    }
}

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        if let Some(handle) = &self.0 {
            handle.abort();
        }
    }
}

/// Load the sources `names` into `array`, reading object `i` as source `i`
/// through `fetch`. `opener` picks the image (full resolution or an
/// overview). Returns the number of rows loaded.
pub(crate) async fn drive_raster<F: RangeFetch + 'static>(
    ctx: &ArrayLoadCtx<'_>,
    array: &str,
    names: &[String],
    opener: TiffOpener,
    fetch: Arc<F>,
    options: &CopyRasterOptions,
) -> Result<u64, DdlError> {
    let mut loader = ArrayLoader::open(ctx, array, LoadInput::Raster)?;
    let georef = loader.view().georef.clone().ok_or_else(|| {
        loader.refuse(RowError::new(
            0,
            "55000",
            format!(
                "array '{array}' has no GEOREF, so no pixel has a cell; \
                 add one with ALTER ARRAY {array} SET GEOREF (...)"
            ),
        ))
    })?;
    let cap = MAX_BLOCK_BYTES.min(input_cap(ctx.state));
    let limits = RasterLimits::new(cap);
    let refuse = |loader: &ArrayLoader, e: DriveError| loader.refuse(drive_refusal(e, names, cap));

    let openers: Vec<TiffOpener> = (0..names.len())
        .map(|i| opener.with_object(ObjectId(u32::try_from(i).unwrap_or(u32::MAX))))
        .collect();
    let readers = open_sources(&*fetch, &openers, limits)
        .await
        .map_err(|e| refuse(&loader, e))?;
    let crs = crate::control::crs_context::crs_context_for(ctx.state);
    let mut headers = Vec::with_capacity(readers.len());
    for (index, (reader, name)) in readers.iter().zip(names).enumerate() {
        let srid = reader
            .header()
            .crs
            .resolve(&crs)
            .map_err(|error| refuse(&loader, DriveError::Raster { index, error }))?;
        headers.push(SourceHeader::new(
            name.clone(),
            reader.header(),
            srid.map(|s| s.code()),
        ));
    }
    check_same_layout(&headers, &readers).map_err(|e| match e {
        LayoutError::Mismatch(mismatch) => loader.refuse(mismatch.into()),
        LayoutError::Reader(error) => loader.refuse(raster_refusal(
            &error,
            names.first().map_or("?", String::as_str),
            cap,
        )),
    })?;
    let view = array_view(loader.view());
    let plan = RasterPlan::new(
        &view,
        &view_georef(&georef),
        &headers,
        &load_options(options),
    )
    .map_err(|e| loader.refuse(e.into()))?;
    if options.adopt_metadata {
        let changes: Vec<_> = adopted_metadata(&view, &plan, &headers)
            .iter()
            .map(meta_alter)
            .collect();
        adopt_attr_metadata(ctx, array, &changes).await?;
    }

    let indexes = wanted_blocks(&readers, &plan)
        .map_err(|error| refuse(&loader, DriveError::Raster { index: 0, error }))?;
    let readers = Arc::new(readers);
    let (tx, mut rx) = mpsc::channel(BLOCK_QUEUE);
    let mut producer = AbortOnDrop(Some(tokio::spawn(produce_blocks(
        Arc::clone(&readers),
        Arc::clone(&fetch),
        indexes,
        tx,
    ))));
    while let Some(next) = rx.recv().await {
        let blocks = next.map_err(|e| refuse(&loader, e))?;
        let rows = BlockRows::new(&plan, &blocks).map_err(|e| loader.refuse(e.into()))?;
        loader
            .feed_rows(
                ctx,
                rows.map(|row| {
                    row.map(|(row_no, row)| (row_no, insert_row(row)))
                        .map_err(RowError::from)
                }),
            )
            .await?;
    }
    // The channel closes once the producer returns. A panic closes it
    // early, so the join result tells a whole input from a cut-short one.
    if let Some(handle) = producer.take() {
        handle
            .await
            .map_err(|e| DdlError::internal(format!("COPY: raster block reader failed: {e}")))?;
    }
    loader.finish(ctx).await
}

/// The block indexes that hold a pixel the plan loads, in order.
fn wanted_blocks<R: RasterReader>(
    readers: &[R],
    plan: &RasterPlan,
) -> Result<Vec<u32>, nodedb_raster::RasterError> {
    let Some(first) = readers.first() else {
        return Ok(Vec::new());
    };
    let mut indexes = Vec::new();
    for index in 0..first.block_count() {
        if plan.wants_block(&first.block_window(index)?) {
            indexes.push(index);
        }
    }
    Ok(indexes)
}
