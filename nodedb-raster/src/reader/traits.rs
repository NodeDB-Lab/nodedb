// SPDX-License-Identifier: Apache-2.0

//! The sans-IO reader contract.
//!
//! An opener never performs I/O. Each call reads what the cache holds and
//! either returns the reader or names the ranges it needs next. The caller
//! fetches them over any transport, inserts them, and calls again. A
//! reader names each block's ranges, and decodes a block from the bytes
//! the caller fetched.

use crate::block::RasterBlock;
use crate::error::RasterError;
use crate::header::RasterHeader;
use crate::layout::BlockWindow;
use crate::limits::RasterLimits;
use crate::source::cache::RangeCache;
use crate::source::range::ByteRange;

/// The result of one open round.
#[derive(Debug)]
pub enum Opened<R> {
    /// The header is read.
    Ready(R),
    /// Fetch these ranges into the cache, then open again.
    Need(Vec<ByteRange>),
}

/// Opens a raster from cached header ranges.
pub trait RasterOpener {
    type Reader: RasterReader;

    /// One open round over `cache`.
    fn open(
        &self,
        cache: &RangeCache,
        limits: RasterLimits,
    ) -> Result<Opened<Self::Reader>, RasterError>;
}

/// Decodes the blocks of an opened raster.
pub trait RasterReader: Send + Sync {
    /// The header the open read.
    fn header(&self) -> &RasterHeader;

    /// Blocks per band. Block indexes run `0..block_count()`.
    fn block_count(&self) -> u32;

    /// The pixel window of block `index`, cropped to the image.
    fn block_window(&self, index: u32) -> Result<BlockWindow, RasterError>;

    /// The ranges block `index` decodes from. An empty range stands for a
    /// block the file omits and needs no fetch.
    fn block_ranges(&self, index: u32) -> Result<Vec<ByteRange>, RasterError>;

    /// Decode block `index` from the bytes of its ranges, in
    /// [`RasterReader::block_ranges`] order. `None` marks a missing
    /// object. A buffer shorter than its range means the object ends there.
    fn decode_block(&self, index: u32, bytes: &[Option<&[u8]>])
    -> Result<RasterBlock, RasterError>;
}
