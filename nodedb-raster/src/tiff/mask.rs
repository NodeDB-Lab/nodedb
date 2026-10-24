// SPDX-License-Identifier: Apache-2.0

//! The internal mask of an image: a 1- or 8-bit IFD flagged
//! `NewSubfileType` bit 4. A zero sample masks its pixel out.
//!
//! The mask has its own block grid. The reader names every mask chunk a
//! data block overlaps and fills the block's mask from them.

use crate::codec::compression::{CODE_NONE, Compression, LercWrap, StreamCodec};
use crate::codec::predictor::{Predictor, PredictorShape};
use crate::error::RasterError;
use crate::layout::{BlockKind, BlockWindow};
use crate::limits::RasterLimits;
use crate::sample::SampleType;
use crate::tiff::byte_order::ByteOrder;
use crate::tiff::error::UnsupportedTiff;
use crate::tiff::image::required_u32;
use crate::tiff::tags::{
    BITS_PER_SAMPLE, COMPRESSION, IMAGE_LENGTH, IMAGE_WIDTH, PREDICTOR, ROWS_PER_STRIP,
    STRIP_BYTE_COUNTS, STRIP_OFFSETS, TILE_BYTE_COUNTS, TILE_LENGTH, TILE_OFFSETS, TILE_WIDTH,
};
use crate::tiff::tile_index::{ChunkIndex, ChunkSpan};
use crate::tiff::values::TagValues;

/// A decodable mask IFD.
#[derive(Debug, Clone, PartialEq)]
pub struct MaskImage {
    pub width: u32,
    pub height: u32,
    pub kind: BlockKind,
    pub block_width: u32,
    pub block_height: u32,
    pub blocks_across: u32,
    /// 1 or 8.
    pub bits: u16,
    pub order: ByteOrder,
    pub codec: StreamCodec,
    pub predictor: Predictor,
    pub index: ChunkIndex,
}

impl MaskImage {
    /// Read the mask IFD of `values`.
    pub fn read(values: &TagValues<'_>, limits: RasterLimits) -> Result<Self, RasterError> {
        let width = required_u32(values, IMAGE_WIDTH, "ImageWidth")?;
        let height = required_u32(values, IMAGE_LENGTH, "ImageLength")?;
        let bits = values.u16_value(BITS_PER_SAMPLE)?.unwrap_or(1);
        if bits != 1 && bits != 8 {
            return Err(UnsupportedTiff::MaskBits { bits }.into());
        }
        let code = values.u16_value(COMPRESSION)?.unwrap_or(CODE_NONE);
        let codec = match Compression::from_code(code, LercWrap::None)? {
            Compression::Stream(codec) => codec,
            Compression::Jpeg | Compression::Lerc(_) => {
                return Err(RasterError::UnsupportedCompression { code });
            }
        };
        let compression = Compression::Stream(codec);
        let predictor = if bits == 8 && compression.takes_predictor() {
            Predictor::from_code(values.u16_value(PREDICTOR)?.unwrap_or(1), SampleType::U8)?
        } else {
            Predictor::None
        };
        let tiled = values.ifd.entry(TILE_WIDTH).is_some();
        let (kind, block_width, block_height) = if tiled {
            (
                BlockKind::Tiles,
                required_u32(values, TILE_WIDTH, "TileWidth")?,
                required_u32(values, TILE_LENGTH, "TileLength")?,
            )
        } else {
            let rows = values
                .uint(ROWS_PER_STRIP)?
                .map_or(height, |rows| {
                    u32::try_from(rows).unwrap_or(u32::MAX).min(height)
                })
                .max(1);
            (BlockKind::Strips, width, rows)
        };
        let blocks_across = width.div_ceil(block_width);
        let blocks_down = height.div_ceil(block_height);
        let chunks = u64::from(blocks_across) * u64::from(blocks_down);
        let mask = Self {
            width,
            height,
            kind,
            block_width,
            block_height,
            blocks_across,
            bits,
            order: values.order,
            codec,
            predictor,
            index: match kind {
                BlockKind::Tiles => ChunkIndex::read(
                    values,
                    (TILE_OFFSETS, "TileOffsets"),
                    (TILE_BYTE_COUNTS, "TileByteCounts"),
                    chunks,
                )?,
                BlockKind::Strips => ChunkIndex::read(
                    values,
                    (STRIP_OFFSETS, "StripOffsets"),
                    (STRIP_BYTE_COUNTS, "StripByteCounts"),
                    chunks,
                )?,
            },
        };
        let needed_bytes = mask.chunk_bytes(0);
        if needed_bytes > limits.block_bytes as u64 {
            return Err(RasterError::BlockTooLarge {
                needed_bytes,
                limit_bytes: limits.block_bytes,
            });
        }
        Ok(mask)
    }

    /// Bytes per stored row: rows pad to whole bytes.
    fn row_bytes(&self) -> u64 {
        (u64::from(self.block_width) * u64::from(self.bits)).div_ceil(8)
    }

    /// Rows of the chunk whose first row is `row_off`.
    fn chunk_rows(&self, row_off: u32) -> u32 {
        match self.kind {
            BlockKind::Tiles => self.block_height,
            BlockKind::Strips => self.block_height.min(self.height - row_off),
        }
    }

    /// Decoded bytes of the chunk whose first row is `row_off`.
    fn chunk_bytes(&self, row_off: u32) -> u64 {
        self.row_bytes() * u64::from(self.chunk_rows(row_off))
    }

    /// The mask chunks `window` overlaps, as `(chunk index, column offset,
    /// row offset)`.
    pub fn chunks_for(&self, window: &BlockWindow) -> Vec<(u64, u32, u32)> {
        if window.width == 0
            || window.height == 0
            || window.col_off >= self.width
            || window.row_off >= self.height
        {
            return Vec::new();
        }
        let last_col = (window.col_off + window.width - 1).min(self.width - 1);
        let last_row = (window.row_off + window.height - 1).min(self.height - 1);
        let (first_x, last_x) = (
            window.col_off / self.block_width,
            last_col / self.block_width,
        );
        let (first_y, last_y) = (
            window.row_off / self.block_height,
            last_row / self.block_height,
        );
        let mut chunks = Vec::new();
        for y in first_y..=last_y {
            for x in first_x..=last_x {
                let index = u64::from(y) * u64::from(self.blocks_across) + u64::from(x);
                chunks.push((index, x * self.block_width, y * self.block_height));
            }
        }
        chunks
    }

    /// The file bytes of chunk `index`.
    pub fn span(&self, index: u64) -> Result<ChunkSpan, RasterError> {
        self.index.span(index)
    }

    /// Mark the window pixels the chunk at `(col, row)` covers, from its
    /// compressed `data`. A sparse chunk masks its pixels out.
    pub fn apply_chunk(
        &self,
        data: Option<&[u8]>,
        (col, row): (u32, u32),
        window: &BlockWindow,
        mask: &mut [bool],
    ) -> Result<(), RasterError> {
        let rows = self.chunk_rows(row);
        let row_bytes = usize::try_from(self.row_bytes()).unwrap_or(usize::MAX);
        let raw = match data {
            Some(data) => {
                let expected = row_bytes.saturating_mul(rows as usize);
                let mut raw = self.codec.decompress(data, expected)?;
                self.predictor.undo(
                    &mut raw,
                    PredictorShape {
                        row_samples: self.block_width as usize,
                        stride: 1,
                        sample_type: SampleType::U8,
                        order: self.order,
                    },
                );
                Some(raw)
            }
            None => None,
        };
        let row_end = row.saturating_add(rows).min(window.row_off + window.height);
        let col_end = col
            .saturating_add(self.block_width)
            .min(window.col_off + window.width);
        for y in row.max(window.row_off)..row_end {
            for x in col.max(window.col_off)..col_end {
                let valid = raw.as_deref().is_some_and(|raw| {
                    let (cx, cy) = ((x - col) as usize, (y - row) as usize);
                    let line = cy * row_bytes;
                    if self.bits == 1 {
                        raw.get(line + cx / 8)
                            .is_some_and(|byte| byte & (0x80 >> (cx % 8)) != 0)
                    } else {
                        raw.get(line + cx).is_some_and(|byte| *byte != 0)
                    }
                });
                let at = (y - window.row_off) as usize * window.width as usize
                    + (x - window.col_off) as usize;
                if let Some(slot) = mask.get_mut(at) {
                    *slot = valid;
                }
            }
        }
        Ok(())
    }
}
