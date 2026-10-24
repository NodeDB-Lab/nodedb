// SPDX-License-Identifier: Apache-2.0

//! `TiffReader`: one opened TIFF image, decoded block by block.
//!
//! A block's ranges are its data chunks (one, or one per band when
//! planar) followed by the internal mask chunks it overlaps. The internal
//! mask applies to every band. A LERC mask applies to its own band.

use crate::block::{BandSamples, RasterBlock};
use crate::error::RasterError;
use crate::header::RasterHeader;
use crate::layout::{BlockWindow, Interleave, RasterLayout};
use crate::limits::RasterLimits;
use crate::reader::traits::RasterReader;
use crate::source::range::{ByteRange, ObjectId};
use crate::tiff::decode::decode_chunk;
use crate::tiff::error::TiffFormatError;
use crate::tiff::geokeys::GeoKeyDirectory;
use crate::tiff::image::TiffImage;
use crate::tiff::mask::MaskImage;
use crate::tiff::samples::{RawChunk, crop_mask, split_bands, zeroed};
use crate::tiff::tile_index::ChunkSpan;

/// An opened TIFF image.
#[derive(Debug, Clone)]
pub struct TiffReader {
    pub(crate) object: ObjectId,
    pub(crate) header: RasterHeader,
    pub(crate) image: TiffImage,
    pub(crate) mask: Option<MaskImage>,
    pub(crate) geokeys: Option<GeoKeyDirectory>,
    pub(crate) limits: RasterLimits,
}

/// One validity mask per band, `None` where every pixel of the band is valid.
type BandMasks = Vec<Option<Vec<bool>>>;

impl TiffReader {
    /// The block grid and band layout.
    pub fn layout(&self) -> &RasterLayout {
        &self.image.layout
    }

    /// The GeoKey directory of the first image, when the file has one.
    pub fn geokeys(&self) -> Option<&GeoKeyDirectory> {
        self.geokeys.as_ref()
    }

    /// True when the image has an internal mask.
    pub fn has_mask(&self) -> bool {
        self.mask.is_some()
    }

    /// The data chunks of block `index`: one, or one per band when planar.
    fn data_chunks(&self, index: u32) -> Vec<u64> {
        let per_plane = u64::from(self.image.layout.block_count());
        match self.image.layout.interleave {
            Interleave::Chunky => vec![u64::from(index)],
            Interleave::Planar => (0..u64::from(self.image.layout.bands))
                .map(|band| band * per_plane + u64::from(index))
                .collect(),
        }
    }

    /// The range of `span`, refusing a chunk over the block cap.
    fn range_of(&self, span: ChunkSpan) -> Result<ByteRange, RasterError> {
        if span.is_sparse() {
            return Ok(ByteRange::new(self.object, span.offset, span.offset));
        }
        if span.len > self.limits.block_bytes as u64 {
            return Err(RasterError::BlockTooLarge {
                needed_bytes: span.len,
                limit_bytes: self.limits.block_bytes,
            });
        }
        Ok(ByteRange::new(
            self.object,
            span.offset,
            span.offset + span.len,
        ))
    }

    /// Decode the data chunks of `window` into bands and their LERC masks,
    /// one mask per band.
    fn decode_bands(
        &self,
        index: u32,
        window: &BlockWindow,
        spans: &[ChunkSpan],
        bytes: &[Option<&[u8]>],
    ) -> Result<(Vec<BandSamples>, BandMasks), RasterError> {
        let layout = &self.image.layout;
        let (cols, rows) = (window.width as usize, window.height as usize);
        let chunk_rows = self.image.chunk_rows(window.row_off);
        let mut bands = Vec::with_capacity(usize::from(layout.bands));
        let mut masks = Vec::with_capacity(usize::from(layout.bands));
        for (span, data) in spans.iter().zip(bytes) {
            if span.is_sparse() {
                for _ in 0..self.image.chunk_samples() {
                    bands.push(zeroed(layout.sample_type, cols * rows));
                    masks.push(Some(vec![false; cols * rows]));
                }
                continue;
            }
            let data = present(*data, *span, self.object)?;
            let decoded = decode_chunk(&self.image, data, chunk_rows)?;
            let chunk = RawChunk {
                bytes: &decoded.bytes,
                order: decoded.order,
                width: layout.block_width as usize,
                samples: self.image.chunk_samples(),
                sample_type: layout.sample_type,
            };
            let needed = chunk.needed(cols, rows);
            if decoded.bytes.len() < needed {
                return Err(RasterError::ShortBlock {
                    index,
                    found: decoded.bytes.len(),
                    expected: needed,
                });
            }
            bands.extend(split_bands(&chunk, cols, rows));
            for sample in 0..self.image.chunk_samples() {
                masks.push(decoded.masks.get(sample).and_then(|mask| {
                    mask.as_ref()
                        .map(|mask| crop_mask(mask, layout.block_width as usize, cols, rows))
                }));
            }
        }
        Ok((bands, masks))
    }
}

/// The bytes of a fetched chunk: the whole span, or a typed error.
fn present(data: Option<&[u8]>, span: ChunkSpan, object: ObjectId) -> Result<&[u8], RasterError> {
    let data = data.ok_or(RasterError::SourceMissing { object: object.0 })?;
    if (data.len() as u64) < span.len {
        return Err(TiffFormatError::Truncated {
            offset: span.offset,
            len: span.len,
            end: span.offset.saturating_add(data.len() as u64),
        }
        .into());
    }
    Ok(data)
}

impl RasterReader for TiffReader {
    fn header(&self) -> &RasterHeader {
        &self.header
    }

    fn block_count(&self) -> u32 {
        self.image.layout.block_count()
    }

    fn block_window(&self, index: u32) -> Result<BlockWindow, RasterError> {
        self.image.layout.window(index)
    }

    fn block_ranges(&self, index: u32) -> Result<Vec<ByteRange>, RasterError> {
        let window = self.image.layout.window(index)?;
        let mut ranges = Vec::new();
        for chunk in self.data_chunks(index) {
            ranges.push(self.range_of(self.image.index.span(chunk)?)?);
        }
        if let Some(mask) = &self.mask {
            for (chunk, _, _) in mask.chunks_for(&window) {
                ranges.push(self.range_of(mask.span(chunk)?)?);
            }
        }
        Ok(ranges)
    }

    fn decode_block(
        &self,
        index: u32,
        bytes: &[Option<&[u8]>],
    ) -> Result<RasterBlock, RasterError> {
        let window = self.image.layout.window(index)?;
        let data_chunks = self.data_chunks(index);
        let mask_chunks = self
            .mask
            .as_ref()
            .map(|mask| mask.chunks_for(&window))
            .unwrap_or_default();
        let expected = data_chunks.len() + mask_chunks.len();
        if bytes.len() != expected {
            return Err(RasterError::BlockBuffers {
                index,
                found: bytes.len(),
                expected,
            });
        }
        let (data_bytes, mask_bytes) = bytes.split_at(data_chunks.len());
        let spans = data_chunks
            .iter()
            .map(|chunk| self.image.index.span(*chunk))
            .collect::<Result<Vec<_>, _>>()?;
        let (bands, mut masks) = self.decode_bands(index, &window, &spans, data_bytes)?;
        if let Some(mask_image) = &self.mask {
            let mut internal = vec![true; window.pixels()];
            for ((chunk, col, row), data) in mask_chunks.iter().zip(mask_bytes) {
                let span = mask_image.span(*chunk)?;
                let data = if span.is_sparse() {
                    None
                } else {
                    Some(present(*data, span, self.object)?)
                };
                mask_image.apply_chunk(data, (*col, *row), &window, &mut internal)?;
            }
            for mask in &mut masks {
                *mask = Some(match mask.take() {
                    None => internal.clone(),
                    Some(lerc) => lerc.iter().zip(&internal).map(|(a, b)| *a && *b).collect(),
                });
            }
        }
        for mask in &mut masks {
            if mask
                .as_ref()
                .is_some_and(|flags| flags.iter().all(|valid| *valid))
            {
                *mask = None;
            }
        }
        Ok(RasterBlock {
            index,
            window,
            bands,
            masks,
        })
    }
}
