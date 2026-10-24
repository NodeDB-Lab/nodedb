// SPDX-License-Identifier: Apache-2.0

//! One image IFD read into what its blocks decode with: the block grid,
//! the codec, the predictor and the block index.

use crate::codec::compression::{CODE_NONE, Compression, LercWrap};
use crate::codec::predictor::Predictor;
use crate::error::RasterError;
use crate::layout::{BlockKind, Interleave, RasterLayout};
use crate::limits::RasterLimits;
use crate::sample::{FORMAT_UINT, SampleType};
use crate::tiff::byte_order::ByteOrder;
use crate::tiff::error::{TiffFormatError, UnsupportedTiff};
use crate::tiff::tags::{
    BITS_PER_SAMPLE, COMPRESSION, IMAGE_LENGTH, IMAGE_WIDTH, JPEG_TABLES, LERC_PARAMETERS,
    PHOTOMETRIC, PLANAR_CONFIG, PREDICTOR, ROWS_PER_STRIP, SAMPLE_FORMAT, SAMPLES_PER_PIXEL,
    STRIP_BYTE_COUNTS, STRIP_OFFSETS, TILE_BYTE_COUNTS, TILE_LENGTH, TILE_OFFSETS, TILE_WIDTH,
    YCBCR_SUBSAMPLING,
};
use crate::tiff::tile_index::ChunkIndex;
use crate::tiff::values::TagValues;

/// Photometric interpretations.
const PHOTOMETRIC_BLACK_IS_ZERO: u16 = 1;
const PHOTOMETRIC_RGB: u16 = 2;
const PHOTOMETRIC_SEPARATED: u16 = 5;
const PHOTOMETRIC_YCBCR: u16 = 6;
/// Largest side of a JPEG frame.
const MAX_JPEG_SIDE: u32 = 65535;

/// A decodable image IFD.
#[derive(Debug, Clone, PartialEq)]
pub struct TiffImage {
    pub layout: RasterLayout,
    pub order: ByteOrder,
    pub compression: Compression,
    pub predictor: Predictor,
    /// `JPEGTables`, for JPEG chunks.
    pub jpeg_tables: Option<Vec<u8>>,
    pub index: ChunkIndex,
}

impl TiffImage {
    /// Read the image IFD of `values`.
    pub fn read(values: &TagValues<'_>, limits: RasterLimits) -> Result<Self, RasterError> {
        let width = required_u32(values, IMAGE_WIDTH, "ImageWidth")?;
        let height = required_u32(values, IMAGE_LENGTH, "ImageLength")?;
        let samples = values.u16_value(SAMPLES_PER_PIXEL)?.unwrap_or(1);
        if samples == 0 {
            return Err(TiffFormatError::TagValue {
                tag: SAMPLES_PER_PIXEL,
                value: 0,
            }
            .into());
        }
        let bits = per_sample(values, BITS_PER_SAMPLE, 1)?;
        let format = per_sample(values, SAMPLE_FORMAT, FORMAT_UINT)?;
        let sample_type = SampleType::from_tiff(format, bits)?;
        let interleave = match values.uint(PLANAR_CONFIG)?.unwrap_or(1) {
            1 => Interleave::Chunky,
            2 => Interleave::Planar,
            value => {
                return Err(TiffFormatError::TagValue {
                    tag: PLANAR_CONFIG,
                    value,
                }
                .into());
            }
        };
        let compression = Compression::from_code(
            values.u16_value(COMPRESSION)?.unwrap_or(CODE_NONE),
            lerc_wrap(values)?,
        )?;
        let chunk_samples = match interleave {
            Interleave::Chunky => samples,
            Interleave::Planar => 1,
        };
        check_photometric(values, samples, compression)?;
        let (kind, block_width, block_height) = block_shape(values, width, height)?;
        if compression == Compression::Jpeg {
            check_jpeg(sample_type, chunk_samples, block_width, block_height)?;
        }
        let predictor = if compression.takes_predictor() {
            Predictor::from_code(values.u16_value(PREDICTOR)?.unwrap_or(1), sample_type)?
        } else {
            Predictor::None
        };
        let too_many = TiffFormatError::TooManyBlocks {
            width,
            height,
            block_width,
            block_height,
        };
        let blocks_across = width.div_ceil(block_width);
        let blocks_down = height.div_ceil(block_height);
        let per_plane = blocks_across
            .checked_mul(blocks_down)
            .ok_or(too_many.clone())?;
        let planes = match interleave {
            Interleave::Chunky => 1,
            Interleave::Planar => u64::from(samples),
        };
        let chunks = u64::from(per_plane) * planes;
        let layout = RasterLayout {
            width,
            height,
            bands: samples,
            sample_type,
            kind,
            interleave,
            block_width,
            block_height,
            blocks_across,
            blocks_down,
        };
        let needed_bytes = layout.block_bytes();
        if needed_bytes > limits.block_bytes as u64 {
            return Err(RasterError::BlockTooLarge {
                needed_bytes,
                limit_bytes: limits.block_bytes,
            });
        }
        let index = match kind {
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
        };
        let jpeg_tables = match compression {
            Compression::Jpeg => values
                .raw(JPEG_TABLES)?
                .map(|(_, bytes)| bytes.into_owned()),
            Compression::Stream(_) | Compression::Lerc(_) => None,
        };
        Ok(Self {
            layout,
            order: values.order,
            compression,
            predictor,
            jpeg_tables,
            index,
        })
    }

    /// Samples per pixel in one chunk.
    pub fn chunk_samples(&self) -> usize {
        match self.layout.interleave {
            Interleave::Chunky => usize::from(self.layout.bands),
            Interleave::Planar => 1,
        }
    }

    /// Rows of the chunk whose first row is `row_off`: a full tile, or a
    /// strip cropped to the image.
    pub fn chunk_rows(&self, row_off: u32) -> u32 {
        match self.layout.kind {
            BlockKind::Tiles => self.layout.block_height,
            BlockKind::Strips => self.layout.block_height.min(self.layout.height - row_off),
        }
    }
}

/// A required, non-zero u32 tag.
pub fn required_u32(
    values: &TagValues<'_>,
    tag: u16,
    name: &'static str,
) -> Result<u32, RasterError> {
    let value = values
        .uint(tag)?
        .ok_or(TiffFormatError::MissingTag { tag, name })?;
    u32::try_from(value)
        .ok()
        .filter(|v| *v > 0)
        .ok_or(TiffFormatError::TagValue { tag, value }.into())
}

/// The one value a per-sample tag holds for every sample. Every sample
/// must share it.
fn per_sample(values: &TagValues<'_>, tag: u16, default: u16) -> Result<u16, RasterError> {
    let Some(all) = values.uints(tag)? else {
        return Ok(default);
    };
    let first = all.first().copied().ok_or(TiffFormatError::TagCount {
        tag,
        found: 0,
        expected: 1,
    })?;
    let first =
        u16::try_from(first).map_err(|_| TiffFormatError::TagValue { tag, value: first })?;
    if let Some(&other) = all.iter().find(|v| **v != u64::from(first)) {
        let other = u16::try_from(other).unwrap_or(u16::MAX);
        return Err(match tag {
            BITS_PER_SAMPLE => UnsupportedTiff::MixedBitsPerSample { first, other },
            _ => UnsupportedTiff::MixedSampleFormats { first, other },
        }
        .into());
    }
    Ok(first)
}

/// The LERC wrapper from `LercParameters` `[version, compression]`.
fn lerc_wrap(values: &TagValues<'_>) -> Result<LercWrap, RasterError> {
    let code = values
        .uints(LERC_PARAMETERS)?
        .and_then(|params| params.get(1).copied())
        .unwrap_or(0);
    LercWrap::from_code(code).ok_or(UnsupportedTiff::LercCompression { code }.into())
}

/// Refuse a photometric interpretation whose samples need a transform the
/// reader does not apply.
fn check_photometric(
    values: &TagValues<'_>,
    samples: u16,
    compression: Compression,
) -> Result<(), RasterError> {
    let default = if samples >= 3 {
        PHOTOMETRIC_RGB
    } else {
        PHOTOMETRIC_BLACK_IS_ZERO
    };
    match values.u16_value(PHOTOMETRIC)?.unwrap_or(default) {
        PHOTOMETRIC_BLACK_IS_ZERO | PHOTOMETRIC_RGB | PHOTOMETRIC_SEPARATED => Ok(()),
        PHOTOMETRIC_YCBCR if compression == Compression::Jpeg => Ok(()),
        PHOTOMETRIC_YCBCR => {
            let sub = values
                .uints(YCBCR_SUBSAMPLING)?
                .unwrap_or_else(|| vec![2, 2]);
            let horizontal = sub.first().copied().unwrap_or(2);
            let vertical = sub.get(1).copied().unwrap_or(2);
            if horizontal == 1 && vertical == 1 {
                Ok(())
            } else {
                Err(UnsupportedTiff::YCbCrSubsampling {
                    horizontal: u16::try_from(horizontal).unwrap_or(u16::MAX),
                    vertical: u16::try_from(vertical).unwrap_or(u16::MAX),
                }
                .into())
            }
        }
        code => Err(RasterError::UnsupportedPhotometric { code }),
    }
}

/// Strips or tiles, and the stored block size.
fn block_shape(
    values: &TagValues<'_>,
    width: u32,
    height: u32,
) -> Result<(BlockKind, u32, u32), RasterError> {
    if values.ifd.entry(TILE_WIDTH).is_some() {
        let tile_width = required_u32(values, TILE_WIDTH, "TileWidth")?;
        let tile_height = required_u32(values, TILE_LENGTH, "TileLength")?;
        return Ok((BlockKind::Tiles, tile_width, tile_height));
    }
    let rows = match values.uint(ROWS_PER_STRIP)? {
        None => height,
        Some(0) => {
            return Err(TiffFormatError::TagValue {
                tag: ROWS_PER_STRIP,
                value: 0,
            }
            .into());
        }
        Some(rows) => u32::try_from(rows).unwrap_or(u32::MAX).min(height),
    };
    Ok((BlockKind::Strips, width, rows))
}

/// Refuse a JPEG image the decoder cannot produce.
fn check_jpeg(
    sample_type: SampleType,
    chunk_samples: u16,
    width: u32,
    height: u32,
) -> Result<(), RasterError> {
    if sample_type != SampleType::U8 {
        return Err(UnsupportedTiff::JpegSampleType { sample_type }.into());
    }
    if chunk_samples != 1 && chunk_samples != 3 {
        return Err(UnsupportedTiff::JpegSamples {
            samples: chunk_samples,
        }
        .into());
    }
    if width > MAX_JPEG_SIDE || height > MAX_JPEG_SIDE {
        return Err(UnsupportedTiff::JpegFrameSize { width, height }.into());
    }
    Ok(())
}
