// SPDX-License-Identifier: Apache-2.0

//! One compressed chunk to raw sample bytes: decompress, then undo the
//! predictor.

use crate::codec::compression::Compression;
use crate::codec::error::CodecError;
use crate::codec::jpeg::{self, JpegChunk};
use crate::codec::lerc::{self, LercChunk};
use crate::codec::predictor::PredictorShape;
use crate::error::RasterError;
use crate::limits::checked_raster_capacity;
use crate::tiff::byte_order::ByteOrder;
use crate::tiff::image::TiffImage;

/// Slack a LERC blob may need over its raw samples: header, mask and
/// incompressible blocks.
const LERC_SLACK: usize = 64 * 1024;

/// A decoded chunk.
#[derive(Debug)]
pub struct DecodedChunk {
    /// `width x rows` pixels of interleaved samples.
    pub bytes: Vec<u8>,
    pub order: ByteOrder,
    /// One mask per chunk sample from LERC validity masks: one flag per
    /// chunk pixel, `None` when every pixel of the sample is valid.
    pub masks: Vec<Option<Vec<bool>>>,
}

/// Decode chunk `data` of `image`, `rows` rows tall.
pub fn decode_chunk(
    image: &TiffImage,
    data: &[u8],
    rows: u32,
) -> Result<DecodedChunk, RasterError> {
    let samples = checked_raster_capacity(image.chunk_samples()).map_err(CodecError::from)?;
    let width = image.layout.block_width;
    let sample_type = image.layout.sample_type;
    // The open-time block cap bounds this product.
    let expected = (width as usize)
        .saturating_mul(rows as usize)
        .saturating_mul(samples)
        .saturating_mul(sample_type.byte_len());
    match image.compression {
        Compression::Stream(codec) => {
            let mut bytes = codec.decompress(data, expected)?;
            image.predictor.undo(
                &mut bytes,
                PredictorShape {
                    row_samples: width as usize * samples,
                    stride: samples,
                    sample_type,
                    order: image.order,
                },
            );
            Ok(DecodedChunk {
                bytes,
                order: image.order,
                masks: vec![None; samples],
            })
        }
        Compression::Jpeg => {
            let bytes = jpeg::decode(JpegChunk {
                data,
                tables: image.jpeg_tables.as_deref(),
                width,
                height: rows,
                components: samples,
            })?;
            Ok(DecodedChunk {
                bytes,
                order: image.order,
                masks: vec![None; samples],
            })
        }
        Compression::Lerc(wrap) => {
            let unwrapped;
            let blob = match wrap.stream() {
                None => data,
                Some(codec) => {
                    let max = expected.saturating_mul(2).saturating_add(LERC_SLACK);
                    unwrapped = codec.decompress_bounded(data, max)?;
                    unwrapped.as_slice()
                }
            };
            let decoded = lerc::decode(
                blob,
                LercChunk {
                    width,
                    height: rows,
                    samples: u32::try_from(samples).unwrap_or(u32::MAX),
                    sample_type,
                },
            )?;
            Ok(DecodedChunk {
                bytes: decoded.bytes,
                order: ByteOrder::Little,
                masks: decoded.masks,
            })
        }
    }
}
