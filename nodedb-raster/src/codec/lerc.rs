// SPDX-License-Identifier: Apache-2.0

//! LERC (code 34887): an Esri LERC blob per chunk, optionally inside a
//! deflate or ZSTD stream (`LercParameters`).
//!
//! A chunky chunk is one blob of depth `samples per pixel`, or one band
//! per sample. A planar chunk is one band of depth 1. Each chunk sample
//! takes the validity mask of its blob band: a blob of depth `samples` has
//! one mask for every sample, and a multi-band blob has one per band.

use lerc::{DataType, SampleData};

use crate::codec::error::CodecError;
use crate::codec::lerc_blob::check_blobs;
use crate::sample::SampleType;

/// The chunk a LERC blob must decode to.
#[derive(Debug, Clone, Copy)]
pub struct LercChunk {
    pub width: u32,
    pub height: u32,
    /// Samples per pixel in the chunk.
    pub samples: u32,
    pub sample_type: SampleType,
}

/// A decoded LERC chunk.
#[derive(Debug)]
pub struct LercDecoded {
    /// Pixel-interleaved samples, little-endian.
    pub bytes: Vec<u8>,
    /// One mask per chunk sample: one flag per pixel, `None` when every
    /// pixel of that sample is valid.
    pub masks: Vec<Option<Vec<bool>>>,
}

/// Decode the LERC blob `blob` into `chunk`.
pub fn decode(blob: &[u8], chunk: LercChunk) -> Result<LercDecoded, CodecError> {
    check_blobs(blob)?;
    let info = lerc::decode_info(blob).map_err(CodecError::Lerc)?;
    let shape_error = || CodecError::LercShape {
        width: info.width,
        height: info.height,
        depth: info.depth,
        bands: info.bands,
        data_type: data_type_name(info.data_type),
        expected_width: chunk.width,
        expected_height: chunk.height,
        expected_depth: chunk.samples,
        expected_type: chunk.sample_type.name(),
    };
    let layout_ok = (info.depth == chunk.samples && info.bands == 1)
        || (info.depth == 1 && info.bands == chunk.samples);
    if info.width != chunk.width
        || info.height != chunk.height
        || !layout_ok
        || sample_type_of(info.data_type) != chunk.sample_type
    {
        return Err(shape_error());
    }
    let image = lerc::decode(blob).map_err(CodecError::Lerc)?;
    let pixels = (chunk.width as usize).saturating_mul(chunk.height as usize);
    let samples = chunk.samples as usize;
    let band_major = info.bands > 1;
    let bytes = interleaved_bytes(&image.data, pixels, samples, band_major);
    let expected = pixels
        .saturating_mul(samples)
        .saturating_mul(chunk.sample_type.byte_len());
    if bytes.len() != expected {
        return Err(CodecError::Short {
            expected,
            found: bytes.len(),
        });
    }
    let masks = (0..samples)
        .map(|sample| {
            let mask = if band_major {
                image
                    .valid_masks
                    .get(sample)
                    .or_else(|| image.valid_masks.first())
            } else {
                image.valid_masks.first()
            };
            mask.filter(|mask| !mask.is_all_valid()).map(|mask| {
                (0..pixels)
                    .map(|pixel| pixel < mask.num_pixels() && mask.is_valid(pixel))
                    .collect()
            })
        })
        .collect();
    Ok(LercDecoded { bytes, masks })
}

/// The sample type of a LERC data type.
fn sample_type_of(data_type: DataType) -> SampleType {
    match data_type {
        DataType::Char => SampleType::I8,
        DataType::Byte => SampleType::U8,
        DataType::Short => SampleType::I16,
        DataType::UShort => SampleType::U16,
        DataType::Int => SampleType::I32,
        DataType::UInt => SampleType::U32,
        DataType::Float => SampleType::F32,
        DataType::Double => SampleType::F64,
    }
}

fn data_type_name(data_type: DataType) -> &'static str {
    sample_type_of(data_type).name()
}

/// Little-endian bytes of `data` in pixel-interleaved order. Band-major
/// data (one band per sample) is interleaved.
fn interleaved_bytes(
    data: &SampleData,
    pixels: usize,
    samples: usize,
    band_major: bool,
) -> Vec<u8> {
    match data {
        SampleData::I8(v) => to_bytes(v, pixels, samples, band_major, |s| s.to_le_bytes()),
        SampleData::U8(v) => to_bytes(v, pixels, samples, band_major, |s| s.to_le_bytes()),
        SampleData::I16(v) => to_bytes(v, pixels, samples, band_major, |s| s.to_le_bytes()),
        SampleData::U16(v) => to_bytes(v, pixels, samples, band_major, |s| s.to_le_bytes()),
        SampleData::I32(v) => to_bytes(v, pixels, samples, band_major, |s| s.to_le_bytes()),
        SampleData::U32(v) => to_bytes(v, pixels, samples, band_major, |s| s.to_le_bytes()),
        SampleData::F32(v) => to_bytes(v, pixels, samples, band_major, |s| s.to_le_bytes()),
        SampleData::F64(v) => to_bytes(v, pixels, samples, band_major, |s| s.to_le_bytes()),
    }
}

fn to_bytes<T: Copy, const N: usize>(
    values: &[T],
    pixels: usize,
    samples: usize,
    band_major: bool,
    le: impl Fn(T) -> [u8; N],
) -> Vec<u8> {
    let mut out = Vec::with_capacity(values.len().saturating_mul(N));
    if band_major && samples > 1 {
        for pixel in 0..pixels {
            for band in 0..samples {
                if let Some(value) = values.get(band * pixels + pixel) {
                    out.extend_from_slice(&le(*value));
                }
            }
        }
    } else {
        for value in values {
            out.extend_from_slice(&le(*value));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use lerc::bitmask::BitMask;
    use lerc::{Image, Precision};

    use super::*;

    fn blob(width: u32, height: u32, data: Vec<u16>, mask: BitMask) -> Vec<u8> {
        let image = Image {
            width,
            height,
            depth: 1,
            bands: 1,
            data_type: DataType::UShort,
            valid_masks: vec![mask],
            data: SampleData::U16(data),
            no_data_value: None,
        };
        lerc::encode(&image, Precision::Lossless).expect("encodes")
    }

    #[test]
    fn a_blob_decodes_with_its_mask() {
        let mut mask = BitMask::all_valid(4);
        mask.set_invalid(2);
        let encoded = blob(2, 2, vec![10, 20, 0, 40], mask);
        let chunk = LercChunk {
            width: 2,
            height: 2,
            samples: 1,
            sample_type: SampleType::U16,
        };
        let decoded = decode(&encoded, chunk).expect("decodes");
        let values: Vec<u16> = decoded
            .bytes
            .as_chunks::<2>()
            .0
            .iter()
            .map(|c| u16::from_le_bytes(*c))
            .collect();
        assert_eq!((values[0], values[1], values[3]), (10, 20, 40));
        assert_eq!(decoded.masks, vec![Some(vec![true, true, false, true])]);
    }

    #[test]
    fn each_band_of_a_multi_band_blob_keeps_its_own_mask() {
        let mut first = BitMask::all_valid(2);
        first.set_invalid(0);
        let mut second = BitMask::all_valid(2);
        second.set_invalid(1);
        let image = Image {
            width: 2,
            height: 1,
            depth: 1,
            bands: 2,
            data_type: DataType::UShort,
            valid_masks: vec![first, second],
            data: SampleData::U16(vec![0, 2, 3, 0]),
            no_data_value: None,
        };
        let encoded = lerc::encode(&image, Precision::Lossless).expect("encodes");
        let chunk = LercChunk {
            width: 2,
            height: 1,
            samples: 2,
            sample_type: SampleType::U16,
        };
        let decoded = decode(&encoded, chunk).expect("decodes");
        assert_eq!(
            decoded.masks,
            vec![Some(vec![false, true]), Some(vec![true, false])]
        );
    }

    #[test]
    fn a_blob_of_another_shape_is_refused() {
        let encoded = blob(2, 2, vec![1, 2, 3, 4], BitMask::all_valid(4));
        let chunk = LercChunk {
            width: 4,
            height: 1,
            samples: 1,
            sample_type: SampleType::U16,
        };
        assert!(matches!(
            decode(&encoded, chunk),
            Err(CodecError::LercShape { .. })
        ));
        let wrong_type = LercChunk {
            width: 2,
            height: 2,
            samples: 1,
            sample_type: SampleType::F32,
        };
        assert!(matches!(
            decode(&encoded, wrong_type),
            Err(CodecError::LercShape { .. })
        ));
        assert!(matches!(
            decode(b"junk", wrong_type),
            Err(CodecError::LercBlob { .. })
        ));
    }
}
