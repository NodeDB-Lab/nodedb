// SPDX-License-Identifier: Apache-2.0

//! TIFF predictors: horizontal differencing (2) and the floating-point
//! byte-plane predictor of TIFF Technical Note 3 (3).

use crate::error::RasterError;
use crate::sample::SampleType;
use crate::tiff::byte_order::ByteOrder;
use crate::tiff::error::UnsupportedTiff;

/// The `Predictor` tag.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Predictor {
    None,
    Horizontal,
    FloatingPoint,
}

/// The shape of one decoded chunk a predictor runs over.
#[derive(Debug, Clone, Copy)]
pub struct PredictorShape {
    /// Samples per row: chunk width times samples per pixel.
    pub row_samples: usize,
    /// Samples per pixel in the chunk: the differencing stride.
    pub stride: usize,
    pub sample_type: SampleType,
    pub order: ByteOrder,
}

impl Predictor {
    /// The predictor of tag value `code` for `sample_type` samples.
    pub fn from_code(code: u16, sample_type: SampleType) -> Result<Self, RasterError> {
        match code {
            1 => Ok(Predictor::None),
            2 => Ok(Predictor::Horizontal),
            3 if sample_type.is_float() => Ok(Predictor::FloatingPoint),
            3 => Err(UnsupportedTiff::FloatPredictorOnIntegers { sample_type }.into()),
            code => Err(UnsupportedTiff::Predictor { code }.into()),
        }
    }

    /// Undo the predictor over `data`, rows of `shape.row_samples` samples
    /// stored in `shape.order`. The floating-point predictor leaves the
    /// samples in `shape.order` as well. A trailing partial row is left
    /// as is.
    pub fn undo(self, data: &mut [u8], shape: PredictorShape) {
        let width = shape.sample_type.byte_len();
        let row_bytes = shape.row_samples.saturating_mul(width);
        if row_bytes == 0 {
            return;
        }
        match self {
            Predictor::None => {}
            Predictor::Horizontal => {
                for row in data.chunks_exact_mut(row_bytes) {
                    undo_horizontal(row, shape.stride, width, shape.order);
                }
            }
            Predictor::FloatingPoint => {
                let mut scratch = vec![0u8; row_bytes];
                for row in data.chunks_exact_mut(row_bytes) {
                    undo_floating(row, &mut scratch, shape.stride, width, shape.order);
                }
            }
        }
    }
}

/// `sample[i] += sample[i - stride]`, wrapping at the sample width.
fn undo_horizontal(row: &mut [u8], stride: usize, width: usize, order: ByteOrder) {
    let samples = row.len() / width;
    let mask = if width >= 8 {
        u64::MAX
    } else {
        (1u64 << (width * 8)) - 1
    };
    for i in stride..samples {
        let (Some(left), Some(here)) = (
            order.uint_at(row, (i - stride) * width, width),
            order.uint_at(row, i * width, width),
        ) else {
            return;
        };
        order.put_uint(row, i * width, width, here.wrapping_add(left) & mask);
    }
}

/// Undo byte differencing at the pixel stride, then gather the byte
/// planes (most significant byte first) back into samples.
fn undo_floating(
    row: &mut [u8],
    scratch: &mut [u8],
    stride: usize,
    width: usize,
    order: ByteOrder,
) {
    for i in stride..row.len() {
        row[i] = row[i].wrapping_add(row[i - stride]);
    }
    let samples = row.len() / width;
    for i in 0..samples {
        for byte in 0..width {
            let plane = row[byte * samples + i];
            let at = match order {
                ByteOrder::Big => i * width + byte,
                ByteOrder::Little => i * width + (width - 1 - byte),
            };
            scratch[at] = plane;
        }
    }
    row.copy_from_slice(scratch);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn horizontal_differences_add_back_per_band() {
        // Two pixels of two u16 bands, little-endian: (1, 100), (+2, -1).
        let mut data = Vec::new();
        for v in [1u16, 100, 2, u16::MAX] {
            data.extend(v.to_le_bytes());
        }
        let shape = PredictorShape {
            row_samples: 4,
            stride: 2,
            sample_type: SampleType::U16,
            order: ByteOrder::Little,
        };
        Predictor::Horizontal.undo(&mut data, shape);
        let samples: Vec<u16> = data
            .as_chunks::<2>()
            .0
            .iter()
            .map(|c| u16::from_le_bytes(*c))
            .collect();
        assert_eq!(samples, vec![1, 100, 3, 99]);
    }

    #[test]
    fn floating_point_planes_reassemble() {
        let values = [1.5f32, -2.25, 1000.0];
        // Encode: big-endian byte planes, then byte differencing at stride 1.
        let be: Vec<[u8; 4]> = values.iter().map(|v| v.to_be_bytes()).collect();
        let mut encoded: Vec<u8> = (0..4)
            .flat_map(|byte| be.iter().map(move |s| s[byte]))
            .collect();
        for i in (1..encoded.len()).rev() {
            encoded[i] = encoded[i].wrapping_sub(encoded[i - 1]);
        }
        for order in [ByteOrder::Little, ByteOrder::Big] {
            let mut data = encoded.clone();
            let shape = PredictorShape {
                row_samples: 3,
                stride: 1,
                sample_type: SampleType::F32,
                order,
            };
            Predictor::FloatingPoint.undo(&mut data, shape);
            let decoded: Vec<f32> = data
                .as_chunks::<4>()
                .0
                .iter()
                .map(|raw| match order {
                    ByteOrder::Little => f32::from_le_bytes(*raw),
                    ByteOrder::Big => f32::from_be_bytes(*raw),
                })
                .collect();
            assert_eq!(decoded, values);
        }
    }

    #[test]
    fn predictor_codes_are_checked() {
        assert_eq!(
            Predictor::from_code(3, SampleType::F64).ok(),
            Some(Predictor::FloatingPoint)
        );
        assert!(matches!(
            Predictor::from_code(3, SampleType::U16),
            Err(RasterError::UnsupportedTiff(
                UnsupportedTiff::FloatPredictorOnIntegers { .. }
            ))
        ));
        assert!(matches!(
            Predictor::from_code(34892, SampleType::U8),
            Err(RasterError::UnsupportedTiff(UnsupportedTiff::Predictor {
                code: 34892
            }))
        ));
    }
}
