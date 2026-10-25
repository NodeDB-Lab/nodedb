// SPDX-License-Identifier: Apache-2.0

//! Raw chunk bytes to typed per-band samples, cropped to the block window.

use crate::block::BandSamples;
use crate::sample::SampleType;
use crate::tiff::byte_order::ByteOrder;

/// One decoded chunk: `samples` interleaved samples per pixel, `width`
/// pixels per row, stored in `order`.
#[derive(Debug, Clone, Copy)]
pub struct RawChunk<'a> {
    pub bytes: &'a [u8],
    pub order: ByteOrder,
    pub width: usize,
    pub samples: usize,
    pub sample_type: SampleType,
}

impl RawChunk<'_> {
    /// Bytes the top-left `cols x rows` window reads.
    pub fn needed(&self, cols: usize, rows: usize) -> usize {
        if cols == 0 || rows == 0 {
            return 0;
        }
        ((rows - 1) * self.width + cols) * self.samples * self.sample_type.byte_len()
    }
}

/// A fixed-width sample read from bytes in a byte order.
trait Sample: Copy {
    const WIDTH: usize;
    fn read(raw: &[u8], order: ByteOrder) -> Self;
}

macro_rules! sample {
    ($ty:ty, $width:expr) => {
        impl Sample for $ty {
            const WIDTH: usize = $width;
            fn read(raw: &[u8], order: ByteOrder) -> Self {
                let bytes: [u8; $width] = raw.try_into().unwrap_or_default();
                match order {
                    ByteOrder::Little => <$ty>::from_le_bytes(bytes),
                    ByteOrder::Big => <$ty>::from_be_bytes(bytes),
                }
            }
        }
    };
}

sample!(u8, 1);
sample!(i8, 1);
sample!(u16, 2);
sample!(i16, 2);
sample!(u32, 4);
sample!(i32, 4);
sample!(f32, 4);
sample!(f64, 8);

/// One vector per chunk sample over the top-left `cols x rows` window.
/// The caller checks `chunk.needed(cols, rows) <= chunk.bytes.len()`.
fn split<T: Sample>(chunk: &RawChunk<'_>, cols: usize, rows: usize) -> Vec<Vec<T>> {
    let pixels = cols * rows;
    let mut bands: Vec<Vec<T>> = (0..chunk.samples)
        .map(|_| Vec::with_capacity(pixels))
        .collect();
    let pixel_bytes = chunk.samples * T::WIDTH;
    for row in 0..rows {
        let start = row * chunk.width * pixel_bytes;
        let line = chunk
            .bytes
            .get(start..start + cols * pixel_bytes)
            .unwrap_or_default();
        for pixel in line.chunks_exact(pixel_bytes) {
            for (band, raw) in bands.iter_mut().zip(pixel.chunks_exact(T::WIDTH)) {
                band.push(T::read(raw, chunk.order));
            }
        }
    }
    bands
}

/// The window's samples, one [`BandSamples`] per chunk sample.
pub fn split_bands(chunk: &RawChunk<'_>, cols: usize, rows: usize) -> Vec<BandSamples> {
    match chunk.sample_type {
        SampleType::U8 => wrap(split::<u8>(chunk, cols, rows), BandSamples::U8),
        SampleType::I8 => wrap(split::<i8>(chunk, cols, rows), BandSamples::I8),
        SampleType::U16 => wrap(split::<u16>(chunk, cols, rows), BandSamples::U16),
        SampleType::I16 => wrap(split::<i16>(chunk, cols, rows), BandSamples::I16),
        SampleType::U32 => wrap(split::<u32>(chunk, cols, rows), BandSamples::U32),
        SampleType::I32 => wrap(split::<i32>(chunk, cols, rows), BandSamples::I32),
        SampleType::F32 => wrap(split::<f32>(chunk, cols, rows), BandSamples::F32),
        SampleType::F64 => wrap(split::<f64>(chunk, cols, rows), BandSamples::F64),
    }
}

fn wrap<T>(bands: Vec<Vec<T>>, variant: fn(Vec<T>) -> BandSamples) -> Vec<BandSamples> {
    bands.into_iter().map(variant).collect()
}

/// `pixels` zero samples of `sample_type`.
pub fn zeroed(sample_type: SampleType, pixels: usize) -> BandSamples {
    match sample_type {
        SampleType::U8 => BandSamples::U8(vec![0; pixels]),
        SampleType::I8 => BandSamples::I8(vec![0; pixels]),
        SampleType::U16 => BandSamples::U16(vec![0; pixels]),
        SampleType::I16 => BandSamples::I16(vec![0; pixels]),
        SampleType::U32 => BandSamples::U32(vec![0; pixels]),
        SampleType::I32 => BandSamples::I32(vec![0; pixels]),
        SampleType::F32 => BandSamples::F32(vec![0.0; pixels]),
        SampleType::F64 => BandSamples::F64(vec![0.0; pixels]),
    }
}

/// The top-left `cols x rows` window of a chunk mask `width` pixels wide.
pub fn crop_mask(mask: &[bool], width: usize, cols: usize, rows: usize) -> Vec<bool> {
    (0..rows)
        .flat_map(|row| (0..cols).map(move |col| row * width + col))
        .map(|at| mask.get(at).copied().unwrap_or(false))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunky_samples_split_and_crop() {
        // A 3-pixel-wide chunk of two big-endian u16 bands, 2 rows.
        let mut bytes = Vec::new();
        for v in [1u16, 10, 2, 20, 3, 30, 4, 40, 5, 50, 6, 60] {
            bytes.extend(v.to_be_bytes());
        }
        let chunk = RawChunk {
            bytes: &bytes,
            order: ByteOrder::Big,
            width: 3,
            samples: 2,
            sample_type: SampleType::U16,
        };
        assert_eq!(chunk.needed(2, 2), (3 + 2) * 2 * 2);
        assert_eq!(
            split_bands(&chunk, 2, 2),
            vec![
                BandSamples::U16(vec![1, 2, 4, 5]),
                BandSamples::U16(vec![10, 20, 40, 50])
            ]
        );
    }

    #[test]
    fn floats_and_masks_crop() {
        let bytes: Vec<u8> = [1.5f64, -2.0]
            .iter()
            .flat_map(|v| v.to_le_bytes())
            .collect();
        let chunk = RawChunk {
            bytes: &bytes,
            order: ByteOrder::Little,
            width: 2,
            samples: 1,
            sample_type: SampleType::F64,
        };
        assert_eq!(split_bands(&chunk, 1, 1), vec![BandSamples::F64(vec![1.5])]);
        assert_eq!(
            crop_mask(&[true, false, false, true], 2, 1, 2),
            vec![true, false]
        );
        assert_eq!(zeroed(SampleType::I8, 2), BandSamples::I8(vec![0, 0]));
    }
}
