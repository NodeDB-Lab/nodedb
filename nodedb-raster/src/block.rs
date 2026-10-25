// SPDX-License-Identifier: Apache-2.0

//! Decoded blocks: one typed, row-major sample vector per band.

use crate::layout::BlockWindow;
use crate::sample::SampleType;

/// One band's samples for one block, row-major, `width * height` long.
#[derive(Debug, Clone, PartialEq)]
pub enum BandSamples {
    U8(Vec<u8>),
    I8(Vec<i8>),
    U16(Vec<u16>),
    I16(Vec<i16>),
    U32(Vec<u32>),
    I32(Vec<i32>),
    F32(Vec<f32>),
    F64(Vec<f64>),
}

impl BandSamples {
    /// Number of samples.
    pub fn len(&self) -> usize {
        match self {
            BandSamples::U8(v) => v.len(),
            BandSamples::I8(v) => v.len(),
            BandSamples::U16(v) => v.len(),
            BandSamples::I16(v) => v.len(),
            BandSamples::U32(v) => v.len(),
            BandSamples::I32(v) => v.len(),
            BandSamples::F32(v) => v.len(),
            BandSamples::F64(v) => v.len(),
        }
    }

    /// True when the band holds no sample.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The sample type.
    pub fn sample_type(&self) -> SampleType {
        match self {
            BandSamples::U8(_) => SampleType::U8,
            BandSamples::I8(_) => SampleType::I8,
            BandSamples::U16(_) => SampleType::U16,
            BandSamples::I16(_) => SampleType::I16,
            BandSamples::U32(_) => SampleType::U32,
            BandSamples::I32(_) => SampleType::I32,
            BandSamples::F32(_) => SampleType::F32,
            BandSamples::F64(_) => SampleType::F64,
        }
    }

    /// Sample `index` widened to `f64`. Every supported type widens
    /// exactly.
    pub fn value_f64(&self, index: usize) -> Option<f64> {
        match self {
            BandSamples::U8(v) => v.get(index).map(|s| f64::from(*s)),
            BandSamples::I8(v) => v.get(index).map(|s| f64::from(*s)),
            BandSamples::U16(v) => v.get(index).map(|s| f64::from(*s)),
            BandSamples::I16(v) => v.get(index).map(|s| f64::from(*s)),
            BandSamples::U32(v) => v.get(index).map(|s| f64::from(*s)),
            BandSamples::I32(v) => v.get(index).map(|s| f64::from(*s)),
            BandSamples::F32(v) => v.get(index).map(|s| f64::from(*s)),
            BandSamples::F64(v) => v.get(index).copied(),
        }
    }
}

/// One decoded block: its window, one sample vector per band, and one
/// validity mask per band.
#[derive(Debug, Clone, PartialEq)]
pub struct RasterBlock {
    pub index: u32,
    pub window: BlockWindow,
    pub bands: Vec<BandSamples>,
    /// One mask per band, in band order: one flag per window pixel,
    /// row-major. `false` marks a sample that holds no data: masked by an
    /// internal mask (every band), invalid in the band's LERC mask, or inside
    /// a block the file omits. `None` when every pixel of the band is valid.
    pub masks: Vec<Option<Vec<bool>>>,
}

impl RasterBlock {
    /// Whether band `band` holds data at pixel `index` of the window.
    pub fn band_valid(&self, band: usize, index: usize) -> bool {
        match self.masks.get(band) {
            Some(Some(mask)) => mask.get(index).copied().unwrap_or(false),
            Some(None) | None => index < self.window.pixels(),
        }
    }

    /// Whether any band holds data at pixel `index` of the window.
    pub fn any_valid(&self, index: usize) -> bool {
        (0..self.bands.len()).any(|band| self.band_valid(band, index))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn values_widen_to_f64() {
        let band = BandSamples::I16(vec![-7, 3]);
        assert_eq!(band.value_f64(0), Some(-7.0));
        assert_eq!(band.value_f64(2), None);
        assert_eq!(band.sample_type(), SampleType::I16);
        assert_eq!(band.len(), 2);
        let wide = BandSamples::I32(vec![i32::MIN]);
        assert_eq!(wide.value_f64(0), Some(f64::from(i32::MIN)));
    }

    #[test]
    fn masks_mark_validity_per_band() {
        let window = BlockWindow {
            outer: Vec::new(),
            col_off: 0,
            row_off: 0,
            width: 2,
            height: 1,
        };
        let mut block = RasterBlock {
            index: 0,
            window,
            bands: vec![BandSamples::U8(vec![1, 2]), BandSamples::U8(vec![3, 4])],
            masks: vec![None, None],
        };
        assert!(block.band_valid(0, 0) && block.band_valid(1, 1));
        assert!(!block.band_valid(0, 2));
        block.masks = vec![Some(vec![true, false]), Some(vec![false, false])];
        assert!(block.band_valid(0, 0));
        assert!(!block.band_valid(0, 1));
        assert!(!block.band_valid(1, 0));
        assert!(block.any_valid(0));
        assert!(!block.any_valid(1));
    }
}
