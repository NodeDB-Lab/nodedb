// SPDX-License-Identifier: Apache-2.0

//! A nodata value typed by its band's sample type.

use nodedb_raster::{BandSamples, SampleType};

use crate::error::RasterLoadError;

/// The nodata value, typed by the sample type.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Nodata {
    Int(i64),
    F32(f32),
    F64(f64),
}

impl Nodata {
    /// Whether sample `index` of `band` is nodata. NaN matches NaN.
    pub fn matches(self, band: &BandSamples, index: usize) -> bool {
        let int = |n: i64, v: Option<i64>| v == Some(n);
        match (self, band) {
            (Nodata::Int(n), BandSamples::U8(v)) => int(n, v.get(index).map(|s| i64::from(*s))),
            (Nodata::Int(n), BandSamples::I8(v)) => int(n, v.get(index).map(|s| i64::from(*s))),
            (Nodata::Int(n), BandSamples::U16(v)) => int(n, v.get(index).map(|s| i64::from(*s))),
            (Nodata::Int(n), BandSamples::I16(v)) => int(n, v.get(index).map(|s| i64::from(*s))),
            (Nodata::Int(n), BandSamples::U32(v)) => int(n, v.get(index).map(|s| i64::from(*s))),
            (Nodata::Int(n), BandSamples::I32(v)) => int(n, v.get(index).map(|s| i64::from(*s))),
            (Nodata::F32(n), BandSamples::F32(v)) => v
                .get(index)
                .is_some_and(|s| *s == n || (s.is_nan() && n.is_nan())),
            (Nodata::F64(n), BandSamples::F64(v)) => v
                .get(index)
                .is_some_and(|s| *s == n || (s.is_nan() && n.is_nan())),
            (
                Nodata::Int(_) | Nodata::F32(_) | Nodata::F64(_),
                BandSamples::U8(_)
                | BandSamples::I8(_)
                | BandSamples::U16(_)
                | BandSamples::I16(_)
                | BandSamples::U32(_)
                | BandSamples::I32(_)
                | BandSamples::F32(_)
                | BandSamples::F64(_),
            ) => false,
        }
    }
}

/// `value` as a nodata of `sample`. An integer type needs a whole value in
/// its range. `f32` takes the nearest `f32`, as GDAL compares.
pub fn typed_nodata(value: f64, sample: SampleType) -> Result<Nodata, RasterLoadError> {
    let range = match sample {
        SampleType::U8 => (f64::from(u8::MIN), f64::from(u8::MAX)),
        SampleType::I8 => (f64::from(i8::MIN), f64::from(i8::MAX)),
        SampleType::U16 => (f64::from(u16::MIN), f64::from(u16::MAX)),
        SampleType::I16 => (f64::from(i16::MIN), f64::from(i16::MAX)),
        SampleType::U32 => (f64::from(u32::MIN), f64::from(u32::MAX)),
        SampleType::I32 => (f64::from(i32::MIN), f64::from(i32::MAX)),
        SampleType::F32 if value.is_finite() && value.abs() > f64::from(f32::MAX) => {
            return Err(RasterLoadError::Nodata { value, sample });
        }
        SampleType::F32 => return Ok(Nodata::F32(value as f32)),
        SampleType::F64 => return Ok(Nodata::F64(value)),
    };
    let (lo, hi) = range;
    if value.fract() == 0.0 && lo <= value && value <= hi {
        Ok(Nodata::Int(value as i64))
    } else {
        Err(RasterLoadError::Nodata { value, sample })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn integer_nodata_must_be_a_sample_value() {
        assert_eq!(typed_nodata(0.0, SampleType::U16), Ok(Nodata::Int(0)));
        assert_eq!(
            typed_nodata(-9999.0, SampleType::I16),
            Ok(Nodata::Int(-9999))
        );
        for (value, sample) in [
            (-1.0, SampleType::U16),
            (65536.0, SampleType::U16),
            (0.5, SampleType::U8),
            (f64::NAN, SampleType::U32),
            (1e39, SampleType::F32),
        ] {
            let err = typed_nodata(value, sample).expect_err("unrepresentable");
            assert_eq!(err.sqlstate(), "22023", "{value} as {sample}");
        }
    }

    #[test]
    fn nan_nodata_matches_nan_samples() {
        let band = BandSamples::F32(vec![f32::NAN, 1.0]);
        let nodata = typed_nodata(f64::NAN, SampleType::F32).expect("NaN is an f32 value");
        assert!(nodata.matches(&band, 0));
        assert!(!nodata.matches(&band, 1));
        let ints = BandSamples::U16(vec![0, 7]);
        assert!(Nodata::Int(0).matches(&ints, 0));
        assert!(!Nodata::Int(0).matches(&ints, 1));
    }
}
