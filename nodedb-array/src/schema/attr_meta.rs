// SPDX-License-Identifier: Apache-2.0

//! Reading metadata of one attribute: nodata, scale, offset, units.
//!
//! A stored cell holds the raw value. The physical value of a numeric cell
//! is `raw * scale + offset`, with a missing `scale` read as 1 and a
//! missing `offset` read as 0. A cell equal to `nodata` holds no data.
//!
//! The metadata stays out of [`super::ArraySchema::content_msgpack`], so
//! setting or dropping it never changes `schema_hash`.

use serde::{Deserialize, Serialize};

use super::attr_spec::AttrType;
use crate::types::cell_value::value::CellValue;

/// Optional reading metadata of an attribute. Every field `None` means
/// the attribute carries none.
#[derive(
    Debug, Clone, Default, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct AttrMeta {
    /// The raw value that marks a cell as holding no data.
    pub nodata: Option<f64>,
    /// The factor a reader multiplies a raw value by.
    pub scale: Option<f64>,
    /// The term a reader adds after the scale.
    pub offset: Option<f64>,
    /// The unit of the physical value, such as `reflectance`.
    pub units: Option<String>,
}

/// Bit equality, so a NaN nodata equals itself.
impl PartialEq for AttrMeta {
    fn eq(&self, other: &Self) -> bool {
        let bits = |v: Option<f64>| v.map(f64::to_bits);
        bits(self.nodata) == bits(other.nodata)
            && bits(self.scale) == bits(other.scale)
            && bits(self.offset) == bits(other.offset)
            && self.units == other.units
    }
}

impl Eq for AttrMeta {}

impl AttrMeta {
    /// Whether no field is set.
    pub fn is_empty(&self) -> bool {
        self.nodata.is_none()
            && self.scale.is_none()
            && self.offset.is_none()
            && self.units.is_none()
    }

    /// The `(scale, offset)` a reader applies, or `None` when neither is set.
    pub fn transform(&self) -> Option<(f64, f64)> {
        if self.scale.is_none() && self.offset.is_none() {
            return None;
        }
        Some((self.scale.unwrap_or(1.0), self.offset.unwrap_or(0.0)))
    }

    /// Whether `v` equals the nodata value.
    ///
    /// An integer cell matches a whole nodata exactly. A `FLOAT32` cell
    /// matches the nearest `f32` of the nodata, as GDAL compares. NaN
    /// matches NaN. NULL, `STRING` and `BYTES` cells never match.
    pub fn is_nodata(&self, v: &CellValue) -> bool {
        let Some(nodata) = self.nodata else {
            return false;
        };
        match v {
            CellValue::Float64(f) => *f == nodata || (f.is_nan() && nodata.is_nan()),
            CellValue::Float32(f) => {
                let n = nodata as f32;
                *f == n || (f.is_nan() && n.is_nan())
            }
            CellValue::Int64(_)
            | CellValue::Int8(_)
            | CellValue::UInt8(_)
            | CellValue::Int16(_)
            | CellValue::UInt16(_)
            | CellValue::Int32(_)
            | CellValue::UInt32(_) => match (v.as_i64(), whole_i64(nodata)) {
                (Some(cell), Some(n)) => cell == n,
                _ => false,
            },
            CellValue::String(_) | CellValue::Bytes(_) | CellValue::Null => false,
        }
    }

    /// The physical value of `v`: NULL for a nodata cell, `raw * scale +
    /// offset` as `FLOAT64` when a transform is set, and `v` unchanged
    /// otherwise.
    pub fn physical_cell(&self, v: CellValue) -> CellValue {
        if self.is_nodata(&v) {
            return CellValue::Null;
        }
        match (self.transform(), v.as_f64()) {
            (Some((scale, offset)), Some(raw)) => CellValue::Float64(raw * scale + offset),
            _ => v,
        }
    }

    /// Check every set field against attribute type `dtype`.
    ///
    /// - `NODATA`, `SCALE` and `OFFSET` apply to numeric attributes only.
    /// - `SCALE` is finite and non-zero. `OFFSET` is finite.
    /// - `NODATA` is a value of `dtype`: a whole number in range for an
    ///   integer type, and a value inside the `f32` range (or NaN or an
    ///   infinity) for `FLOAT32`.
    /// - `UNITS` is non-empty.
    pub fn check(&self, dtype: AttrType) -> Result<(), AttrMetaError> {
        let numeric = dtype.int_range().is_some() || dtype.is_float();
        if !numeric && (self.nodata.is_some() || self.scale.is_some() || self.offset.is_some()) {
            return Err(AttrMetaError::NotNumeric {
                dtype: dtype.sql_name(),
            });
        }
        if let Some(scale) = self.scale
            && (!scale.is_finite() || scale == 0.0)
        {
            return Err(AttrMetaError::Scale { scale });
        }
        if let Some(offset) = self.offset
            && !offset.is_finite()
        {
            return Err(AttrMetaError::Offset { offset });
        }
        if let Some(nodata) = self.nodata
            && !nodata_fits(nodata, dtype)
        {
            return Err(AttrMetaError::Nodata {
                nodata,
                dtype: dtype.sql_name(),
            });
        }
        if self.units.as_deref().is_some_and(str::is_empty) {
            return Err(AttrMetaError::EmptyUnits);
        }
        Ok(())
    }
}

/// Why attr metadata does not fit its attribute.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum AttrMetaError {
    #[error("NODATA, SCALE and OFFSET apply to numeric attributes, not {dtype}")]
    NotNumeric { dtype: &'static str },
    #[error("SCALE {scale} must be finite and non-zero")]
    Scale { scale: f64 },
    #[error("OFFSET {offset} must be finite")]
    Offset { offset: f64 },
    #[error("NODATA {nodata} is not a {dtype} value, so no cell can match it")]
    Nodata { nodata: f64, dtype: &'static str },
    #[error("UNITS must not be empty; drop it with DROP UNITS")]
    EmptyUnits,
}

/// `v` as an `i64` when it is a whole number inside the `i64` range.
fn whole_i64(v: f64) -> Option<i64> {
    // 2^63 is exact in f64. Every whole f64 in [-2^63, 2^63) is an i64.
    const TWO_POW_63: f64 = 9_223_372_036_854_775_808.0;
    (v.fract() == 0.0 && (-TWO_POW_63..TWO_POW_63).contains(&v)).then_some(v as i64)
}

/// Whether `nodata` is a value of `dtype`.
fn nodata_fits(nodata: f64, dtype: AttrType) -> bool {
    match dtype {
        AttrType::Float64 => true,
        AttrType::Float32 => !nodata.is_finite() || nodata.abs() <= f64::from(f32::MAX),
        AttrType::Int64
        | AttrType::Int8
        | AttrType::UInt8
        | AttrType::Int16
        | AttrType::UInt16
        | AttrType::Int32
        | AttrType::UInt32 => match (whole_i64(nodata), dtype.int_range()) {
            (Some(n), Some((lo, hi))) => lo <= n && n <= hi,
            _ => false,
        },
        AttrType::String | AttrType::Bytes => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta(nodata: Option<f64>, scale: Option<f64>, offset: Option<f64>) -> AttrMeta {
        AttrMeta {
            nodata,
            scale,
            offset,
            units: None,
        }
    }

    #[test]
    fn nodata_must_be_a_value_of_the_type() {
        assert!(meta(Some(0.0), None, None).check(AttrType::UInt16).is_ok());
        assert!(meta(Some(-128.0), None, None).check(AttrType::Int8).is_ok());
        assert!(
            meta(Some(f64::NAN), None, None)
                .check(AttrType::Float32)
                .is_ok()
        );
        for (nodata, dtype) in [
            (-1.0, AttrType::UInt16),
            (65_536.0, AttrType::UInt16),
            (0.5, AttrType::UInt8),
            (128.0, AttrType::Int8),
            (f64::NAN, AttrType::Int32),
            (1e39, AttrType::Float32),
            (9_223_372_036_854_775_808.0, AttrType::Int64),
        ] {
            assert!(
                meta(Some(nodata), None, None).check(dtype).is_err(),
                "{nodata} as {dtype:?}"
            );
        }
    }

    #[test]
    fn scale_offset_and_units_are_checked() {
        assert!(meta(None, Some(0.0), None).check(AttrType::Int16).is_err());
        assert!(
            meta(None, Some(f64::INFINITY), None)
                .check(AttrType::Int16)
                .is_err()
        );
        assert!(
            meta(None, None, Some(f64::NAN))
                .check(AttrType::Int16)
                .is_err()
        );
        assert!(meta(Some(0.0), None, None).check(AttrType::String).is_err());
        let empty_units = AttrMeta {
            units: Some(String::new()),
            ..AttrMeta::default()
        };
        assert!(empty_units.check(AttrType::Float64).is_err());
        let text_units = AttrMeta {
            units: Some("label".into()),
            ..AttrMeta::default()
        };
        assert!(text_units.check(AttrType::String).is_ok());
    }

    #[test]
    fn nodata_matches_by_type() {
        let ints = meta(Some(0.0), None, None);
        assert!(ints.is_nodata(&CellValue::UInt16(0)));
        assert!(!ints.is_nodata(&CellValue::UInt16(1)));
        assert!(!ints.is_nodata(&CellValue::Null));
        let neg = meta(Some(-9999.0), None, None);
        assert!(neg.is_nodata(&CellValue::Int16(-9999)));
        assert!(neg.is_nodata(&CellValue::Float32(-9999.0)));
        let tenth = meta(Some(0.1), None, None);
        assert!(tenth.is_nodata(&CellValue::Float32(0.1)));
        assert!(!tenth.is_nodata(&CellValue::Int64(0)));
        let nan = meta(Some(f64::NAN), None, None);
        assert!(nan.is_nodata(&CellValue::Float64(f64::NAN)));
        assert!(nan.is_nodata(&CellValue::Float32(f32::NAN)));
        assert!(!nan.is_nodata(&CellValue::Float64(1.0)));
    }

    #[test]
    fn physical_cells_apply_scale_and_offset() {
        let landsat = meta(Some(0.0), Some(0.0000275), Some(-0.2));
        assert_eq!(landsat.physical_cell(CellValue::UInt16(0)), CellValue::Null);
        let CellValue::Float64(v) = landsat.physical_cell(CellValue::UInt16(10_000)) else {
            panic!("a scaled cell is FLOAT64");
        };
        assert!((v - 0.075).abs() < 1e-12, "{v}");
        let plain = meta(Some(0.0), None, None);
        assert_eq!(
            plain.physical_cell(CellValue::UInt16(7)),
            CellValue::UInt16(7)
        );
        assert_eq!(plain.physical_cell(CellValue::Null), CellValue::Null);
    }

    #[test]
    fn nan_nodata_equals_itself() {
        let a = meta(Some(f64::NAN), None, None);
        assert_eq!(a, a.clone());
        assert_ne!(a, meta(None, None, None));
        assert!(AttrMeta::default().is_empty());
    }
}
