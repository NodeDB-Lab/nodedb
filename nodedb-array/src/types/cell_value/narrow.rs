// SPDX-License-Identifier: Apache-2.0

//! Narrowing a numeric literal to an attribute's declared type.
//!
//! An integer outside the declared width is refused. A float narrows to
//! `f32` by rounding, and only a finite value that rounds to infinity is
//! refused. Both refusals are SQLSTATE `22003` at the SQL surface.

use thiserror::Error;

use crate::schema::attr_spec::AttrType;
use crate::types::cell_value::value::CellValue;

/// Why a literal does not fit an attribute type.
#[derive(Debug, Clone, PartialEq, Error)]
pub enum NarrowError {
    /// The value is outside the type's range.
    #[error("value {value} is out of range for type {dtype}")]
    OutOfRange { value: String, dtype: &'static str },
    /// The literal's kind cannot be stored in the type at all.
    #[error("a {got} value cannot be stored in a {dtype} attribute")]
    WrongKind {
        got: &'static str,
        dtype: &'static str,
    },
}

impl NarrowError {
    /// The array error for this refusal on attribute `attr` of `array`.
    pub fn for_attr(self, array: &str, attr: &str) -> crate::error::ArrayError {
        crate::error::ArrayError::Narrow {
            array: array.to_string(),
            attr: attr.to_string(),
            source: self,
        }
    }

    /// Whether the value is out of range, SQLSTATE `22003`, as opposed to a
    /// kind the type never holds.
    pub fn is_out_of_range(&self) -> bool {
        match self {
            NarrowError::OutOfRange { .. } => true,
            NarrowError::WrongKind { .. } => false,
        }
    }
}

impl CellValue {
    /// The cell an integer literal stores as in a `dtype` attribute.
    pub fn from_int_literal(v: i64, dtype: AttrType) -> Result<CellValue, NarrowError> {
        let out_of_range = || NarrowError::OutOfRange {
            value: v.to_string(),
            dtype: dtype.sql_name(),
        };
        match dtype {
            AttrType::Int64 => Ok(CellValue::Int64(v)),
            AttrType::Int8 => i8::try_from(v)
                .map(CellValue::Int8)
                .map_err(|_| out_of_range()),
            AttrType::UInt8 => u8::try_from(v)
                .map(CellValue::UInt8)
                .map_err(|_| out_of_range()),
            AttrType::Int16 => i16::try_from(v)
                .map(CellValue::Int16)
                .map_err(|_| out_of_range()),
            AttrType::UInt16 => u16::try_from(v)
                .map(CellValue::UInt16)
                .map_err(|_| out_of_range()),
            AttrType::Int32 => i32::try_from(v)
                .map(CellValue::Int32)
                .map_err(|_| out_of_range()),
            AttrType::UInt32 => u32::try_from(v)
                .map(CellValue::UInt32)
                .map_err(|_| out_of_range()),
            AttrType::Float64 => Ok(CellValue::Float64(v as f64)),
            AttrType::Float32 => Ok(CellValue::Float32(v as f32)),
            AttrType::String | AttrType::Bytes => Err(NarrowError::WrongKind {
                got: "integer",
                dtype: dtype.sql_name(),
            }),
        }
    }

    /// The cell a float literal stores as in a `dtype` attribute.
    pub fn from_float_literal(v: f64, dtype: AttrType) -> Result<CellValue, NarrowError> {
        match dtype {
            AttrType::Float64 => Ok(CellValue::Float64(v)),
            AttrType::Float32 => {
                let narrowed = v as f32;
                if v.is_finite() && narrowed.is_infinite() {
                    return Err(NarrowError::OutOfRange {
                        value: v.to_string(),
                        dtype: dtype.sql_name(),
                    });
                }
                Ok(CellValue::Float32(narrowed))
            }
            AttrType::Int64
            | AttrType::Int8
            | AttrType::UInt8
            | AttrType::Int16
            | AttrType::UInt16
            | AttrType::Int32
            | AttrType::UInt32
            | AttrType::String
            | AttrType::Bytes => Err(NarrowError::WrongKind {
                got: "float",
                dtype: dtype.sql_name(),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn integers_store_at_their_width() {
        assert_eq!(
            CellValue::from_int_literal(255, AttrType::UInt8),
            Ok(CellValue::UInt8(255))
        );
        assert_eq!(
            CellValue::from_int_literal(-32768, AttrType::Int16),
            Ok(CellValue::Int16(-32768))
        );
        assert_eq!(
            CellValue::from_int_literal(65535, AttrType::UInt16),
            Ok(CellValue::UInt16(65535))
        );
        assert_eq!(
            CellValue::from_int_literal(4_294_967_295, AttrType::UInt32),
            Ok(CellValue::UInt32(u32::MAX))
        );
        assert_eq!(
            CellValue::from_int_literal(-128, AttrType::Int8),
            Ok(CellValue::Int8(-128))
        );
        assert_eq!(
            CellValue::from_int_literal(3, AttrType::Float32),
            Ok(CellValue::Float32(3.0))
        );
    }

    #[test]
    fn integers_past_the_width_are_refused() {
        let cases = [
            (256, AttrType::UInt8),
            (-1, AttrType::UInt8),
            (32768, AttrType::Int16),
            (-32769, AttrType::Int16),
            (65536, AttrType::UInt16),
            (-1, AttrType::UInt16),
            (2_147_483_648, AttrType::Int32),
            (4_294_967_296, AttrType::UInt32),
            (-1, AttrType::UInt32),
            (128, AttrType::Int8),
            (-129, AttrType::Int8),
        ];
        for (v, dtype) in cases {
            assert!(
                matches!(
                    CellValue::from_int_literal(v, dtype),
                    Err(NarrowError::OutOfRange { .. })
                ),
                "{v} into {dtype:?}"
            );
        }
    }

    #[test]
    fn floats_round_into_float32() {
        assert_eq!(
            CellValue::from_float_literal(0.1, AttrType::Float32),
            Ok(CellValue::Float32(0.1))
        );
        assert!(matches!(
            CellValue::from_float_literal(1e39, AttrType::Float32),
            Err(NarrowError::OutOfRange { .. })
        ));
        assert!(matches!(
            CellValue::from_float_literal(f64::INFINITY, AttrType::Float32),
            Ok(CellValue::Float32(v)) if v.is_infinite()
        ));
    }

    #[test]
    fn a_float_is_never_an_integer() {
        assert!(matches!(
            CellValue::from_float_literal(1.0, AttrType::Int16),
            Err(NarrowError::WrongKind { .. })
        ));
        assert!(matches!(
            CellValue::from_int_literal(1, AttrType::String),
            Err(NarrowError::WrongKind { .. })
        ));
    }
}
