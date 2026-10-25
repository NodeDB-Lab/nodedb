// SPDX-License-Identifier: Apache-2.0

//! Attribute payload for a single cell.
//!
//! A cell's value is one [`CellValue`] per attribute defined on the
//! schema. The schema's `attrs.len()` and ordering determine the
//! payload layout. A write checks a cell against the schema with
//! `ArraySchema::check_cell`.

use serde::{Deserialize, Serialize};

use crate::schema::attr_spec::AttrType;
use crate::types::cell_value::widen::widen_f32;

/// Typed attribute payload.
///
/// Keep this enum strictly in sync with [`crate::schema::AttrType`]:
/// every attribute type has exactly one cell variant.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum CellValue {
    Int64(i64),
    Float64(f64),
    /// UTF-8 attribute payload (variant calls, sample IDs, etc.).
    String(String),
    /// Opaque byte attribute (compressed BLOBs, raw read pileups).
    Bytes(Vec<u8>),
    /// Cell present but value absent — distinct from "not written".
    Null,
    UInt8(u8),
    Int16(i16),
    UInt16(u16),
    Int32(i32),
    UInt32(u32),
    Float32(f32),
    Int8(i8),
}

impl CellValue {
    pub fn is_null(&self) -> bool {
        matches!(self, CellValue::Null)
    }

    /// The attribute type this value belongs to. `Null` belongs to none.
    pub fn attr_type(&self) -> Option<AttrType> {
        match self {
            CellValue::Int64(_) => Some(AttrType::Int64),
            CellValue::Float64(_) => Some(AttrType::Float64),
            CellValue::String(_) => Some(AttrType::String),
            CellValue::Bytes(_) => Some(AttrType::Bytes),
            CellValue::Null => None,
            CellValue::UInt8(_) => Some(AttrType::UInt8),
            CellValue::Int16(_) => Some(AttrType::Int16),
            CellValue::UInt16(_) => Some(AttrType::UInt16),
            CellValue::Int32(_) => Some(AttrType::Int32),
            CellValue::UInt32(_) => Some(AttrType::UInt32),
            CellValue::Float32(_) => Some(AttrType::Float32),
            CellValue::Int8(_) => Some(AttrType::Int8),
        }
    }

    /// The value of an integer cell, widened to `i64`. `None` for every
    /// other cell.
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            CellValue::Int64(v) => Some(*v),
            CellValue::Int8(v) => Some(i64::from(*v)),
            CellValue::UInt8(v) => Some(i64::from(*v)),
            CellValue::Int16(v) => Some(i64::from(*v)),
            CellValue::UInt16(v) => Some(i64::from(*v)),
            CellValue::Int32(v) => Some(i64::from(*v)),
            CellValue::UInt32(v) => Some(i64::from(*v)),
            CellValue::Float64(_)
            | CellValue::Float32(_)
            | CellValue::String(_)
            | CellValue::Bytes(_)
            | CellValue::Null => None,
        }
    }

    /// The value of a float cell, widened to `f64`. A `Float32` widens
    /// through [`widen_f32`]. `None` for every other cell, integer cells
    /// included.
    pub fn as_float(&self) -> Option<f64> {
        match self {
            CellValue::Float64(v) => Some(*v),
            CellValue::Float32(v) => Some(widen_f32(*v)),
            CellValue::Int64(_)
            | CellValue::Int8(_)
            | CellValue::UInt8(_)
            | CellValue::Int16(_)
            | CellValue::UInt16(_)
            | CellValue::Int32(_)
            | CellValue::UInt32(_)
            | CellValue::String(_)
            | CellValue::Bytes(_)
            | CellValue::Null => None,
        }
    }

    /// The value of any numeric cell as `f64`. An `Int64` past 2^53 rounds.
    pub fn as_f64(&self) -> Option<f64> {
        match self.as_i64() {
            Some(v) => Some(v as f64),
            None => self.as_float(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cell_value_null_predicate() {
        assert!(CellValue::Null.is_null());
        assert!(!CellValue::Int64(0).is_null());
    }

    #[test]
    fn numeric_cells_widen() {
        assert_eq!(CellValue::UInt16(65535).as_i64(), Some(65535));
        assert_eq!(CellValue::Int16(-3).as_f64(), Some(-3.0));
        assert_eq!(CellValue::Float32(0.5).as_float(), Some(0.5));
        assert_eq!(CellValue::Float32(0.5).as_i64(), None);
        assert_eq!(CellValue::UInt8(1).attr_type(), Some(AttrType::UInt8));
        assert_eq!(CellValue::Int8(-1).attr_type(), Some(AttrType::Int8));
        assert_eq!(CellValue::Int8(-128).as_i64(), Some(-128));
        assert_eq!(CellValue::Null.attr_type(), None);
    }
}
