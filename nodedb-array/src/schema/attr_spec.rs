// SPDX-License-Identifier: Apache-2.0

//! Attribute specification.

use serde::{Deserialize, Serialize};

use super::attr_meta::AttrMeta;

/// Attribute type tag. One-to-one with
/// [`crate::types::cell_value::value::CellValue`] (excluding `Null`,
/// which is per-cell, not per-attribute).
///
/// The narrow integer and `Float32` types store a cell at its declared
/// width. A value outside the width is refused at write time, never
/// wrapped or saturated.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[serde(rename_all = "snake_case")]
#[msgpack(c_enum)]
pub enum AttrType {
    Int64,
    Float64,
    String,
    Bytes,
    #[serde(rename = "uint8")]
    UInt8,
    Int16,
    #[serde(rename = "uint16")]
    UInt16,
    Int32,
    #[serde(rename = "uint32")]
    UInt32,
    Float32,
    /// A 1-byte signed integer. The array DDL names widths in bits, so
    /// `INT8` is this type, not PostgreSQL's 8-byte `int8`.
    #[serde(rename = "int8")]
    Int8,
}

impl AttrType {
    /// The canonical DDL name of the type.
    pub fn sql_name(self) -> &'static str {
        match self {
            AttrType::Int64 => "INT64",
            AttrType::Float64 => "FLOAT64",
            AttrType::String => "STRING",
            AttrType::Bytes => "BYTES",
            AttrType::UInt8 => "UINT8",
            AttrType::Int16 => "INT16",
            AttrType::UInt16 => "UINT16",
            AttrType::Int32 => "INT32",
            AttrType::UInt32 => "UINT32",
            AttrType::Float32 => "FLOAT32",
            AttrType::Int8 => "INT8",
        }
    }

    /// The inclusive range an integer type holds, or `None` for a
    /// non-integer type.
    pub fn int_range(self) -> Option<(i64, i64)> {
        match self {
            AttrType::Int64 => Some((i64::MIN, i64::MAX)),
            AttrType::Int8 => Some((i64::from(i8::MIN), i64::from(i8::MAX))),
            AttrType::UInt8 => Some((0, i64::from(u8::MAX))),
            AttrType::Int16 => Some((i64::from(i16::MIN), i64::from(i16::MAX))),
            AttrType::UInt16 => Some((0, i64::from(u16::MAX))),
            AttrType::Int32 => Some((i64::from(i32::MIN), i64::from(i32::MAX))),
            AttrType::UInt32 => Some((0, i64::from(u32::MAX))),
            AttrType::Float64 | AttrType::Float32 | AttrType::String | AttrType::Bytes => None,
        }
    }

    /// Whether the type is `Float32` or `Float64`.
    pub fn is_float(self) -> bool {
        match self {
            AttrType::Float64 | AttrType::Float32 => true,
            AttrType::Int64
            | AttrType::Int8
            | AttrType::UInt8
            | AttrType::Int16
            | AttrType::UInt16
            | AttrType::Int32
            | AttrType::UInt32
            | AttrType::String
            | AttrType::Bytes => false,
        }
    }
}

/// One attribute column on an [`super::ArraySchema`].
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct AttrSpec {
    pub name: String,
    pub dtype: AttrType,
    /// Whether the attribute may carry [`crate::types::CellValue::Null`].
    pub nullable: bool,
    /// Nodata, scale, offset and units. Not part of
    /// [`super::ArraySchema::content_msgpack`], so it never changes
    /// `schema_hash`.
    pub meta: AttrMeta,
}

impl AttrSpec {
    pub fn new(name: impl Into<String>, dtype: AttrType, nullable: bool) -> Self {
        Self {
            name: name.into(),
            dtype,
            nullable,
            meta: AttrMeta::default(),
        }
    }

    /// This attribute with `meta` in place of its current metadata.
    pub fn with_meta(self, meta: AttrMeta) -> Self {
        Self { meta, ..self }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn attr_spec_round_trip_eq() {
        let a = AttrSpec::new("qual", AttrType::Float64, true);
        let b = a.clone();
        assert_eq!(a, b);
    }

    #[test]
    fn integer_ranges_match_their_widths() {
        assert_eq!(AttrType::UInt8.int_range(), Some((0, 255)));
        assert_eq!(AttrType::Int8.int_range(), Some((-128, 127)));
        assert_eq!(AttrType::Int8.sql_name(), "INT8");
        assert_eq!(AttrType::Int16.int_range(), Some((-32768, 32767)));
        assert_eq!(AttrType::UInt16.int_range(), Some((0, 65535)));
        assert_eq!(AttrType::UInt32.int_range(), Some((0, 4_294_967_295)));
        assert_eq!(AttrType::Float32.int_range(), None);
        assert!(AttrType::Float32.is_float());
        assert!(!AttrType::Int32.is_float());
    }
}
