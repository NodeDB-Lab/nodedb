//! Attribute specification.

use serde::{Deserialize, Serialize};

/// Attribute type tag. One-to-one with
/// [`crate::types::cell_value::value::CellValue`] (excluding `Null`,
/// which is per-cell, not per-attribute).
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
}

impl AttrSpec {
    pub fn new(name: impl Into<String>, dtype: AttrType, nullable: bool) -> Self {
        Self {
            name: name.into(),
            dtype,
            nullable,
        }
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
}
