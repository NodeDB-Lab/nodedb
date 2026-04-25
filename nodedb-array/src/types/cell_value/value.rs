//! Attribute payload for a single cell.
//!
//! A cell's value is one [`CellValue`] per attribute defined on the
//! schema. The schema's `attrs.len()` and ordering determine the
//! payload layout — validation lives in
//! [`crate::schema::validation::attrs`].

use serde::{Deserialize, Serialize};

/// Typed attribute payload.
///
/// Keep this enum strictly in sync with [`crate::schema::AttrType`]:
/// every attribute type has exactly one cell variant.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum CellValue {
    Int64(i64),
    Float64(f64),
    /// UTF-8 attribute payload (variant calls, sample IDs, etc.).
    String(String),
    /// Opaque byte attribute (compressed BLOBs, raw read pileups).
    Bytes(Vec<u8>),
    /// Cell present but value absent — distinct from "not written".
    Null,
}

impl CellValue {
    pub fn is_null(&self) -> bool {
        matches!(self, CellValue::Null)
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
}
