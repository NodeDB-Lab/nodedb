//! Variable-arity ND coordinate.
//!
//! A `Coord` is one cell address: a tuple of per-dimension scalars.
//! Arity must match the schema's `dims.len()` — checked at
//! [`crate::schema::validation`] time, not constructed here.
//!
//! Each component is one of [`CoordValue`]'s typed variants so
//! Hilbert/Z-order encoders (Tier 1) can dispatch by `DimType` without
//! re-parsing.

use serde::{Deserialize, Serialize};

/// One per-dimension scalar.
///
/// Variants mirror [`crate::schema::DimType`]: every dim type has
/// exactly one coordinate variant. Strings are owned to keep `Coord`
/// `'static`-friendly for buffering in memtables.
#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum CoordValue {
    Int64(i64),
    Float64(f64),
    /// Wall-clock milliseconds since epoch.
    TimestampMs(i64),
    String(String),
}

impl Eq for CoordValue {}

impl std::hash::Hash for CoordValue {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            CoordValue::Int64(v) | CoordValue::TimestampMs(v) => v.hash(state),
            CoordValue::Float64(v) => v.to_bits().hash(state),
            CoordValue::String(v) => v.hash(state),
        }
    }
}

/// Variable-arity coordinate tuple.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Coord {
    components: Vec<CoordValue>,
}

impl Coord {
    pub fn new(components: Vec<CoordValue>) -> Self {
        Self { components }
    }

    pub fn arity(&self) -> usize {
        self.components.len()
    }

    pub fn components(&self) -> &[CoordValue] {
        &self.components
    }

    pub fn into_components(self) -> Vec<CoordValue> {
        self.components
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn coord_arity_reports_component_count() {
        let c = Coord::new(vec![
            CoordValue::Int64(7),
            CoordValue::Int64(42),
            CoordValue::String("ALT".into()),
        ]);
        assert_eq!(c.arity(), 3);
    }

    #[test]
    fn coord_value_equality_handles_floats() {
        let a = CoordValue::Float64(1.5);
        let b = CoordValue::Float64(1.5);
        assert_eq!(a, b);
    }
}
