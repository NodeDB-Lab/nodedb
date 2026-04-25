//! Dimension specification.

use serde::{Deserialize, Serialize};

use crate::types::Domain;

/// Dimension type tag. Each variant has a one-to-one mapping with
/// [`crate::types::coord::value::CoordValue`] — the encoder dispatches
/// off this tag.
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
pub enum DimType {
    Int64,
    Float64,
    /// Wall-clock milliseconds since epoch — same wire shape as `Int64`
    /// but treated separately so SQL surfaces and analyzers can format
    /// it correctly.
    TimestampMs,
    String,
}

/// One dimension of an [`super::ArraySchema`].
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
pub struct DimSpec {
    pub name: String,
    pub dtype: DimType,
    pub domain: Domain,
}

impl DimSpec {
    pub fn new(name: impl Into<String>, dtype: DimType, domain: Domain) -> Self {
        Self {
            name: name.into(),
            dtype,
            domain,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::domain::DomainBound;

    #[test]
    fn dim_spec_round_trip_eq() {
        let d = DimSpec::new(
            "chrom",
            DimType::Int64,
            Domain::new(DomainBound::Int64(0), DomainBound::Int64(24)),
        );
        let e = d.clone();
        assert_eq!(d, e);
    }
}
