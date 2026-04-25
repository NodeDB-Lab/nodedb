//! Per-dimension closed interval `[lo, hi]`.
//!
//! Stored on each [`crate::schema::DimSpec`]. The interval is closed on
//! both ends — a coordinate `c` is in-domain iff `lo <= c <= hi`.

use serde::{Deserialize, Serialize};

/// Typed domain bound. Variant tag must match the dim's type.
#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum DomainBound {
    Int64(i64),
    Float64(f64),
    TimestampMs(i64),
    /// String-typed dims use lexicographic ordering on the bound.
    String(String),
}

impl Eq for DomainBound {}

/// Closed `[lo, hi]` interval over a single dimension.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Domain {
    pub lo: DomainBound,
    pub hi: DomainBound,
}

impl Domain {
    pub fn new(lo: DomainBound, hi: DomainBound) -> Self {
        Self { lo, hi }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn domain_round_trip_eq() {
        let d = Domain::new(DomainBound::Int64(0), DomainBound::Int64(1_000_000));
        let e = d.clone();
        assert_eq!(d, e);
    }
}
