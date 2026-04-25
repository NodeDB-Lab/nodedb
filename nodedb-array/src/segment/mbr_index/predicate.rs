//! `DomainBound` comparison + bbox intersection.
//!
//! `DomainBound` is `PartialOrd` only because Float64 carries NaN; in
//! practice `TileMBR::dim_mins/maxs` are produced by the MBR builder
//! and never contain NaN. We treat any non-`Some(Ordering)` from
//! `partial_cmp` as "not less than" — conservative, never prunes a
//! valid tile.

use super::node::BBox;
use crate::types::domain::DomainBound;

/// Strict less-than for two `DomainBound`s. `false` if not comparable.
pub fn lt_bound(a: &DomainBound, b: &DomainBound) -> bool {
    a.partial_cmp(b)
        .is_some_and(|o| o == std::cmp::Ordering::Less)
}

fn le_bound(a: &DomainBound, b: &DomainBound) -> bool {
    a.partial_cmp(b)
        .is_some_and(|o| o != std::cmp::Ordering::Greater)
}

/// Per-dim closed-interval predicate the caller supplies to query the
/// tree. `None` on either side means "unbounded".
#[derive(Debug, Clone, Default)]
pub struct MbrQueryPredicate {
    pub per_dim: Vec<DimPredicate>,
}

#[derive(Debug, Clone, Default)]
pub struct DimPredicate {
    pub lo: Option<DomainBound>,
    pub hi: Option<DomainBound>,
}

impl MbrQueryPredicate {
    pub fn new(per_dim: Vec<DimPredicate>) -> Self {
        Self { per_dim }
    }

    /// Does this predicate's per-dim range intersect `bbox`?
    ///
    /// True if for every dim where the predicate has a bound, the
    /// tile's [min, max] overlaps the predicate's [lo, hi]. Dims the
    /// predicate doesn't constrain are accepted by default. If the
    /// predicate has more dims than the bbox, extra dims are ignored
    /// (stale predicate after schema change → conservative pass).
    pub fn intersects(&self, bbox: &BBox) -> bool {
        for (i, dp) in self.per_dim.iter().enumerate() {
            if i >= bbox.arity() {
                break;
            }
            if let Some(lo) = &dp.lo
                && lt_bound(&bbox.max[i], lo)
            {
                return false;
            }
            if let Some(hi) = &dp.hi
                && lt_bound(hi, &bbox.min[i])
            {
                return false;
            }
            // bounds equal-touching is overlap — handled by `lt_bound`
            // returning false on equality.
            let _ = le_bound; // keep helper compiled for future symmetric checks
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn b(min: i64, max: i64) -> BBox {
        BBox {
            min: vec![DomainBound::Int64(min)],
            max: vec![DomainBound::Int64(max)],
        }
    }

    fn pred(lo: Option<i64>, hi: Option<i64>) -> MbrQueryPredicate {
        MbrQueryPredicate::new(vec![DimPredicate {
            lo: lo.map(DomainBound::Int64),
            hi: hi.map(DomainBound::Int64),
        }])
    }

    #[test]
    fn fully_inside_intersects() {
        assert!(pred(Some(2), Some(8)).intersects(&b(0, 10)));
    }

    #[test]
    fn fully_outside_left() {
        assert!(!pred(Some(20), Some(30)).intersects(&b(0, 10)));
    }

    #[test]
    fn fully_outside_right() {
        assert!(!pred(Some(-10), Some(-5)).intersects(&b(0, 10)));
    }

    #[test]
    fn touching_intersects() {
        assert!(pred(Some(10), Some(20)).intersects(&b(0, 10)));
    }

    #[test]
    fn unbounded_intersects() {
        assert!(pred(None, None).intersects(&b(5, 7)));
    }
}
