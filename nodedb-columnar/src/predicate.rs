//! Scan predicates for block-level predicate pushdown.
//!
//! A `ScanPredicate` describes a filter on a single column that can be
//! evaluated against `BlockStats` to skip entire blocks without decompressing.

use crate::format::BlockStats;

/// A predicate on a single column for block-level pushdown.
#[derive(Debug, Clone)]
pub struct ScanPredicate {
    /// Column index in the schema.
    pub col_idx: usize,
    /// The comparison operation.
    pub op: PredicateOp,
    /// The threshold value (as f64 for uniform comparison against BlockStats).
    pub value: f64,
}

/// Comparison operator for scan predicates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PredicateOp {
    /// `column > value`
    Gt,
    /// `column >= value`
    Gte,
    /// `column < value`
    Lt,
    /// `column <= value`
    Lte,
    /// `column = value`
    Eq,
    /// `column != value`
    Ne,
}

impl ScanPredicate {
    /// Create a predicate: column > value.
    pub fn gt(col_idx: usize, value: f64) -> Self {
        Self {
            col_idx,
            op: PredicateOp::Gt,
            value,
        }
    }

    /// Create a predicate: column >= value.
    pub fn gte(col_idx: usize, value: f64) -> Self {
        Self {
            col_idx,
            op: PredicateOp::Gte,
            value,
        }
    }

    /// Create a predicate: column < value.
    pub fn lt(col_idx: usize, value: f64) -> Self {
        Self {
            col_idx,
            op: PredicateOp::Lt,
            value,
        }
    }

    /// Create a predicate: column <= value.
    pub fn lte(col_idx: usize, value: f64) -> Self {
        Self {
            col_idx,
            op: PredicateOp::Lte,
            value,
        }
    }

    /// Create a predicate: column = value.
    pub fn eq(col_idx: usize, value: f64) -> Self {
        Self {
            col_idx,
            op: PredicateOp::Eq,
            value,
        }
    }

    /// Create a predicate: column != value.
    /// Named `not_eq` to avoid conflict with `PartialEq::ne`.
    pub fn not_eq(col_idx: usize, value: f64) -> Self {
        Self {
            col_idx,
            op: PredicateOp::Ne,
            value,
        }
    }

    /// Whether a block can be entirely skipped based on its statistics.
    ///
    /// Returns `true` if the block provably contains no matching rows.
    /// Returns `false` if the block might contain matching rows (must scan).
    pub fn can_skip_block(&self, stats: &BlockStats) -> bool {
        // Non-numeric columns (NaN stats) can never be skipped.
        if stats.min.is_nan() || stats.max.is_nan() {
            return false;
        }

        match self.op {
            // column > value → skip if block.max <= value
            PredicateOp::Gt => stats.max <= self.value,
            // column >= value → skip if block.max < value
            PredicateOp::Gte => stats.max < self.value,
            // column < value → skip if block.min >= value
            PredicateOp::Lt => stats.min >= self.value,
            // column <= value → skip if block.min > value
            PredicateOp::Lte => stats.min > self.value,
            // column = value → skip if value outside [min, max]
            PredicateOp::Eq => self.value < stats.min || self.value > stats.max,
            // column != value → skip only if entire block is that single value
            PredicateOp::Ne => stats.min == self.value && stats.max == self.value,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stats(min: f64, max: f64) -> BlockStats {
        BlockStats {
            min,
            max,
            null_count: 0,
            row_count: 1024,
        }
    }

    #[test]
    fn gt_predicate() {
        let pred = ScanPredicate::gt(0, 50.0);
        // Block [10, 40] → max=40 ≤ 50 → skip.
        assert!(pred.can_skip_block(&stats(10.0, 40.0)));
        // Block [10, 60] → max=60 > 50 → scan.
        assert!(!pred.can_skip_block(&stats(10.0, 60.0)));
        // Block [10, 50] → max=50 ≤ 50 → skip (strict >).
        assert!(pred.can_skip_block(&stats(10.0, 50.0)));
    }

    #[test]
    fn gte_predicate() {
        let pred = ScanPredicate::gte(0, 50.0);
        // Block [10, 49] → max=49 < 50 → skip.
        assert!(pred.can_skip_block(&stats(10.0, 49.0)));
        // Block [10, 50] → max=50 ≥ 50 → scan.
        assert!(!pred.can_skip_block(&stats(10.0, 50.0)));
    }

    #[test]
    fn lt_predicate() {
        let pred = ScanPredicate::lt(0, 50.0);
        // Block [60, 100] → min=60 ≥ 50 → skip.
        assert!(pred.can_skip_block(&stats(60.0, 100.0)));
        // Block [40, 100] → min=40 < 50 → scan.
        assert!(!pred.can_skip_block(&stats(40.0, 100.0)));
    }

    #[test]
    fn eq_predicate() {
        let pred = ScanPredicate::eq(0, 50.0);
        // Block [10, 40] → 50 > max → skip.
        assert!(pred.can_skip_block(&stats(10.0, 40.0)));
        // Block [60, 100] → 50 < min → skip.
        assert!(pred.can_skip_block(&stats(60.0, 100.0)));
        // Block [40, 60] → 50 in range → scan.
        assert!(!pred.can_skip_block(&stats(40.0, 60.0)));
    }

    #[test]
    fn ne_predicate() {
        let pred = ScanPredicate::not_eq(0, 50.0);
        // Block [50, 50] → entire block is 50 → skip.
        assert!(pred.can_skip_block(&stats(50.0, 50.0)));
        // Block [40, 60] → not all 50 → scan.
        assert!(!pred.can_skip_block(&stats(40.0, 60.0)));
    }

    #[test]
    fn non_numeric_never_skipped() {
        let pred = ScanPredicate::gt(0, 50.0);
        let nan_stats = BlockStats::non_numeric(0, 1024);
        assert!(!pred.can_skip_block(&nan_stats));
    }
}
