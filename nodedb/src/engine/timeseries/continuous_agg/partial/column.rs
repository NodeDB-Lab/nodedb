// SPDX-License-Identifier: BUSL-1.1

//! Partial state of one source column inside one aggregate bucket.
//!
//! The rule matches the ad-hoc aggregate path:
//!
//! - SUM / AVG total in an [`ExactSum`]: an integer cell adds exactly, a
//!   float cell adds Kahan-compensated.
//! - MIN / MAX keep the original cell, compared exactly by
//!   [`value_replaces`].
//! - FIRST / LAST keep the original cell at the lowest / highest timestamp.
//! - The sketches (HLL, t-digest, top-K) are approximate by definition.
//!
//! Every part merges without loss, so a bucket built from several refreshes,
//! out-of-order flushes, or a lower-tier rollup equals one pass over the rows.

use nodedb_query::ExactSum;
use nodedb_query::window::extremum::value_replaces;
use nodedb_types::Value;
use nodedb_types::approx::{HyperLogLog, SpaceSaving, TDigest};

use super::super::definition::AggFunction;
use crate::util::fnv1a_hash;

/// A cell at its timestamp, for FIRST / LAST.
#[derive(Debug, Clone, PartialEq)]
pub struct TimedCell {
    pub ts: i64,
    pub value: Value,
}

/// Partial aggregate state of one source column.
#[derive(Debug, Default)]
pub struct ColumnPartial {
    /// Exact SUM / AVG state. Its count is the number of cells added.
    pub sum: ExactSum,
    /// Smallest cell, as added.
    pub min: Option<Value>,
    /// Largest cell, as added.
    pub max: Option<Value>,
    /// Cell at the lowest timestamp. An equal timestamp keeps the earlier.
    pub first: Option<TimedCell>,
    /// Cell at the highest timestamp. An equal timestamp keeps the earlier.
    pub last: Option<TimedCell>,
    pub hll: Option<HyperLogLog>,
    pub tdigest: Option<TDigest>,
    pub topk: Option<SpaceSaving>,
}

impl ColumnPartial {
    /// An empty column state feeding the sketches `functions` name.
    pub fn with_sketches(functions: &[AggFunction]) -> Self {
        let mut column = Self::default();
        for function in functions {
            match function {
                AggFunction::CountDistinct if column.hll.is_none() => {
                    column.hll = Some(HyperLogLog::new());
                }
                AggFunction::Percentile(_) if column.tdigest.is_none() => {
                    column.tdigest = Some(TDigest::new());
                }
                AggFunction::TopK(k) if column.topk.is_none() => {
                    column.topk = Some(SpaceSaving::new(*k));
                }
                _ => {}
            }
        }
        column
    }

    /// Add one integer cell at timestamp `ts`, kept exact.
    pub fn add_int(&mut self, ts: i64, v: i64) {
        self.sum.add_i64(v);
        // Distinct integers hash apart even above 2^53, where their `f64`
        // readings collide.
        self.add_cell(
            ts,
            Value::Integer(v),
            fnv1a_hash(&v.to_le_bytes()),
            v as f64,
        );
    }

    /// Add one float cell at timestamp `ts`.
    pub fn add_float(&mut self, ts: i64, v: f64) {
        self.sum.add_f64(v);
        self.add_cell(
            ts,
            Value::Float(v),
            fnv1a_hash(&v.to_bits().to_le_bytes()),
            v,
        );
    }

    /// Feed the sketches and the extremes with a cell already added to the
    /// sum. `hash` is its sketch identity, `reading` its `f64` reading.
    fn add_cell(&mut self, ts: i64, cell: Value, hash: u64, reading: f64) {
        if let Some(hll) = &mut self.hll {
            hll.add(hash);
        }
        if let Some(td) = &mut self.tdigest {
            td.add(reading);
        }
        if let Some(ss) = &mut self.topk {
            ss.add(hash);
        }
        if value_replaces(&cell, self.min.as_ref(), false) {
            self.min = Some(cell.clone());
        }
        if value_replaces(&cell, self.max.as_ref(), true) {
            self.max = Some(cell.clone());
        }
        if self.first.as_ref().is_none_or(|f| ts < f.ts) {
            self.first = Some(TimedCell {
                ts,
                value: cell.clone(),
            });
        }
        if self.last.as_ref().is_none_or(|l| ts > l.ts) {
            self.last = Some(TimedCell { ts, value: cell });
        }
    }

    /// Fold `other` into this state without loss.
    pub fn merge(&mut self, other: &ColumnPartial) {
        self.sum.merge(&other.sum);
        if let Some(min) = &other.min
            && value_replaces(min, self.min.as_ref(), false)
        {
            self.min = Some(min.clone());
        }
        if let Some(max) = &other.max
            && value_replaces(max, self.max.as_ref(), true)
        {
            self.max = Some(max.clone());
        }
        if let Some(first) = &other.first
            && self.first.as_ref().is_none_or(|f| first.ts < f.ts)
        {
            self.first = Some(first.clone());
        }
        if let Some(last) = &other.last
            && self.last.as_ref().is_none_or(|l| last.ts > l.ts)
        {
            self.last = Some(last.clone());
        }
        if let (Some(mine), Some(theirs)) = (&mut self.hll, &other.hll) {
            mine.merge(theirs);
        }
        if let (Some(mine), Some(theirs)) = (&mut self.tdigest, &other.tdigest) {
            mine.merge(theirs);
        }
        if let (Some(mine), Some(theirs)) = (&mut self.topk, &other.topk) {
            mine.merge(theirs);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ABOVE: i64 = 9_007_199_254_740_993;
    const AT: i64 = 9_007_199_254_740_992;

    #[test]
    fn integers_past_two_pow_53_stay_exact() {
        let mut c = ColumnPartial::default();
        c.add_int(2, ABOVE);
        c.add_int(1, AT);
        assert_eq!(c.sum.sum().unwrap(), Value::Integer(ABOVE + AT));
        assert_eq!(c.min, Some(Value::Integer(AT)));
        assert_eq!(c.max, Some(Value::Integer(ABOVE)));
        assert_eq!(
            c.first.as_ref().map(|f| &f.value),
            Some(&Value::Integer(AT))
        );
        assert_eq!(
            c.last.as_ref().map(|l| &l.value),
            Some(&Value::Integer(ABOVE))
        );
    }

    #[test]
    fn merge_equals_single_pass() {
        let cells = [ABOVE, AT, i64::MAX, 1_700_000_000_000_000_001];
        let mut single = ColumnPartial::default();
        for (ts, v) in cells.iter().enumerate() {
            single.add_int(ts as i64, *v);
        }
        let mut a = ColumnPartial::default();
        let mut b = ColumnPartial::default();
        for (ts, v) in cells.iter().enumerate() {
            let part = if ts % 2 == 0 { &mut a } else { &mut b };
            part.add_int(ts as i64, *v);
        }
        b.merge(&a);
        assert_eq!(b.sum, single.sum);
        assert_eq!(b.min, single.min);
        assert_eq!(b.max, single.max);
        assert_eq!(b.first, single.first);
        assert_eq!(b.last, single.last);
    }

    #[test]
    fn distinct_integers_above_two_pow_53_count_apart() {
        let mut c = ColumnPartial::with_sketches(&[AggFunction::CountDistinct]);
        c.add_int(0, ABOVE);
        c.add_int(1, AT);
        let estimate = c.hll.as_ref().map_or(0.0, HyperLogLog::estimate);
        assert!(estimate > 1.5, "two distinct values, estimate {estimate}");
    }
}
