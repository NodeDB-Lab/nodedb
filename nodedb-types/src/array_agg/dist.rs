// SPDX-License-Identifier: Apache-2.0

//! An exact value distribution: every distinct value with its count.
//!
//! Values are kept in ascending order. Floats order with NaN above every
//! other value, as PostgreSQL sorts them, and `-0.0` equals `0.0`. Two
//! distributions merge exactly by summing the counts of equal values.
//!
//! A distribution past [`MAX_DIST_ENTRIES`] distinct values drops its
//! entries and becomes [`ValueDist::OverBudget`]. Finalizing one is a
//! typed error, never an approximation.

use std::cmp::Ordering;
use std::fmt;

use serde::{Deserialize, Serialize};

/// The most distinct values one distribution holds. At 16 bytes a run,
/// this bounds a partial near 64 MiB.
pub const MAX_DIST_ENTRIES: usize = 1 << 22;

/// One distinct integer value and how many cells hold it.
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
pub struct IntRun {
    pub value: i64,
    pub count: u64,
}

/// One distinct float value and how many cells hold it.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct FloatRun {
    pub value: f64,
    pub count: u64,
}

/// One value of a distribution, typed as it was stored.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum DistValue {
    Int(i64),
    Float(f64),
}

/// A distribution's distinct values passed [`MAX_DIST_ENTRIES`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DistBudgetExceeded {
    /// Distinct values seen when the budget ran out. A lower bound.
    pub distinct: u64,
}

impl fmt::Display for DistBudgetExceeded {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "an exact median, percentile or mode holds at most {MAX_DIST_ENTRIES} distinct \
             values, and the input holds at least {}; narrow the slice or zone, or group by \
             a dim",
            self.distinct
        )
    }
}

/// An exact value distribution.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum ValueDist {
    /// No value.
    Empty,
    /// Distinct integers, strictly ascending.
    Ints(Vec<IntRun>),
    /// Distinct floats, strictly ascending, NaN last.
    Floats(Vec<FloatRun>),
    /// More than [`MAX_DIST_ENTRIES`] distinct values. The entries are gone.
    OverBudget { distinct: u64 },
}

/// The float total order a distribution keeps: NaN above every other
/// value, `-0.0` equal to `0.0`.
pub(super) fn cmp_float(a: f64, b: f64) -> Ordering {
    match (a.is_nan(), b.is_nan()) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Greater,
        (false, true) => Ordering::Less,
        (false, false) => a.partial_cmp(&b).unwrap_or(Ordering::Equal),
    }
}

impl ValueDist {
    /// The number of values: the sum of every run's count.
    pub fn count(&self) -> u64 {
        match self {
            ValueDist::Empty | ValueDist::OverBudget { .. } => 0,
            ValueDist::Ints(runs) => runs.iter().map(|r| r.count).sum(),
            ValueDist::Floats(runs) => runs.iter().map(|r| r.count).sum(),
        }
    }

    /// The number of distinct values.
    pub fn distinct(&self) -> u64 {
        match self {
            ValueDist::Empty => 0,
            ValueDist::Ints(runs) => runs.len() as u64,
            ValueDist::Floats(runs) => runs.len() as u64,
            ValueDist::OverBudget { distinct } => *distinct,
        }
    }

    /// The exact merge of two distributions.
    ///
    /// An integer and a float distribution merge as floats. An integer past
    /// 2^53 then rounds, which happens only when one attr mixes kinds.
    pub fn merge(self, other: ValueDist) -> ValueDist {
        match (self, other) {
            (ValueDist::Empty, x) | (x, ValueDist::Empty) => x,
            (ValueDist::OverBudget { distinct: a }, x)
            | (x, ValueDist::OverBudget { distinct: a }) => ValueDist::OverBudget {
                distinct: a.max(x.distinct()),
            },
            (ValueDist::Ints(a), ValueDist::Ints(b)) => budgeted(
                merge_runs(a, b, |x, y| x.value.cmp(&y.value), add_int),
                ValueDist::Ints,
            ),
            (ValueDist::Floats(a), ValueDist::Floats(b)) => budgeted(
                merge_runs(a, b, |x, y| cmp_float(x.value, y.value), add_float),
                ValueDist::Floats,
            ),
            (ValueDist::Ints(a), ValueDist::Floats(b))
            | (ValueDist::Floats(b), ValueDist::Ints(a)) => {
                ValueDist::Floats(ints_as_floats(a)).merge(ValueDist::Floats(b))
            }
        }
    }

    /// The exact `p` percentile, `0 <= p <= 1`, by linear interpolation
    /// between the closest ranks.
    ///
    /// With `N` values sorted ascending and numbered from 0, the result is
    /// at rank `h = p * (N - 1)`: `v[floor(h)] + (h - floor(h)) *
    /// (v[ceil(h)] - v[floor(h)])`. This is PostgreSQL's
    /// `percentile_cont`. `None` when the distribution holds no value.
    pub fn percentile(&self, p: f64) -> Result<Option<f64>, DistBudgetExceeded> {
        let total = match self {
            ValueDist::OverBudget { distinct } => {
                return Err(DistBudgetExceeded {
                    distinct: *distinct,
                });
            }
            other => other.count(),
        };
        if total == 0 {
            return Ok(None);
        }
        let h = p.clamp(0.0, 1.0) * (total - 1) as f64;
        let lo_rank = h.floor() as u64;
        let hi_rank = h.ceil() as u64;
        let frac = h - h.floor();
        let (Some(lo), Some(hi)) = (self.value_at(lo_rank), self.value_at(hi_rank)) else {
            return Ok(None);
        };
        Ok(Some(interpolate(lo, hi, frac)))
    }

    /// The value held by the most cells. Among equal counts, the smallest
    /// value. `None` when the distribution holds no value.
    pub fn mode(&self) -> Result<Option<DistValue>, DistBudgetExceeded> {
        Ok(match self {
            ValueDist::Empty => None,
            ValueDist::OverBudget { distinct } => {
                return Err(DistBudgetExceeded {
                    distinct: *distinct,
                });
            }
            // Runs ascend, so the first run of the highest count is the
            // smallest value among the ties.
            ValueDist::Ints(runs) => runs
                .iter()
                .fold(None::<IntRun>, |best, r| match best {
                    Some(b) if b.count >= r.count => Some(b),
                    _ => Some(*r),
                })
                .map(|r| DistValue::Int(r.value)),
            ValueDist::Floats(runs) => runs
                .iter()
                .fold(None::<FloatRun>, |best, r| match best {
                    Some(b) if b.count >= r.count => Some(b),
                    _ => Some(*r),
                })
                .map(|r| DistValue::Float(r.value)),
        })
    }

    /// The value at 0-based `rank` of the ascending values.
    fn value_at(&self, rank: u64) -> Option<DistValue> {
        let mut seen = 0u64;
        match self {
            ValueDist::Empty | ValueDist::OverBudget { .. } => None,
            ValueDist::Ints(runs) => runs.iter().find_map(|r| {
                seen += r.count;
                (rank < seen).then_some(DistValue::Int(r.value))
            }),
            ValueDist::Floats(runs) => runs.iter().find_map(|r| {
                seen += r.count;
                (rank < seen).then_some(DistValue::Float(r.value))
            }),
        }
    }
}

/// `lo + frac * (hi - lo)`. Two integers subtract exactly in `i128`.
fn interpolate(lo: DistValue, hi: DistValue, frac: f64) -> f64 {
    match (lo, hi) {
        (DistValue::Int(a), DistValue::Int(b)) => {
            if frac == 0.0 {
                return a as f64;
            }
            a as f64 + frac * ((i128::from(b) - i128::from(a)) as f64)
        }
        (a, b) => {
            let (a, b) = (as_f64(a), as_f64(b));
            if frac == 0.0 { a } else { a + frac * (b - a) }
        }
    }
}

fn as_f64(v: DistValue) -> f64 {
    match v {
        DistValue::Int(i) => i as f64,
        DistValue::Float(f) => f,
    }
}

fn add_int(a: IntRun, b: IntRun) -> IntRun {
    IntRun {
        value: a.value,
        count: a.count.saturating_add(b.count),
    }
}

fn add_float(a: FloatRun, b: FloatRun) -> FloatRun {
    FloatRun {
        value: a.value,
        count: a.count.saturating_add(b.count),
    }
}

fn ints_as_floats(runs: Vec<IntRun>) -> Vec<FloatRun> {
    let mut out: Vec<FloatRun> = Vec::with_capacity(runs.len());
    for r in runs {
        let value = r.value as f64;
        // Two integers past 2^53 can round to one float: fold them.
        match out.last_mut() {
            Some(last) if last.value == value => last.count = last.count.saturating_add(r.count),
            _ => out.push(FloatRun {
                value,
                count: r.count,
            }),
        }
    }
    out
}

/// Merge two ascending run lists, summing the counts of equal values.
fn merge_runs<T: Copy>(
    a: Vec<T>,
    b: Vec<T>,
    cmp: impl Fn(&T, &T) -> Ordering,
    add: impl Fn(T, T) -> T,
) -> Vec<T> {
    let mut out = Vec::with_capacity(a.len() + b.len());
    let (mut i, mut j) = (0, 0);
    while i < a.len() && j < b.len() {
        match cmp(&a[i], &b[j]) {
            Ordering::Less => {
                out.push(a[i]);
                i += 1;
            }
            Ordering::Greater => {
                out.push(b[j]);
                j += 1;
            }
            Ordering::Equal => {
                out.push(add(a[i], b[j]));
                i += 1;
                j += 1;
            }
        }
    }
    out.extend_from_slice(&a[i..]);
    out.extend_from_slice(&b[j..]);
    out
}

/// `runs` wrapped by `wrap`, or [`ValueDist::OverBudget`] past the budget.
fn budgeted<T>(runs: Vec<T>, wrap: fn(Vec<T>) -> ValueDist) -> ValueDist {
    if runs.len() > MAX_DIST_ENTRIES {
        ValueDist::OverBudget {
            distinct: runs.len() as u64,
        }
    } else if runs.is_empty() {
        ValueDist::Empty
    } else {
        wrap(runs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ints(pairs: &[(i64, u64)]) -> ValueDist {
        ValueDist::Ints(
            pairs
                .iter()
                .map(|&(value, count)| IntRun { value, count })
                .collect(),
        )
    }

    fn floats(pairs: &[(f64, u64)]) -> ValueDist {
        ValueDist::Floats(
            pairs
                .iter()
                .map(|&(value, count)| FloatRun { value, count })
                .collect(),
        )
    }

    #[test]
    fn merge_sums_equal_values_and_keeps_order() {
        let a = ints(&[(1, 2), (5, 1)]);
        let b = ints(&[(0, 1), (5, 3), (9, 1)]);
        assert_eq!(a.merge(b), ints(&[(0, 1), (1, 2), (5, 4), (9, 1)]));
        assert_eq!(ValueDist::Empty.merge(ints(&[(3, 1)])), ints(&[(3, 1)]));
    }

    #[test]
    fn percentile_interpolates_between_closest_ranks() {
        // Values 1, 2, 3, 4.
        let d = ints(&[(1, 1), (2, 1), (3, 1), (4, 1)]);
        assert_eq!(d.percentile(0.5), Ok(Some(2.5)));
        assert_eq!(d.percentile(0.0), Ok(Some(1.0)));
        assert_eq!(d.percentile(1.0), Ok(Some(4.0)));
        assert_eq!(d.percentile(0.25), Ok(Some(1.75)));
        // Values 7, 7, 7, 10: rank 1.5 sits between two sevens.
        let runs = ints(&[(7, 3), (10, 1)]);
        assert_eq!(runs.percentile(0.5), Ok(Some(7.0)));
        assert_eq!(ValueDist::Empty.percentile(0.5), Ok(None));
    }

    #[test]
    fn integer_interpolation_is_exact_past_2_pow_53() {
        let big = 1i64 << 60;
        let d = ints(&[(big, 1), (big + 2, 1)]);
        assert_eq!(d.percentile(0.5), Ok(Some((big + 1) as f64)));
    }

    #[test]
    fn mode_breaks_ties_by_the_smallest_value() {
        let d = ints(&[(-3, 2), (4, 2), (9, 1)]);
        assert_eq!(d.mode(), Ok(Some(DistValue::Int(-3))));
        let f = floats(&[(0.5, 1), (2.5, 3)]);
        assert_eq!(f.mode(), Ok(Some(DistValue::Float(2.5))));
        assert_eq!(ValueDist::Empty.mode(), Ok(None));
    }

    #[test]
    fn nan_sorts_last_and_zeroes_are_equal() {
        let a = floats(&[(-0.0, 1), (f64::NAN, 1)]);
        let b = floats(&[(0.0, 2)]);
        let merged = a.merge(b);
        let ValueDist::Floats(runs) = &merged else {
            panic!("floats stay floats");
        };
        assert_eq!(runs.len(), 2);
        assert_eq!(runs[0].count, 3);
        assert!(runs[1].value.is_nan());
        assert!(merged.percentile(1.0).unwrap().unwrap().is_nan());
    }

    #[test]
    fn over_budget_is_a_typed_error() {
        let over = ValueDist::OverBudget { distinct: 9 };
        let merged = over.merge(ints(&[(1, 1)]));
        assert!(matches!(merged, ValueDist::OverBudget { distinct: 9 }));
        assert_eq!(
            merged.percentile(0.5),
            Err(DistBudgetExceeded { distinct: 9 })
        );
        assert_eq!(merged.mode(), Err(DistBudgetExceeded { distinct: 9 }));
    }

    #[test]
    fn mixed_kinds_merge_as_floats() {
        let merged = ints(&[(1, 1), (2, 1)]).merge(floats(&[(1.0, 1), (1.5, 1)]));
        assert_eq!(merged, floats(&[(1.0, 2), (1.5, 1), (2.0, 1)]));
    }

    #[test]
    fn wire_round_trip() {
        for d in [
            ValueDist::Empty,
            ints(&[(-1, 4), (8, 1)]),
            floats(&[(0.25, 2)]),
            ValueDist::OverBudget { distinct: 5 },
        ] {
            let bytes = zerompk::to_msgpack_vec(&d).expect("encode");
            let back: ValueDist = zerompk::from_msgpack(&bytes).expect("decode");
            assert_eq!(back, d);
        }
    }
}
