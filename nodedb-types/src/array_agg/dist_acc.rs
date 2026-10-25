// SPDX-License-Identifier: Apache-2.0

//! The running form of a [`ValueDist`]: a count per distinct value.
//!
//! A core folds cells into a hash map, one insert per cell, and sorts the
//! distinct values once when it hands the partial on. Memory follows the
//! distinct values, not the cells, so a `UINT16` band of any size holds at
//! most 65 536 entries.

use std::collections::HashMap;

use super::dist::{DistValue, FloatRun, IntRun, MAX_DIST_ENTRIES, ValueDist, cmp_float};

/// A running value distribution.
#[derive(Debug, Clone, Default, PartialEq)]
pub enum DistAcc {
    #[default]
    Empty,
    Ints(HashMap<i64, u64>),
    /// Keyed by the canonical bits of each float: `-0.0` folds into `0.0`
    /// and every NaN into one NaN.
    Floats(HashMap<u64, u64>),
    /// More than [`MAX_DIST_ENTRIES`] distinct values. The counts are gone.
    OverBudget {
        distinct: u64,
    },
}

fn float_key(v: f64) -> u64 {
    if v.is_nan() {
        f64::NAN.to_bits()
    } else if v == 0.0 {
        0.0f64.to_bits()
    } else {
        v.to_bits()
    }
}

/// Integer counts re-keyed as floats. Two integers past 2^53 that round to
/// one float fold together.
fn ints_as_float_counts(map: HashMap<i64, u64>) -> HashMap<u64, u64> {
    let mut floats: HashMap<u64, u64> = HashMap::with_capacity(map.len());
    for (v, n) in map {
        *floats.entry(float_key(v as f64)).or_insert(0) += n;
    }
    floats
}

impl DistAcc {
    /// Fold `n` cells holding `v`.
    pub fn insert(&mut self, v: DistValue, n: u64) {
        match v {
            DistValue::Int(i) => match self {
                DistAcc::OverBudget { .. } => return,
                DistAcc::Empty => *self = DistAcc::Ints(HashMap::from([(i, n)])),
                DistAcc::Ints(map) => *map.entry(i).or_insert(0) += n,
                DistAcc::Floats(map) => *map.entry(float_key(i as f64)).or_insert(0) += n,
            },
            DistValue::Float(f) => {
                let key = float_key(f);
                match self {
                    DistAcc::OverBudget { .. } => return,
                    DistAcc::Empty => *self = DistAcc::Floats(HashMap::from([(key, n)])),
                    DistAcc::Floats(map) => *map.entry(key).or_insert(0) += n,
                    DistAcc::Ints(map) => {
                        let mut floats = ints_as_float_counts(std::mem::take(map));
                        *floats.entry(key).or_insert(0) += n;
                        *self = DistAcc::Floats(floats);
                    }
                }
            }
        }
        self.check_budget();
    }

    /// Fold every value of `other` into this distribution.
    pub fn merge(&mut self, other: DistAcc) {
        match other {
            DistAcc::Empty => {}
            DistAcc::OverBudget { distinct } => {
                *self = DistAcc::OverBudget {
                    distinct: distinct.max(self.distinct()),
                }
            }
            DistAcc::Ints(map) => {
                for (v, n) in map {
                    self.insert(DistValue::Int(v), n);
                }
            }
            DistAcc::Floats(map) => {
                for (bits, n) in map {
                    self.insert(DistValue::Float(f64::from_bits(bits)), n);
                }
            }
        }
    }

    /// The number of distinct values.
    pub fn distinct(&self) -> u64 {
        match self {
            DistAcc::Empty => 0,
            DistAcc::Ints(map) => map.len() as u64,
            DistAcc::Floats(map) => map.len() as u64,
            DistAcc::OverBudget { distinct } => *distinct,
        }
    }

    /// The running form of a received distribution.
    pub fn from_dist(dist: ValueDist) -> Self {
        match dist {
            ValueDist::Empty => DistAcc::Empty,
            ValueDist::OverBudget { distinct } => DistAcc::OverBudget { distinct },
            ValueDist::Ints(runs) => {
                DistAcc::Ints(runs.into_iter().map(|r| (r.value, r.count)).collect())
            }
            ValueDist::Floats(runs) => DistAcc::Floats(
                runs.into_iter()
                    .map(|r| (float_key(r.value), r.count))
                    .collect(),
            ),
        }
    }

    /// The sorted wire form.
    pub fn into_dist(self) -> ValueDist {
        match self {
            DistAcc::Empty => ValueDist::Empty,
            DistAcc::OverBudget { distinct } => ValueDist::OverBudget { distinct },
            DistAcc::Ints(map) => {
                let mut runs: Vec<IntRun> = map
                    .into_iter()
                    .map(|(value, count)| IntRun { value, count })
                    .collect();
                runs.sort_unstable_by_key(|r| r.value);
                ValueDist::Ints(runs)
            }
            DistAcc::Floats(map) => {
                let mut runs: Vec<FloatRun> = map
                    .into_iter()
                    .map(|(bits, count)| FloatRun {
                        value: f64::from_bits(bits),
                        count,
                    })
                    .collect();
                runs.sort_unstable_by(|a, b| cmp_float(a.value, b.value));
                ValueDist::Floats(runs)
            }
        }
    }

    fn check_budget(&mut self) {
        let distinct = self.distinct();
        if distinct > MAX_DIST_ENTRIES as u64 && !matches!(self, DistAcc::OverBudget { .. }) {
            *self = DistAcc::OverBudget { distinct };
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn folds_counts_and_sorts_once() {
        let mut acc = DistAcc::default();
        for v in [5, -2, 5, 9, -2, 5] {
            acc.insert(DistValue::Int(v), 1);
        }
        assert_eq!(
            acc.into_dist(),
            ValueDist::Ints(vec![
                IntRun {
                    value: -2,
                    count: 2
                },
                IntRun { value: 5, count: 3 },
                IntRun { value: 9, count: 1 },
            ])
        );
    }

    #[test]
    fn merged_accumulators_equal_one_pass() {
        let values = [3.5, -1.0, 3.5, f64::NAN, 0.0, -0.0, 7.25];
        let mut one = DistAcc::default();
        for v in values {
            one.insert(DistValue::Float(v), 1);
        }
        let (left, right) = values.split_at(3);
        let mut a = DistAcc::default();
        for v in left {
            a.insert(DistValue::Float(*v), 1);
        }
        let mut b = DistAcc::default();
        for v in right {
            b.insert(DistValue::Float(*v), 1);
        }
        a.merge(b);
        let (x, y) = (a.into_dist(), one.into_dist());
        assert_eq!(x.count(), y.count());
        assert_eq!(x.distinct(), y.distinct());
        assert_eq!(x.percentile(0.5), y.percentile(0.5));
        // -0.0 and 0.0 count as one value.
        assert_eq!(y.distinct(), 5);
    }

    #[test]
    fn an_int_then_a_float_widens() {
        let mut acc = DistAcc::default();
        acc.insert(DistValue::Int(2), 1);
        acc.insert(DistValue::Float(2.5), 1);
        acc.insert(DistValue::Int(2), 1);
        assert_eq!(
            acc.into_dist(),
            ValueDist::Floats(vec![
                FloatRun {
                    value: 2.0,
                    count: 2
                },
                FloatRun {
                    value: 2.5,
                    count: 1
                },
            ])
        );
    }

    #[test]
    fn the_wire_form_round_trips() {
        let mut acc = DistAcc::default();
        for v in [2.5, -1.0, 2.5] {
            acc.insert(DistValue::Float(v), 1);
        }
        let dist = acc.clone().into_dist();
        assert_eq!(DistAcc::from_dist(dist.clone()).into_dist(), dist);
        assert_eq!(DistAcc::from_dist(dist), acc);
    }

    #[test]
    fn over_budget_absorbs_merges() {
        let mut acc = DistAcc::OverBudget { distinct: 10 };
        acc.insert(DistValue::Int(1), 1);
        acc.merge(DistAcc::Ints(HashMap::from([(2, 1)])));
        assert_eq!(acc.into_dist(), ValueDist::OverBudget { distinct: 10 });
    }
}
