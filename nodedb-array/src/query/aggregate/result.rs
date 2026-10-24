// SPDX-License-Identifier: Apache-2.0

//! Partial results of the array reducers, their exact merge, and their
//! final value.

use nodedb_types::array_agg::{
    DistAcc, DistBudgetExceeded, DistValue, HistogramBin, HistogramSpec, Moments,
};

use super::reducer::Reducer;
use crate::error::{ArrayError, ArrayResult};
use crate::query::agg_number::{AggNumber, AggScalar};

/// Partial result for one reducer over one (group of) cell(s).
#[derive(Debug, Clone, PartialEq)]
pub enum AggregateResult {
    Sum {
        value: AggNumber,
        count: u64,
    },
    Count {
        count: u64,
    },
    Min {
        value: AggNumber,
        count: u64,
    },
    Max {
        value: AggNumber,
        count: u64,
    },
    /// `(sum, count)`, so partials merge exactly.
    Mean {
        sum: AggNumber,
        count: u64,
    },
    /// `Median`, `Percentile` and `Mode`: every distinct value with its count.
    Dist {
        reducer: Reducer,
        acc: DistAcc,
    },
    /// The variance and standard-deviation reducers.
    Moments {
        reducer: Reducer,
        moments: Moments,
    },
    /// Histogram bin counts, one per bin of `spec`.
    Histogram {
        spec: HistogramSpec,
        counts: Vec<u64>,
    },
    /// No cell contributed yet.
    Empty(Reducer),
}

/// The final value of a reducer.
#[derive(Debug, Clone, PartialEq)]
pub enum AggOutput {
    /// One value. `None` when no cell contributed.
    Scalar(Option<AggScalar>),
    /// One entry per histogram bin, zero-count bins included.
    Bins(Vec<HistogramBin>),
}

impl AggregateResult {
    /// The partial no cell has contributed to.
    pub fn empty(reducer: Reducer) -> Self {
        AggregateResult::Empty(reducer)
    }

    /// Merge two partials of the same reducer. The executor merges only
    /// same-reducer partials. A mismatched pair keeps the left side.
    pub fn merge(self, other: AggregateResult) -> AggregateResult {
        use AggregateResult::*;
        match (self, other) {
            (Empty(_), x) | (x, Empty(_)) => x,
            (
                Sum {
                    value: a,
                    count: ca,
                },
                Sum {
                    value: b,
                    count: cb,
                },
            ) => Sum {
                value: a.sum_with(b),
                count: ca + cb,
            },
            (Count { count: a }, Count { count: b }) => Count { count: a + b },
            (
                Min {
                    value: a,
                    count: ca,
                },
                Min {
                    value: b,
                    count: cb,
                },
            ) => Min {
                value: a.min_with(b),
                count: ca + cb,
            },
            (
                Max {
                    value: a,
                    count: ca,
                },
                Max {
                    value: b,
                    count: cb,
                },
            ) => Max {
                value: a.max_with(b),
                count: ca + cb,
            },
            (Mean { sum: a, count: ca }, Mean { sum: b, count: cb }) => Mean {
                sum: a.sum_with(b),
                count: ca + cb,
            },
            (Dist { reducer, mut acc }, Dist { acc: other, .. }) => {
                acc.merge(other);
                Dist { reducer, acc }
            }
            (Moments { reducer, moments }, Moments { moments: other, .. }) => Moments {
                reducer,
                moments: moments.merge(other),
            },
            (Histogram { spec, mut counts }, Histogram { counts: other, .. }) => {
                spec.merge_counts(&mut counts, &other);
                Histogram { spec, counts }
            }
            (lhs, _) => lhs,
        }
    }

    /// Merge `other` into this partial in place.
    pub fn absorb(&mut self, other: AggregateResult) {
        let current = std::mem::replace(self, AggregateResult::Empty(Reducer::Count));
        *self = current.merge(other);
    }

    /// The final value.
    ///
    /// - `SUM` of an integer attr is an exact integer and refuses past `i64`
    ///   (SQLSTATE `22003`). `MIN`, `MAX` and `MODE` keep the attr's kind.
    ///   `COUNT` is an integer.
    /// - `MEAN`, `MEDIAN`, `PERCENTILE`, the variances and the standard
    ///   deviations are floats.
    /// - A median, percentile or mode past the distinct-value budget refuses
    ///   (SQLSTATE `54000`).
    /// - A histogram answers every bin, a zero count included.
    pub fn finalize(self) -> ArrayResult<AggOutput> {
        let scalar = |v: Option<AggScalar>| -> ArrayResult<AggOutput> { Ok(AggOutput::Scalar(v)) };
        match self {
            AggregateResult::Sum { value, .. }
            | AggregateResult::Min { value, .. }
            | AggregateResult::Max { value, .. } => scalar(Some(value.finish()?)),
            AggregateResult::Count { count } => scalar(Some(AggScalar::Int(
                i64::try_from(count).map_err(|_| ArrayError::NumericOverflow {
                    detail: format!("count {count} is out of range for bigint"),
                })?,
            ))),
            AggregateResult::Mean { sum, count } if count > 0 => {
                scalar(Some(AggScalar::Float(sum.to_f64() / count as f64)))
            }
            AggregateResult::Mean { .. } => scalar(None),
            AggregateResult::Dist { reducer, acc } => scalar(finish_dist(reducer, acc)?),
            AggregateResult::Moments { reducer, moments } => {
                scalar(finish_moments(reducer, &moments).map(AggScalar::Float))
            }
            AggregateResult::Histogram { spec, counts } => {
                Ok(AggOutput::Bins(spec.finish(&counts)))
            }
            AggregateResult::Empty(Reducer::Histogram(spec)) => {
                Ok(AggOutput::Bins(spec.finish(&[])))
            }
            AggregateResult::Empty(_) => scalar(None),
        }
    }
}

/// The final value of a median, percentile or mode.
fn finish_dist(reducer: Reducer, acc: DistAcc) -> ArrayResult<Option<AggScalar>> {
    let dist = acc.into_dist();
    let refused = |e: DistBudgetExceeded| ArrayError::AggregateBudgetExceeded {
        detail: e.to_string(),
    };
    Ok(match reducer {
        Reducer::Median => dist.percentile(0.5).map_err(refused)?.map(AggScalar::Float),
        Reducer::Percentile(p) => dist.percentile(p).map_err(refused)?.map(AggScalar::Float),
        Reducer::Mode => dist.mode().map_err(refused)?.map(|v| match v {
            DistValue::Int(i) => AggScalar::Int(i),
            DistValue::Float(f) => AggScalar::Float(f),
        }),
        Reducer::Sum
        | Reducer::Count
        | Reducer::Min
        | Reducer::Max
        | Reducer::Mean
        | Reducer::StdDevPop
        | Reducer::StdDevSamp
        | Reducer::VarPop
        | Reducer::VarSamp
        | Reducer::Histogram(_) => None,
    })
}

/// The final value of a variance or standard deviation.
pub fn finish_moments(reducer: Reducer, moments: &Moments) -> Option<f64> {
    match reducer {
        Reducer::VarPop => moments.var_pop(),
        Reducer::VarSamp => moments.var_samp(),
        Reducer::StdDevPop => moments.var_pop().map(f64::sqrt),
        Reducer::StdDevSamp => moments.var_samp().map(f64::sqrt),
        Reducer::Sum
        | Reducer::Count
        | Reducer::Min
        | Reducer::Max
        | Reducer::Mean
        | Reducer::Median
        | Reducer::Percentile(_)
        | Reducer::Mode
        | Reducer::Histogram(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fin(r: AggregateResult) -> Option<AggScalar> {
        match r.finalize().expect("finalize") {
            AggOutput::Scalar(v) => v,
            AggOutput::Bins(b) => panic!("scalar reducer answered bins: {b:?}"),
        }
    }

    #[test]
    fn mean_merges_exactly() {
        let a = AggregateResult::Mean {
            sum: AggNumber::Float(10.0),
            count: 4,
        };
        let b = AggregateResult::Mean {
            sum: AggNumber::Float(6.0),
            count: 2,
        };
        assert_eq!(fin(a.merge(b)), Some(AggScalar::Float(16.0 / 6.0)));
    }

    #[test]
    fn empty_scalar_reducers_finalize_to_none() {
        for r in [
            Reducer::Sum,
            Reducer::Count,
            Reducer::Median,
            Reducer::Percentile(0.9),
            Reducer::Mode,
            Reducer::VarSamp,
            Reducer::StdDevPop,
        ] {
            assert_eq!(fin(AggregateResult::Empty(r)), None, "{r:?}");
        }
    }

    #[test]
    fn an_empty_histogram_answers_every_bin_at_zero() {
        let spec = HistogramSpec::new(3, 0.0, 3.0).expect("valid");
        let AggOutput::Bins(bins) = AggregateResult::Empty(Reducer::Histogram(spec))
            .finalize()
            .expect("finalize")
        else {
            panic!("histogram answers bins");
        };
        assert_eq!(bins.len(), 3);
        assert!(bins.iter().all(|b| b.count == 0));
    }

    #[test]
    fn an_over_budget_distribution_is_refused() {
        let r = AggregateResult::Dist {
            reducer: Reducer::Median,
            acc: DistAcc::OverBudget { distinct: 7 },
        };
        assert!(matches!(
            r.finalize(),
            Err(ArrayError::AggregateBudgetExceeded { .. })
        ));
    }

    #[test]
    fn absorb_equals_merge() {
        let mut a = AggregateResult::Count { count: 2 };
        a.absorb(AggregateResult::Count { count: 3 });
        assert_eq!(a, AggregateResult::Count { count: 5 });
    }
}
