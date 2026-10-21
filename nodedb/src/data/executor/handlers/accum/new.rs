// SPDX-License-Identifier: BUSL-1.1

//! `AggAccum::new` — construct the zero-state accumulator for a spec.

use std::collections::{HashMap, HashSet};

use super::state::AggAccum;
use nodedb_physical::physical_plan::{AggregateParam, AggregateSpec};

/// A spec whose `param` is not the one its function takes. A default would
/// answer a different question than the one asked, so this is an error.
fn wrong_param(agg: &AggregateSpec, expected: &str) -> crate::Error {
    crate::Error::BadRequest {
        detail: format!(
            "{}() needs {expected}, the spec carries {:?}",
            agg.function, agg.param
        ),
    }
}

/// The fraction of `approx_percentile` / `percentile_cont`, in `[0, 1]`.
fn percentile_param(agg: &AggregateSpec) -> crate::Result<f64> {
    match agg.param {
        Some(AggregateParam::Percentile { fraction }) if (0.0..=1.0).contains(&fraction) => {
            Ok(fraction)
        }
        _ => Err(wrong_param(agg, "a percentile fraction in [0, 1]")),
    }
}

/// The `k` of `approx_topk`, at least 1.
fn topk_param(agg: &AggregateSpec) -> crate::Result<usize> {
    match agg.param {
        Some(AggregateParam::TopK { k }) if k >= 1 => {
            usize::try_from(k).map_err(|_| wrong_param(agg, "a k this platform can hold"))
        }
        _ => Err(wrong_param(agg, "a k of at least 1")),
    }
}

impl AggAccum {
    /// The zero-state accumulator of `agg`. Every name in
    /// `GENERIC_AGGREGATE_FUNCTIONS` has one. Any other name is refused: the
    /// Control Plane refuses unknown names before it builds a spec, and this
    /// guard keeps an unknown name from ever accumulating as another function.
    pub(crate) fn new(agg: &AggregateSpec) -> crate::Result<Self> {
        Ok(match agg.function.as_str() {
            "count" => AggAccum::Count { n: 0 },
            "sum" | "avg" => AggAccum::SumAvg {
                sum: nodedb_query::ExactSum::new(),
            },
            "sum_distinct" | "avg_distinct" => AggAccum::SumAvgDistinct {
                seen: HashMap::new(),
            },
            "min" => AggAccum::Min { best: None },
            "max" => AggAccum::Max { best: None },
            "count_distinct" => AggAccum::CountDistinct {
                seen: HashSet::new(),
            },
            "stddev" | "stddev_pop" | "stddev_samp" | "variance" | "var_pop" | "var_samp"
            | "ts_stddev" => AggAccum::Welford {
                moments: nodedb_types::array_agg::Moments::default(),
            },
            "approx_count_distinct" => AggAccum::Hll {
                hll: nodedb_types::approx::HyperLogLog::new(),
            },
            "approx_percentile" => AggAccum::TDigest {
                digest: nodedb_types::approx::TDigest::new(),
                pct: percentile_param(agg)?,
            },
            "approx_topk" => {
                let k = topk_param(agg)?;
                AggAccum::TopK {
                    ss: nodedb_types::approx::SpaceSaving::new(k),
                    k,
                }
            }
            "array_agg" => AggAccum::ArrayAgg { values: Vec::new() },
            "array_agg_distinct" => AggAccum::ArrayAggDistinct {
                seen: HashSet::new(),
                values: Vec::new(),
            },
            "percentile_cont" => {
                let pct = percentile_param(agg)?;
                AggAccum::PercentileCont {
                    values: Vec::new(),
                    pct,
                }
            }
            "string_agg" | "group_concat" => AggAccum::StringAgg { parts: Vec::new() },
            unknown => {
                return Err(crate::Error::FeatureNotSupported {
                    detail: format!("aggregate function {unknown}() has no accumulator"),
                });
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_physical::physical_plan::{
        GENERIC_AGGREGATE_FUNCTIONS, PARAMETERIZED_AGGREGATE_FUNCTIONS,
    };

    fn spec(function: &str, param: Option<AggregateParam>) -> AggregateSpec {
        AggregateSpec {
            function: function.to_string(),
            alias: format!("{function}(v)"),
            user_alias: None,
            field: "v".to_string(),
            expr: None,
            param,
        }
    }

    /// Every function the planner sends to the generic aggregate has its own
    /// accumulator. Only `count` builds the count accumulator.
    #[test]
    fn every_generic_function_has_an_accumulator() {
        for &function in GENERIC_AGGREGATE_FUNCTIONS {
            let param = match function {
                "approx_topk" => Some(AggregateParam::TopK { k: 1 }),
                f if PARAMETERIZED_AGGREGATE_FUNCTIONS.contains(&f) => {
                    Some(AggregateParam::Percentile { fraction: 0.5 })
                }
                _ => None,
            };
            let accum = AggAccum::new(&spec(function, param))
                .unwrap_or_else(|e| panic!("{function} has no accumulator: {e:?}"));
            assert_eq!(
                matches!(accum, AggAccum::Count { .. }),
                function == "count",
                "{function} has no accumulator of its own"
            );
        }
    }

    /// An unknown name is refused by name, never counted.
    #[test]
    fn an_unknown_function_is_refused() {
        match AggAccum::new(&spec("median", None)) {
            Err(crate::Error::FeatureNotSupported { detail }) => {
                assert!(detail.contains("median()"), "{detail}");
            }
            Err(other) => panic!("expected FeatureNotSupported, got {other:?}"),
            Ok(_) => panic!("an unknown function built an accumulator"),
        }
    }

    /// A parameterized function reads its parameter from the spec, and a
    /// missing or bad one is an error: `approx_percentile(v, 0.9)` once
    /// answered the median and `approx_topk(v, 3)` the top 10.
    #[test]
    fn a_parameterized_function_uses_its_parameter() {
        let percentile = |fraction| Some(AggregateParam::Percentile { fraction });
        match AggAccum::new(&spec("approx_percentile", percentile(0.9))) {
            Ok(AggAccum::TDigest { pct, .. }) => assert_eq!(pct, 0.9),
            _ => panic!("expected a t-digest at 0.9"),
        }
        match AggAccum::new(&spec("approx_topk", Some(AggregateParam::TopK { k: 3 }))) {
            Ok(AggAccum::TopK { k, .. }) => assert_eq!(k, 3),
            _ => panic!("expected a top-3"),
        }
        for (function, param) in [
            ("approx_percentile", None),
            ("approx_percentile", percentile(1.5)),
            ("approx_percentile", Some(AggregateParam::TopK { k: 3 })),
            ("approx_topk", percentile(0.5)),
            ("approx_topk", Some(AggregateParam::TopK { k: 0 })),
            ("percentile_cont", None),
        ] {
            assert!(
                matches!(
                    AggAccum::new(&spec(function, param)),
                    Err(crate::Error::BadRequest { .. })
                ),
                "{function} with {param:?} must be refused"
            );
        }
    }
}
