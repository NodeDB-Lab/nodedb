// SPDX-License-Identifier: BUSL-1.1

//! `AggAccum::finalize` — consume the accumulator and produce the result `Value`.

use super::state::AggAccum;
use nodedb_physical::physical_plan::AggregateSpec;
use nodedb_types::Value;
use nodedb_types::array_agg::Moments;

impl AggAccum {
    /// Consume the accumulator and produce the final `Value`.
    ///
    /// Fails with `EvalError::NumericOverflow` when an exact integer SUM /
    /// AVG total lies outside the `Decimal` range.
    pub(crate) fn finalize(self, agg: &AggregateSpec) -> Result<Value, nodedb_query::EvalError> {
        Ok(match self {
            AggAccum::Count { n } => Value::Integer(n as i64),
            // No contributing value is NULL for both SUM and AVG: SUM has no
            // zero identity in SQL. Integer inputs total exactly.
            AggAccum::SumAvg { sum } => {
                if agg.function == "avg" {
                    sum.avg()?
                } else {
                    sum.sum()?
                }
            }
            AggAccum::SumAvgDistinct { seen } => {
                // A DISTINCT sum is order-independent, so the arbitrary map
                // iteration order gives a deterministic result.
                let mut sum = nodedb_query::ExactSum::new();
                for v in seen.values() {
                    sum.add_value(v);
                }
                if agg.function == "avg_distinct" {
                    sum.avg()?
                } else {
                    sum.sum()?
                }
            }
            AggAccum::Min { best } => best.unwrap_or(Value::Null),
            AggAccum::Max { best } => best.unwrap_or(Value::Null),
            AggAccum::CountDistinct { seen } => Value::Integer(seen.len() as i64),
            AggAccum::Welford { moments } => welford_finalize(&agg.function, &moments),
            AggAccum::Hll { hll } => Value::Integer(hll.estimate().round() as i64),
            AggAccum::TDigest { digest, pct } => {
                let r = digest.quantile(pct);
                if r.is_nan() {
                    Value::Null
                } else {
                    Value::Float(r)
                }
            }
            AggAccum::TopK { ss, k } => {
                let arr: Vec<Value> = ss
                    .top_k()
                    .into_iter()
                    .take(k)
                    .map(|(item, count, error)| {
                        Value::Object(
                            [
                                ("item".to_string(), Value::Integer(item as i64)),
                                ("count".to_string(), Value::Integer(count as i64)),
                                ("error".to_string(), Value::Integer(error as i64)),
                            ]
                            .into_iter()
                            .collect(),
                        )
                    })
                    .collect();
                Value::Array(arr)
            }
            AggAccum::ArrayAgg { values } => Value::Array(values),
            AggAccum::ArrayAggDistinct { values, .. } => {
                Value::Array(values.into_iter().map(|(_, value)| value).collect())
            }
            AggAccum::PercentileCont { mut values, pct } => {
                if values.is_empty() {
                    return Ok(Value::Null);
                }
                values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                let idx = (pct * (values.len() - 1) as f64).clamp(0.0, (values.len() - 1) as f64);
                let lo = idx.floor() as usize;
                let hi = idx.ceil() as usize;
                let frac = idx - lo as f64;
                Value::Float(values[lo] * (1.0 - frac) + values[hi] * frac)
            }
            AggAccum::StringAgg { parts } => Value::String(parts.join(",")),
        })
    }
}

/// The final value of Welford `moments`, with PostgreSQL semantics.
///
/// - `stddev`, `stddev_samp`, `variance`, `var_samp`: sample forms, divided
///   by `n - 1`. NULL for fewer than 2 values.
/// - `stddev_pop`, `var_pop`: population forms, divided by `n`. NULL for no
///   value, 0 for one value.
/// - `ts_stddev`: population form, NULL for fewer than 2 values, as the
///   native timeseries scan computes it.
fn welford_finalize(function: &str, moments: &Moments) -> Value {
    let variance = match function {
        "stddev_pop" | "var_pop" => moments.var_pop(),
        "ts_stddev" if moments.count >= 2 => moments.var_pop(),
        "stddev" | "stddev_samp" | "variance" | "var_samp" => moments.var_samp(),
        _ => None,
    };
    let is_stddev = function.starts_with("stddev") || function == "ts_stddev";
    match variance {
        Some(variance) => Value::Float(if is_stddev { variance.sqrt() } else { variance }),
        None => Value::Null,
    }
}

#[cfg(test)]
mod tests {
    use super::welford_finalize;
    use nodedb_types::Value;
    use nodedb_types::array_agg::Moments;

    /// Values 2, 4, 4, 4, 5, 5, 7, 9: mean 5, sum of squared deviations 32.
    const N: u64 = 8;
    const M2: f64 = 32.0;

    fn moments(count: u64, m2: f64) -> Moments {
        Moments {
            count,
            mean: 0.0,
            m2,
        }
    }

    fn float(v: Value) -> f64 {
        match v {
            Value::Float(f) => f,
            other => panic!("expected a float, got {other:?}"),
        }
    }

    #[test]
    fn sample_forms_divide_by_n_minus_one() {
        let sample_variance = 32.0 / 7.0;
        for function in ["variance", "var_samp"] {
            assert_eq!(
                float(welford_finalize(function, &moments(N, M2))),
                sample_variance
            );
        }
        for function in ["stddev", "stddev_samp"] {
            assert_eq!(
                float(welford_finalize(function, &moments(N, M2))),
                sample_variance.sqrt()
            );
        }
    }

    #[test]
    fn population_forms_divide_by_n() {
        assert_eq!(float(welford_finalize("var_pop", &moments(N, M2))), 4.0);
        assert_eq!(float(welford_finalize("stddev_pop", &moments(N, M2))), 2.0);
        assert_eq!(float(welford_finalize("ts_stddev", &moments(N, M2))), 2.0);
    }

    #[test]
    fn small_inputs_follow_postgres() {
        for function in ["stddev", "stddev_samp", "variance", "var_samp", "ts_stddev"] {
            assert!(matches!(
                welford_finalize(function, &moments(1, 0.0)),
                Value::Null
            ));
        }
        assert_eq!(float(welford_finalize("stddev_pop", &moments(1, 0.0))), 0.0);
        assert_eq!(float(welford_finalize("var_pop", &moments(1, 0.0))), 0.0);
        for function in ["stddev_pop", "var_pop"] {
            assert!(matches!(
                welford_finalize(function, &moments(0, 0.0)),
                Value::Null
            ));
        }
    }
}
