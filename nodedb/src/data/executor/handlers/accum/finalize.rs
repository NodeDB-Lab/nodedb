// SPDX-License-Identifier: BUSL-1.1

//! `AggAccum::finalize` — consume the accumulator and produce the result `Value`.

use super::state::AggAccum;
use nodedb_physical::physical_plan::AggregateSpec;
use nodedb_types::Value;

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
            AggAccum::Welford { n, mean: _, m2 } => {
                if n < 2 {
                    return Ok(Value::Null);
                }
                let population = matches!(
                    agg.function.as_str(),
                    "stddev" | "stddev_pop" | "variance" | "var_pop"
                );
                let divisor = if population { n as f64 } else { (n - 1) as f64 };
                let variance = m2 / divisor;
                let result = if agg.function.contains("stddev") {
                    variance.sqrt()
                } else {
                    variance
                };
                Value::Float(result)
            }
            AggAccum::Hll { hll } => Value::Integer(hll.estimate().round() as i64),
            AggAccum::TDigest { digest } => {
                let pct = agg
                    .field
                    .find(':')
                    .and_then(|i| agg.field[..i].parse().ok())
                    .unwrap_or(0.5);
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
            AggAccum::ArrayAggDistinct { values, .. } => Value::Array(values),
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
