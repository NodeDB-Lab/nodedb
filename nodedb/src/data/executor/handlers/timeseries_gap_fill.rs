// SPDX-License-Identifier: BUSL-1.1

//! Gap-fill for bucketed timeseries aggregation results.
//!
//! Fills missing time buckets within the query time range using one of:
//! - `null`: empty accumulators (zero count)
//! - `prev`: LOCF (last observation carried forward)
//! - `linear`: linear interpolation between nearest real neighbors
//! - `<number>`: literal constant value
//!
//! Each aggregate fills from its own real buckets. A filled value keeps the
//! type the real buckets' AVG has: a numeric over integer and decimal
//! columns, a float over float columns. A literal fill is a numeric for an
//! aggregate whose AVG is numeric, read exactly from the literal's text.

use nodedb_query::EvalError;
use nodedb_types::Value;
use rust_decimal::Decimal;

use crate::data::executor::core_loop::TsGroupKeyKind;
use crate::engine::timeseries::columnar_agg::AggAccum;
use crate::engine::timeseries::gap_fill::GapFillStrategy;
use crate::engine::timeseries::grouped_scan::{GroupCell, GroupKey, GroupedAggResult};

/// The key of bucket `ts` of the group whose cells are `cells`.
fn bucket_key(ts: i64, cells: &[GroupCell]) -> GroupKey {
    GroupKey {
        bucket: Some(ts),
        cells: cells.to_vec(),
    }
}

/// Apply gap-fill to a `GroupedAggResult` for bucketed queries.
///
/// Inserts synthetic group entries for missing time buckets within the query
/// time range. Only applies when `bucket_interval_ms > 0` and a gap-fill
/// strategy is specified. `aggregate_kinds` holds the declared kind of the
/// column each aggregate reads, in aggregate order.
///
/// Fails when a real bucket's AVG fails, or when a literal fill cannot be a
/// numeric for an aggregate whose AVG is numeric.
pub(super) fn apply_gap_fill_to_grouped(
    mut result: GroupedAggResult,
    time_range: (i64, i64),
    bucket_interval_ms: i64,
    gap_fill_strategy: &str,
    aggregates: &[(String, String)],
    aggregate_kinds: &[TsGroupKeyKind],
) -> Result<GroupedAggResult, EvalError> {
    let Some(strategy) = GapFillStrategy::parse(gap_fill_strategy) else {
        return Ok(result);
    };
    let num_aggs = aggregates.len();
    let literal_fill = match strategy {
        GapFillStrategy::Literal(val) => Some(literal_values(
            &result,
            num_aggs,
            aggregate_kinds,
            gap_fill_strategy,
            val,
        )?),
        _ => None,
    };

    // Collect every distinct group: the GROUP BY cells of each key.
    let group_suffixes: std::collections::HashSet<Vec<GroupCell>> =
        result.groups.keys().map(|k| k.cells.clone()).collect();

    // With no groups (e.g., no GROUP BY), fill the one group with no cells.
    let suffixes: Vec<Vec<GroupCell>> = if group_suffixes.is_empty() {
        vec![Vec::new()]
    } else {
        group_suffixes.into_iter().collect()
    };

    // Enumerate expected buckets.
    let (start_ms, end_ms) = time_range;
    let aligned_start = start_ms - start_ms.rem_euclid(bucket_interval_ms);

    for suffix in &suffixes {
        let mut ts = aligned_start;
        while ts < end_ms {
            let key = bucket_key(ts, suffix);
            if !result.groups.contains_key(&key) {
                let mut accums = Vec::with_capacity(num_aggs);
                for agg in 0..num_aggs {
                    let probe = Probe {
                        result: &result,
                        suffix,
                        agg,
                        interval: bucket_interval_ms,
                        end_ms,
                    };
                    let fill = match strategy {
                        GapFillStrategy::Null => None,
                        GapFillStrategy::Literal(_) => {
                            literal_fill.as_ref().and_then(|v| v.get(agg)).cloned()
                        }
                        // LOCF: the nearest real bucket before.
                        GapFillStrategy::Prev => probe.prev_avg(ts)?.map(|(_, v)| v),
                        GapFillStrategy::Linear => {
                            match (probe.prev_avg(ts)?, probe.next_avg(ts)?) {
                                (Some((pt, pv)), Some((nt, nv))) => {
                                    Some(interpolate((pt, &pv), (nt, &nv), ts))
                                }
                                (Some((_, v)), None) | (None, Some((_, v))) => Some(v),
                                (None, None) => None,
                            }
                        }
                    };
                    accums.push(fill.map_or_else(AggAccum::default, |v| constant_accum(&v)));
                }
                result.groups.insert(key, accums);
            }
            ts += bucket_interval_ms;
        }
    }

    Ok(result)
}

/// The literal fill of each aggregate: the literal's numeric when the
/// aggregate's AVG is numeric, else the float `val`.
fn literal_values(
    result: &GroupedAggResult,
    num_aggs: usize,
    aggregate_kinds: &[TsGroupKeyKind],
    text: &str,
    val: f64,
) -> Result<Vec<Value>, EvalError> {
    (0..num_aggs)
        .map(|agg| {
            if avg_is_numeric(result, aggregate_kinds.get(agg), agg)? {
                nodedb_query::cast::parse_numeric(text).map(Value::Decimal)
            } else {
                Ok(Value::Float(val))
            }
        })
        .collect()
}

/// Whether aggregate `agg`'s AVG is a numeric. An integer or decimal column
/// gives a numeric AVG and a float column a float one. A column of any
/// other kind gives what its real buckets give, a float when none has a
/// value.
fn avg_is_numeric(
    result: &GroupedAggResult,
    kind: Option<&TsGroupKeyKind>,
    agg: usize,
) -> Result<bool, EvalError> {
    match kind {
        Some(TsGroupKeyKind::Integer | TsGroupKeyKind::Decimal) => Ok(true),
        Some(TsGroupKeyKind::Float) => Ok(false),
        _ => {
            for accums in result.groups.values() {
                if let Some(a) = accums.get(agg)
                    && a.count > 0
                {
                    return Ok(matches!(a.avg_value()?, Value::Decimal(_)));
                }
            }
            Ok(false)
        }
    }
}

/// One accumulator that holds the one value `val`: a decimal stays a
/// decimal, any other value is read as a float.
fn constant_accum(val: &Value) -> AggAccum {
    let mut a = AggAccum::default();
    match val {
        Value::Decimal(d) => a.feed_decimal(*d),
        other => a.feed(value_f64(other)),
    }
    a
}

/// The value at `ts` on the line through `(pt, pv)` and `(nt, nv)`. Two
/// decimals interpolate exactly, at PostgreSQL's division scale. Any other
/// pair interpolates as floats.
fn interpolate(prev: (i64, &Value), next: (i64, &Value), ts: i64) -> Value {
    let ((pt, pv), (nt, nv)) = (prev, next);
    if let (Value::Decimal(p), Value::Decimal(n)) = (pv, nv) {
        let step = n
            .checked_sub(*p)
            .and_then(|span| span.checked_mul(Decimal::from(ts - pt)))
            .and_then(|scaled| {
                nodedb_query::numeric_div::numeric_div(scaled, Decimal::from(nt - pt))
            })
            .and_then(|step| p.checked_add(step));
        if let Some(value) = step {
            return Value::Decimal(value);
        }
    }
    let (p, n) = (value_f64(pv), value_f64(nv));
    let t = (ts - pt) as f64 / (nt - pt) as f64;
    Value::Float(p + t * (n - p))
}

/// `value` read as a float, NaN when it is no number.
fn value_f64(value: &Value) -> f64 {
    match value {
        Value::Decimal(d) => nodedb_types::decimal_float::decimal_to_f64(*d),
        other => other.as_f64().unwrap_or(f64::NAN),
    }
}

/// The AVG a real bucket carries into a gap: its typed AVG, or its running
/// mean when it holds no numeric value. A failed AVG fails the fill.
fn bucket_avg(accum: &AggAccum) -> Result<Value, EvalError> {
    Ok(match accum.avg_value()? {
        value @ (Value::Decimal(_) | Value::Float(_)) => value,
        _ => Value::Float(accum.mean_f64()),
    })
}

/// The real buckets of one aggregate of one group.
struct Probe<'a> {
    result: &'a GroupedAggResult,
    suffix: &'a [GroupCell],
    /// The aggregate's position.
    agg: usize,
    interval: i64,
    end_ms: i64,
}

impl Probe<'_> {
    /// The AVG of the bucket at `ts`, when it has a value.
    fn avg_at(&self, ts: i64) -> Result<Option<Value>, EvalError> {
        match self
            .result
            .groups
            .get(&bucket_key(ts, self.suffix))
            .and_then(|accums| accums.get(self.agg))
        {
            Some(a) if a.count > 0 => bucket_avg(a).map(Some),
            _ => Ok(None),
        }
    }

    /// The nearest bucket with a value before `target_ts`, and its AVG.
    fn prev_avg(&self, target_ts: i64) -> Result<Option<(i64, Value)>, EvalError> {
        let mut ts = target_ts - self.interval;
        while ts >= 0 {
            if let Some(v) = self.avg_at(ts)? {
                return Ok(Some((ts, v)));
            }
            ts -= self.interval;
        }
        Ok(None)
    }

    /// The nearest bucket with a value after `target_ts`, and its AVG.
    fn next_avg(&self, target_ts: i64) -> Result<Option<(i64, Value)>, EvalError> {
        let mut ts = target_ts + self.interval;
        while ts < self.end_ms {
            if let Some(v) = self.avg_at(ts)? {
                return Ok(Some((ts, v)));
            }
            ts += self.interval;
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MINUTE: i64 = 60_000;

    fn dec(text: &str) -> Decimal {
        Decimal::from_str_exact(text).expect("decimal")
    }

    /// A result with one decimal aggregate and real buckets at `buckets`.
    fn decimal_result(buckets: &[(i64, &str)]) -> GroupedAggResult {
        let mut result = GroupedAggResult::new(1);
        for (ts, value) in buckets {
            let mut a = AggAccum::default();
            a.feed_decimal(dec(value));
            result.groups.insert(bucket_key(*ts, &[]), vec![a]);
        }
        result
    }

    fn avg_at(result: &GroupedAggResult, ts: i64) -> Value {
        result.groups[&bucket_key(ts, &[])][0]
            .avg_value()
            .expect("avg")
    }

    fn fill(result: GroupedAggResult, strategy: &str, kind: TsGroupKeyKind) -> GroupedAggResult {
        let aggregates = [("avg".to_string(), "price".to_string())];
        apply_gap_fill_to_grouped(
            result,
            (0, 3 * MINUTE),
            MINUTE,
            strategy,
            &aggregates,
            &[kind],
        )
        .expect("gap fill")
    }

    /// A literal fill of a numeric AVG is the literal's exact numeric.
    #[test]
    fn literal_fill_of_a_decimal_avg_is_a_decimal() {
        let filled = fill(
            decimal_result(&[(0, "1.25")]),
            "0.1",
            TsGroupKeyKind::Decimal,
        );
        let Value::Decimal(d) = avg_at(&filled, MINUTE) else {
            panic!("a literal fill of a numeric AVG is a Decimal");
        };
        assert_eq!(d.normalize(), dec("0.1"));
        // An untyped column follows its real buckets.
        let filled = fill(decimal_result(&[(0, "1.25")]), "2", TsGroupKeyKind::Text);
        assert!(matches!(avg_at(&filled, MINUTE), Value::Decimal(_)));
        // A float column fills a float.
        let filled = fill(decimal_result(&[]), "2", TsGroupKeyKind::Float);
        assert_eq!(avg_at(&filled, MINUTE), Value::Float(2.0));
    }

    /// LOCF and interpolation between decimal buckets stay decimal.
    #[test]
    fn prev_and_linear_fills_keep_the_decimal_type() {
        let filled = fill(
            decimal_result(&[(0, "1.0"), (2 * MINUTE, "2.0")]),
            "linear",
            TsGroupKeyKind::Decimal,
        );
        let Value::Decimal(mid) = avg_at(&filled, MINUTE) else {
            panic!("an interpolated numeric AVG is a Decimal");
        };
        assert_eq!(mid.normalize(), dec("1.5"));
        let filled = fill(
            decimal_result(&[(0, "1.0")]),
            "prev",
            TsGroupKeyKind::Decimal,
        );
        let Value::Decimal(carried) = avg_at(&filled, 2 * MINUTE) else {
            panic!("a carried numeric AVG is a Decimal");
        };
        assert_eq!(carried.normalize(), dec("1"));
    }

    /// A real bucket whose AVG fails fails the fill.
    #[test]
    fn a_failed_bucket_avg_fails_the_fill() {
        let mut result = GroupedAggResult::new(1);
        let mut overflowed = AggAccum::default();
        overflowed.feed_decimal(Decimal::MAX);
        overflowed.feed_decimal(Decimal::MAX);
        result.groups.insert(bucket_key(0, &[]), vec![overflowed]);
        let aggregates = [("avg".to_string(), "price".to_string())];
        let filled = apply_gap_fill_to_grouped(
            result,
            (0, 2 * MINUTE),
            MINUTE,
            "prev",
            &aggregates,
            &[TsGroupKeyKind::Decimal],
        );
        assert!(matches!(filled, Err(EvalError::NumericOverflow { .. })));
    }
}
