// SPDX-License-Identifier: BUSL-1.1

//! Gap-fill for bucketed timeseries aggregation results.
//!
//! Fills missing time buckets within the query time range using one of:
//! - `null`: empty accumulators (zero count)
//! - `prev`: LOCF (last observation carried forward)
//! - `linear`: linear interpolation between nearest real neighbors
//! - `<number>`: literal constant value

use crate::engine::timeseries::columnar_agg::AggAccum;
use crate::engine::timeseries::gap_fill::GapFillStrategy;
use crate::engine::timeseries::grouped_scan::GroupedAggResult;

/// Apply gap-fill to a `GroupedAggResult` for bucketed queries.
///
/// Inserts synthetic group entries for missing time buckets within the query
/// time range. Only applies when `bucket_interval_ms > 0` and a gap-fill
/// strategy is specified.
pub(super) fn apply_gap_fill_to_grouped(
    mut result: GroupedAggResult,
    time_range: (i64, i64),
    bucket_interval_ms: i64,
    gap_fill_strategy: &str,
    _group_by: &[String],
    aggregates: &[(String, String)],
) -> GroupedAggResult {
    let strategy = match GapFillStrategy::parse(gap_fill_strategy) {
        Some(s) => s,
        None => return result,
    };

    let num_aggs = aggregates.len();

    // Collect all unique group suffixes (parts after the bucket timestamp).
    let group_suffixes: std::collections::HashSet<String> = result
        .groups
        .keys()
        .map(|k| {
            // Key format: "bucket_ts\0group1\0group2"
            k.find('\0')
                .map(|pos| k[pos..].to_string())
                .unwrap_or_default()
        })
        .collect();

    // If no groups found (e.g., no GROUP BY), use a single empty suffix.
    let suffixes: Vec<String> =
        if group_suffixes.is_empty() || group_suffixes.iter().all(|s| s.is_empty()) {
            vec![String::new()]
        } else {
            group_suffixes.into_iter().collect()
        };

    // Enumerate expected buckets.
    let (start_ms, end_ms) = time_range;
    let aligned_start = start_ms - start_ms.rem_euclid(bucket_interval_ms);

    for suffix in &suffixes {
        let mut ts = aligned_start;
        while ts < end_ms {
            let key = format!("{ts}{suffix}");
            if !result.groups.contains_key(&key) {
                // Missing bucket — create synthetic accumulators.
                let accums = match strategy {
                    GapFillStrategy::Null => make_empty_accums(num_aggs),
                    GapFillStrategy::Literal(val) => make_constant_accums(num_aggs, val),
                    GapFillStrategy::Prev => {
                        // LOCF: search backward for the nearest real bucket.
                        match find_prev_bucket_avg(&result, suffix, ts, bucket_interval_ms, end_ms)
                        {
                            Some((_, pv)) => make_constant_accums(num_aggs, pv),
                            None => make_empty_accums(num_aggs),
                        }
                    }
                    GapFillStrategy::Linear => {
                        // Find prev and next real buckets for interpolation.
                        let prev_val =
                            find_prev_bucket_avg(&result, suffix, ts, bucket_interval_ms, end_ms);
                        let next_val =
                            find_next_bucket_avg(&result, suffix, ts, bucket_interval_ms, end_ms);
                        match (prev_val, next_val) {
                            (Some((pt, pv)), Some((nt, nv))) => {
                                let t = (ts - pt) as f64 / (nt - pt) as f64;
                                let interp = pv + t * (nv - pv);
                                make_constant_accums(num_aggs, interp)
                            }
                            (Some((_, pv)), None) => make_constant_accums(num_aggs, pv),
                            (None, Some((_, nv))) => make_constant_accums(num_aggs, nv),
                            (None, None) => make_empty_accums(num_aggs),
                        }
                    }
                };
                result.groups.insert(key, accums);
            }
            ts += bucket_interval_ms;
        }
    }

    result
}

fn make_empty_accums(n: usize) -> Vec<AggAccum> {
    (0..n).map(|_| AggAccum::default()).collect()
}

fn make_constant_accums(n: usize, val: f64) -> Vec<AggAccum> {
    (0..n)
        .map(|_| {
            let mut a = AggAccum::default();
            a.feed(val);
            a
        })
        .collect()
}

/// Find the nearest previous real bucket's average value.
fn find_prev_bucket_avg(
    result: &GroupedAggResult,
    suffix: &str,
    target_ts: i64,
    interval: i64,
    _end_ms: i64,
) -> Option<(i64, f64)> {
    let mut ts = target_ts - interval;
    while ts >= 0 {
        let key = format!("{ts}{suffix}");
        if let Some(accums) = result.groups.get(&key)
            && let Some(a) = accums.first()
            && a.count > 0
        {
            return Some((ts, a.mean_f64()));
        }
        ts -= interval;
    }
    None
}

/// Find the nearest next real bucket's average value.
fn find_next_bucket_avg(
    result: &GroupedAggResult,
    suffix: &str,
    target_ts: i64,
    interval: i64,
    end_ms: i64,
) -> Option<(i64, f64)> {
    let mut ts = target_ts + interval;
    while ts < end_ms {
        let key = format!("{ts}{suffix}");
        if let Some(accums) = result.groups.get(&key)
            && let Some(a) = accums.first()
            && a.count > 0
        {
            return Some((ts, a.mean_f64()));
        }
        ts += interval;
    }
    None
}
