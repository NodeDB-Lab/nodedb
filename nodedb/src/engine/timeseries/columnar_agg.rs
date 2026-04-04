//! Columnar aggregation functions for timeseries data.
//!
//! Operates on contiguous column slices (`&[f64]`, `&[i64]`) for
//! cache-friendly aggregation. Hot paths (sum/min/max/range filter)
//! dispatch to SIMD kernels via `simd_agg::ts_runtime()` which
//! auto-detects AVX-512 / AVX2 / NEON at startup.

use super::simd_agg::ts_runtime;

/// Aggregation result for a group of rows.
#[derive(Debug, Clone, Default)]
pub struct AggResult {
    pub count: u64,
    pub sum: f64,
    pub min: f64,
    pub max: f64,
    /// First value in group (by insertion order).
    pub first: f64,
    /// Last value in group (by insertion order).
    pub last: f64,
}

impl AggResult {
    pub fn avg(&self) -> f64 {
        if self.count == 0 {
            f64::NAN
        } else {
            self.sum / self.count as f64
        }
    }
}

/// Compute all standard aggregates over an f64 column slice.
///
/// Dispatches sum/min/max to SIMD kernels (AVX-512/AVX2/NEON) via
/// `ts_runtime()`. Falls back to scalar with Kahan compensation.
pub fn aggregate_f64(values: &[f64]) -> AggResult {
    if values.is_empty() {
        return AggResult {
            min: f64::NAN,
            max: f64::NAN,
            first: f64::NAN,
            last: f64::NAN,
            ..Default::default()
        };
    }

    // Filter out NaN values for SIMD paths (SIMD min/max don't handle NaN correctly).
    let has_nan = values.iter().any(|v| v.is_nan());

    let (sum, min, max, count) = if has_nan {
        // Slow path: skip NaN values.
        let mut s = 0.0f64;
        let mut comp = 0.0f64;
        let mut mn = f64::INFINITY;
        let mut mx = f64::NEG_INFINITY;
        let mut c = 0u64;
        for &v in values {
            if v.is_nan() {
                continue;
            }
            c += 1;
            let y = v - comp;
            let t = s + y;
            comp = (t - s) - y;
            s = t;
            if v < mn {
                mn = v;
            }
            if v > mx {
                mx = v;
            }
        }
        (s, mn, mx, c)
    } else {
        // Fast path: SIMD dispatch for clean data.
        let rt = ts_runtime();
        let s = (rt.sum_f64)(values);
        let mn = (rt.min_f64)(values);
        let mx = (rt.max_f64)(values);
        (s, mn, mx, values.len() as u64)
    };

    AggResult {
        count,
        sum,
        min,
        max,
        first: values[0],
        last: values[values.len() - 1],
    }
}

/// Compute aggregates over an i64 column slice.
///
/// Dispatches sum/min/max to SIMD kernels (AVX-512/AVX2/NEON) via
/// `i64_runtime()`. Falls back to scalar with i128 accumulator.
pub fn aggregate_i64(values: &[i64]) -> AggResultI64 {
    if values.is_empty() {
        return AggResultI64::default();
    }

    let rt = nodedb_query::simd_agg_i64::i64_runtime();
    let sum = (rt.sum_i64)(values);
    let min = (rt.min_i64)(values);
    let max = (rt.max_i64)(values);

    AggResultI64 {
        count: values.len() as u64,
        sum,
        min,
        max,
        first: values[0],
        last: values[values.len() - 1],
    }
}

/// Aggregation result for i64 columns.
#[derive(Debug, Clone, Default)]
pub struct AggResultI64 {
    pub count: u64,
    pub sum: i128,
    pub min: i64,
    pub max: i64,
    pub first: i64,
    pub last: i64,
}

impl AggResultI64 {
    pub fn avg(&self) -> f64 {
        if self.count == 0 {
            f64::NAN
        } else {
            self.sum as f64 / self.count as f64
        }
    }
}

/// Filter a column by a timestamp range bitmask.
///
/// Returns indices of rows where `timestamps[i]` is within `[min_ts, max_ts]`.
/// The result can be used as a selection vector for column scans.
pub fn timestamp_range_filter(timestamps: &[i64], min_ts: i64, max_ts: i64) -> Vec<u32> {
    let rt = ts_runtime();
    (rt.range_filter_i64)(timestamps, min_ts, max_ts)
}

/// Aggregate f64 values at selected row indices.
pub fn aggregate_f64_filtered(values: &[f64], indices: &[u32]) -> AggResult {
    if indices.is_empty() {
        return AggResult {
            min: f64::NAN,
            max: f64::NAN,
            first: f64::NAN,
            last: f64::NAN,
            ..Default::default()
        };
    }

    let mut sum = 0.0f64;
    let mut compensation = 0.0f64;
    let mut min = f64::INFINITY;
    let mut max = f64::NEG_INFINITY;
    let mut count = 0u64;

    for &idx in indices {
        let v = values[idx as usize];
        if v.is_nan() {
            continue;
        }
        count += 1;
        let y = v - compensation;
        let t = sum + y;
        compensation = (t - sum) - y;
        sum = t;
        if v < min {
            min = v;
        }
        if v > max {
            max = v;
        }
    }

    AggResult {
        count,
        sum,
        min,
        max,
        first: values[indices[0] as usize],
        last: values[indices[indices.len() - 1] as usize],
    }
}

/// Group rows by time bucket and compute per-bucket aggregates.
///
/// `timestamps` and `values` must have the same length.
/// Returns `(bucket_start, AggResult)` pairs sorted by bucket.
///
/// Uses streaming accumulators — O(B) allocations where B = number of
/// buckets, not O(N) like the previous Vec-per-bucket approach.
pub fn aggregate_by_time_bucket(
    timestamps: &[i64],
    values: &[f64],
    bucket_interval_ms: i64,
) -> Vec<(i64, AggResult)> {
    use super::time_bucket::time_bucket;
    use std::collections::BTreeMap;

    let mut buckets: BTreeMap<i64, AggAccum> = BTreeMap::new();
    for (i, &ts) in timestamps.iter().enumerate() {
        let bucket_key = time_bucket(bucket_interval_ms, ts);
        let accum = buckets.entry(bucket_key).or_default();
        let v = values[i];
        if !v.is_nan() {
            accum.feed(v);
        }
    }

    buckets
        .into_iter()
        .map(|(bucket, accum)| (bucket, accum.into_agg_result()))
        .collect()
}

/// Count-only time-bucket aggregation (no value column needed).
///
/// For `COUNT(*)` queries — avoids reading a Float64 column entirely.
pub fn count_by_time_bucket(timestamps: &[i64], bucket_interval_ms: i64) -> Vec<(i64, AggResult)> {
    use super::time_bucket::time_bucket;
    use std::collections::BTreeMap;

    let mut buckets: BTreeMap<i64, u64> = BTreeMap::new();
    for &ts in timestamps {
        *buckets
            .entry(time_bucket(bucket_interval_ms, ts))
            .or_default() += 1;
    }

    buckets
        .into_iter()
        .map(|(bucket, count)| {
            (
                bucket,
                AggResult {
                    count,
                    ..Default::default()
                },
            )
        })
        .collect()
}

/// Streaming accumulator for single-pass aggregation.
///
/// Maintains running sum (with Kahan compensation), min, max, count,
/// first, and last. Zero intermediate allocation.
#[derive(Debug, Clone)]
pub struct AggAccum {
    pub count: u64,
    sum: f64,
    compensation: f64,
    pub min: f64,
    pub max: f64,
    first: f64,
    last: f64,
    /// Welford's M2 for online variance/stddev computation.
    mean: f64,
    m2: f64,
}

impl Default for AggAccum {
    fn default() -> Self {
        Self {
            count: 0,
            sum: 0.0,
            compensation: 0.0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
            first: f64::NAN,
            last: f64::NAN,
            mean: 0.0,
            m2: 0.0,
        }
    }
}

impl AggAccum {
    /// Feed a single value into the accumulator.
    pub fn feed(&mut self, v: f64) {
        if self.count == 0 {
            self.first = v;
        }
        self.last = v;
        self.count += 1;
        // Kahan compensated summation.
        let y = v - self.compensation;
        let t = self.sum + y;
        self.compensation = (t - self.sum) - y;
        self.sum = t;
        if v < self.min {
            self.min = v;
        }
        if v > self.max {
            self.max = v;
        }
        // Welford's online variance (for stddev).
        let delta = v - self.mean;
        self.mean += delta / self.count as f64;
        let delta2 = v - self.mean;
        self.m2 += delta * delta2;
    }

    /// Increment count without a value (for `COUNT(*)` on non-numeric columns).
    pub fn feed_count_only(&mut self) {
        self.count += 1;
    }

    /// Merge another accumulator into this one.
    pub fn merge(&mut self, other: &AggAccum) {
        if other.count == 0 {
            return;
        }
        if self.count == 0 {
            self.first = other.first;
        }
        self.last = other.last;

        // Chan's parallel algorithm for merging Welford variance.
        let n_a = self.count as f64;
        let n_b = other.count as f64;
        let delta = other.mean - self.mean;
        self.m2 += other.m2 + delta * delta * n_a * n_b / (n_a + n_b);
        self.mean = (n_a * self.mean + n_b * other.mean) / (n_a + n_b);

        self.count += other.count;
        self.sum += other.sum;
        if other.min < self.min {
            self.min = other.min;
        }
        if other.max > self.max {
            self.max = other.max;
        }
    }

    /// Population standard deviation. Returns 0.0 if fewer than 2 values.
    pub fn stddev_population(&self) -> f64 {
        if self.count < 2 {
            return 0.0;
        }
        (self.m2 / self.count as f64).max(0.0).sqrt()
    }

    /// Convert to final `AggResult`.
    pub fn into_agg_result(self) -> AggResult {
        AggResult {
            count: self.count,
            sum: self.sum,
            min: if self.count == 0 { f64::NAN } else { self.min },
            max: if self.count == 0 { f64::NAN } else { self.max },
            first: self.first,
            last: self.last,
        }
    }

    pub fn sum(&self) -> f64 {
        self.sum
    }

    pub fn first(&self) -> f64 {
        self.first
    }

    pub fn last(&self) -> f64 {
        self.last
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_aggregate() {
        let result = aggregate_f64(&[]);
        assert_eq!(result.count, 0);
        assert!(result.min.is_nan());
    }

    #[test]
    fn simple_aggregate() {
        let values = [1.0, 2.0, 3.0, 4.0, 5.0];
        let result = aggregate_f64(&values);
        assert_eq!(result.count, 5);
        assert!((result.sum - 15.0).abs() < f64::EPSILON);
        assert!((result.min - 1.0).abs() < f64::EPSILON);
        assert!((result.max - 5.0).abs() < f64::EPSILON);
        assert!((result.avg() - 3.0).abs() < f64::EPSILON);
        assert!((result.first - 1.0).abs() < f64::EPSILON);
        assert!((result.last - 5.0).abs() < f64::EPSILON);
    }

    #[test]
    fn kahan_accuracy() {
        // Kahan compensated summation should handle this better than naive sum.
        let mut values = vec![1e-10; 1_000_000];
        values.insert(0, 1.0);
        let result = aggregate_f64(&values);
        let expected = 1.0 + 1_000_000.0 * 1e-10;
        let error = (result.sum - expected).abs();
        assert!(
            error < 1e-6,
            "Kahan sum error too large: {error} (sum={}, expected={expected})",
            result.sum
        );
    }

    #[test]
    fn i64_aggregate() {
        let values = [10, 20, 30, 40, 50];
        let result = aggregate_i64(&values);
        assert_eq!(result.count, 5);
        assert_eq!(result.sum, 150);
        assert_eq!(result.min, 10);
        assert_eq!(result.max, 50);
        assert!((result.avg() - 30.0).abs() < f64::EPSILON);
    }

    #[test]
    fn timestamp_filter() {
        let timestamps = [100, 200, 300, 400, 500];
        let indices = timestamp_range_filter(&timestamps, 200, 400);
        assert_eq!(indices, vec![1, 2, 3]);
    }

    #[test]
    fn filtered_aggregate() {
        let values = [1.0, 2.0, 3.0, 4.0, 5.0];
        let indices = vec![1, 2, 3]; // select values[1..3]
        let result = aggregate_f64_filtered(&values, &indices);
        assert_eq!(result.count, 3);
        assert!((result.sum - 9.0).abs() < f64::EPSILON);
        assert!((result.first - 2.0).abs() < f64::EPSILON);
        assert!((result.last - 4.0).abs() < f64::EPSILON);
    }

    #[test]
    fn time_bucket_aggregate() {
        let timestamps = [0, 100, 200, 300, 400, 500, 600, 700, 800, 900];
        let values = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0];
        let buckets = aggregate_by_time_bucket(&timestamps, &values, 500);
        assert_eq!(buckets.len(), 2);
        assert_eq!(buckets[0].0, 0);
        assert_eq!(buckets[0].1.count, 5);
        assert!((buckets[0].1.sum - 15.0).abs() < f64::EPSILON);
        assert_eq!(buckets[1].0, 500);
        assert_eq!(buckets[1].1.count, 5);
        assert!((buckets[1].1.sum - 40.0).abs() < f64::EPSILON);
    }

    #[test]
    fn nan_values_skipped() {
        let values = [1.0, f64::NAN, 3.0, f64::NAN, 5.0];
        let result = aggregate_f64(&values);
        assert_eq!(result.count, 3);
        assert!((result.sum - 9.0).abs() < f64::EPSILON);
    }

    #[test]
    fn i64_overflow_safe() {
        let values = [i64::MAX, i64::MAX];
        let result = aggregate_i64(&values);
        assert_eq!(result.sum, 2 * i64::MAX as i128);
    }
}
