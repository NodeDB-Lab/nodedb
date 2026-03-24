//! Columnar aggregation functions for timeseries data.
//!
//! Operates on contiguous column slices (`&[f64]`, `&[i64]`) for
//! cache-friendly, SIMD-optimizable aggregation. These functions are
//! building blocks for `time_bucket` GROUP BY queries.
//!
//! When numr SIMD is available, the hot inner loops can be replaced
//! with vectorized equivalents. The current implementation is scalar
//! but structured for easy SIMD drop-in.

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
/// Uses Kahan compensated summation for numerical accuracy.
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

    let mut sum = 0.0f64;
    let mut compensation = 0.0f64; // Kahan compensator
    let mut min = f64::INFINITY;
    let mut max = f64::NEG_INFINITY;
    let mut count = 0u64;

    for &v in values {
        if v.is_nan() {
            continue;
        }
        count += 1;

        // Kahan summation.
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
        first: values[0],
        last: values[values.len() - 1],
    }
}

/// Compute aggregates over an i64 column slice.
pub fn aggregate_i64(values: &[i64]) -> AggResultI64 {
    if values.is_empty() {
        return AggResultI64::default();
    }

    let mut sum = 0i128; // Use i128 to avoid overflow.
    let mut min = i64::MAX;
    let mut max = i64::MIN;
    let mut count = 0u64;

    for &v in values {
        count += 1;
        sum += v as i128;
        if v < min {
            min = v;
        }
        if v > max {
            max = v;
        }
    }

    AggResultI64 {
        count,
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
    // Scalar loop — replace with SIMD comparison when numr provides it.
    let mut indices = Vec::new();
    for (i, &ts) in timestamps.iter().enumerate() {
        if ts >= min_ts && ts <= max_ts {
            indices.push(i as u32);
        }
    }
    indices
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
pub fn aggregate_by_time_bucket(
    timestamps: &[i64],
    values: &[f64],
    bucket_interval_ms: i64,
) -> Vec<(i64, AggResult)> {
    use super::time_bucket::time_bucket;
    use std::collections::BTreeMap;

    let mut buckets: BTreeMap<i64, Vec<f64>> = BTreeMap::new();
    for (i, &ts) in timestamps.iter().enumerate() {
        let bucket = time_bucket(bucket_interval_ms, ts);
        buckets.entry(bucket).or_default().push(values[i]);
    }

    buckets
        .into_iter()
        .map(|(bucket, vals)| (bucket, aggregate_f64(&vals)))
        .collect()
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
