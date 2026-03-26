//! Partial aggregate state for incremental merging.
//!
//! Stores enough state per (bucket, group_key) to merge incrementally:
//! count, sum, min, max, first/last timestamps and values.

use serde::{Deserialize, Serialize};

use super::definition::AggFunction;

/// Partial aggregate state for a single (bucket, group_key) combination.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartialAggregate {
    pub bucket_ts: i64,
    /// Symbol IDs for GROUP BY columns.
    pub group_key: Vec<u32>,
    pub count: u64,
    pub sum: f64,
    pub min: f64,
    pub max: f64,
    pub first_ts: i64,
    pub first_val: f64,
    pub last_ts: i64,
    pub last_val: f64,
}

impl PartialAggregate {
    /// Create from a single sample.
    pub fn new(bucket_ts: i64, group_key: Vec<u32>, ts: i64, val: f64) -> Self {
        Self {
            bucket_ts,
            group_key,
            count: 1,
            sum: val,
            min: val,
            max: val,
            first_ts: ts,
            first_val: val,
            last_ts: ts,
            last_val: val,
        }
    }

    /// Merge another sample into this partial aggregate.
    pub fn merge_sample(&mut self, ts: i64, val: f64) {
        self.count += 1;
        self.sum += val;
        if val < self.min {
            self.min = val;
        }
        if val > self.max {
            self.max = val;
        }
        if ts < self.first_ts {
            self.first_ts = ts;
            self.first_val = val;
        }
        if ts > self.last_ts {
            self.last_ts = ts;
            self.last_val = val;
        }
    }

    /// Merge another partial aggregate (for cross-shard or incremental merge).
    pub fn merge_partial(&mut self, other: &PartialAggregate) {
        self.count += other.count;
        self.sum += other.sum;
        if other.min < self.min {
            self.min = other.min;
        }
        if other.max > self.max {
            self.max = other.max;
        }
        if other.first_ts < self.first_ts {
            self.first_ts = other.first_ts;
            self.first_val = other.first_val;
        }
        if other.last_ts > self.last_ts {
            self.last_ts = other.last_ts;
            self.last_val = other.last_val;
        }
    }

    /// Compute a final aggregate value from the partial state.
    pub fn finalize(&self, function: AggFunction) -> f64 {
        match function {
            AggFunction::Sum => self.sum,
            AggFunction::Count => self.count as f64,
            AggFunction::Min => self.min,
            AggFunction::Max => self.max,
            AggFunction::Avg => {
                if self.count == 0 {
                    0.0
                } else {
                    self.sum / self.count as f64
                }
            }
            AggFunction::First => self.first_val,
            AggFunction::Last => self.last_val,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_sample() {
        let pa = PartialAggregate::new(0, vec![], 100, 42.0);
        assert_eq!(pa.count, 1);
        assert_eq!(pa.finalize(AggFunction::Sum), 42.0);
        assert_eq!(pa.finalize(AggFunction::Avg), 42.0);
    }

    #[test]
    fn merge_samples() {
        let mut pa = PartialAggregate::new(0, vec![], 100, 10.0);
        pa.merge_sample(200, 20.0);
        pa.merge_sample(300, 30.0);

        assert_eq!(pa.finalize(AggFunction::Count), 3.0);
        assert_eq!(pa.finalize(AggFunction::Sum), 60.0);
        assert_eq!(pa.finalize(AggFunction::Min), 10.0);
        assert_eq!(pa.finalize(AggFunction::Max), 30.0);
        assert!((pa.finalize(AggFunction::Avg) - 20.0).abs() < f64::EPSILON);
        assert_eq!(pa.finalize(AggFunction::First), 10.0);
        assert_eq!(pa.finalize(AggFunction::Last), 30.0);
    }

    #[test]
    fn merge_partials() {
        let mut a = PartialAggregate::new(0, vec![], 100, 10.0);
        a.merge_sample(200, 20.0);

        let mut b = PartialAggregate::new(0, vec![], 50, 5.0);
        b.merge_sample(300, 30.0);

        a.merge_partial(&b);
        assert_eq!(a.count, 4);
        assert_eq!(a.sum, 65.0);
        assert_eq!(a.min, 5.0);
        assert_eq!(a.max, 30.0);
        assert_eq!(a.first_ts, 50);
        assert_eq!(a.first_val, 5.0);
        assert_eq!(a.last_ts, 300);
        assert_eq!(a.last_val, 30.0);
    }
}
