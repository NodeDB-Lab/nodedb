//! `time_bucket(interval, timestamp)` function for timeseries downsampling.
//!
//! Truncates a timestamp to the start of the interval it falls in.
//! Used in `GROUP BY time_bucket('5m', ts)` for dashboard-style aggregation.
//!
//! Examples:
//! - `time_bucket('1h', 1704070800000)` → `1704067200000` (truncated to hour)
//! - `time_bucket('5m', 1704067500000)` → `1704067200000` (truncated to 5-min)
//! - `time_bucket('1d', ts)` → day start

/// Truncate a timestamp (ms) to the start of the given interval bucket.
///
/// `interval_ms` must be > 0. For calendar intervals (month/year),
/// use `time_bucket_calendar` instead.
pub fn time_bucket(interval_ms: i64, timestamp_ms: i64) -> i64 {
    if interval_ms <= 0 {
        return timestamp_ms;
    }
    (timestamp_ms / interval_ms) * interval_ms
}

/// Parse an interval string like "5m", "1h", "1d" into milliseconds.
pub fn parse_interval_ms(s: &str) -> crate::Result<i64> {
    let s = s.trim();
    if s.is_empty() {
        return Err(crate::Error::BadRequest {
            detail: "empty interval".into(),
        });
    }

    let (num_str, unit) = if s.len() > 1 && s.as_bytes()[s.len() - 1].is_ascii_alphabetic() {
        (&s[..s.len() - 1], &s[s.len() - 1..])
    } else {
        return Err(crate::Error::BadRequest {
            detail: format!("invalid interval '{s}'"),
        });
    };

    let n: i64 = num_str.parse().map_err(|e| crate::Error::BadRequest {
        detail: format!("invalid number in interval: {e}"),
    })?;
    if n <= 0 {
        return Err(crate::Error::BadRequest {
            detail: "interval must be > 0".into(),
        });
    }

    match unit {
        "s" => Ok(n * 1_000),
        "m" => Ok(n * 60_000),
        "h" => Ok(n * 3_600_000),
        "d" => Ok(n * 86_400_000),
        "w" => Ok(n * 604_800_000),
        _ => Err(crate::Error::BadRequest {
            detail: format!("unknown unit '{unit}'"),
        }),
    }
}

/// FILL strategies for time_bucket gaps.
#[derive(Debug, Clone, PartialEq)]
pub enum FillStrategy {
    /// Leave gaps as NULL.
    Null,
    /// Fill with the previous non-null value.
    Prev,
    /// Linear interpolation between surrounding values.
    Linear,
    /// Fill with a constant value.
    Constant(f64),
}

/// Apply fill strategy to a series of (bucket_start, Option<f64>) values.
///
/// `buckets` must be sorted by bucket_start. Missing buckets are NOT inserted
/// — the caller is responsible for generating the full bucket sequence.
pub fn apply_fill(values: &mut [(i64, Option<f64>)], strategy: &FillStrategy) {
    match strategy {
        FillStrategy::Null => {} // Already null.
        FillStrategy::Prev => {
            let mut prev: Option<f64> = None;
            for (_, v) in values.iter_mut() {
                if v.is_some() {
                    prev = *v;
                } else {
                    *v = prev;
                }
            }
        }
        FillStrategy::Linear => {
            // Find gaps and interpolate.
            let len = values.len();
            let mut i = 0;
            while i < len {
                if values[i].1.is_none() {
                    // Find the start and end of the gap.
                    let gap_start = i;
                    while i < len && values[i].1.is_none() {
                        i += 1;
                    }
                    let gap_end = i; // exclusive

                    // Get surrounding values for interpolation.
                    let before = if gap_start > 0 {
                        values[gap_start - 1].1
                    } else {
                        None
                    };
                    let after = if gap_end < len {
                        values[gap_end].1
                    } else {
                        None
                    };

                    if let (Some(v0), Some(v1)) = (before, after) {
                        let total_steps = (gap_end - gap_start + 1) as f64;
                        for (j, item) in values[gap_start..gap_end].iter_mut().enumerate() {
                            let t = (j + 1) as f64 / total_steps;
                            item.1 = Some(v0 + (v1 - v0) * t);
                        }
                    }
                } else {
                    i += 1;
                }
            }
        }
        FillStrategy::Constant(c) => {
            for (_, v) in values.iter_mut() {
                if v.is_none() {
                    *v = Some(*c);
                }
            }
        }
    }
}

/// Generate a full bucket sequence for a time range.
///
/// Returns bucket start timestamps from `start_ms` to `end_ms` (inclusive of start).
pub fn generate_buckets(start_ms: i64, end_ms: i64, interval_ms: i64) -> Vec<i64> {
    if interval_ms <= 0 {
        return Vec::new();
    }
    let first = time_bucket(interval_ms, start_ms);
    let mut buckets = Vec::new();
    let mut ts = first;
    while ts <= end_ms {
        buckets.push(ts);
        ts += interval_ms;
    }
    buckets
}

/// Find the most recent row per key group (equivalent to QuestDB's LATEST ON).
///
/// `data` is a slice of (timestamp, key, value) sorted by timestamp.
/// Returns one (timestamp, key, value) per unique key — the latest one.
pub fn latest_by_key<K: Eq + std::hash::Hash + Clone, V: Clone>(
    data: &[(i64, K, V)],
) -> Vec<(i64, K, V)> {
    use std::collections::HashMap;
    let mut latest: HashMap<K, (i64, K, V)> = HashMap::new();
    for (ts, key, val) in data {
        let entry = latest
            .entry(key.clone())
            .or_insert_with(|| (*ts, key.clone(), val.clone()));
        if *ts > entry.0 {
            *entry = (*ts, key.clone(), val.clone());
        }
    }
    let mut result: Vec<_> = latest.into_values().collect();
    result.sort_by_key(|(ts, _, _)| *ts);
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn time_bucket_hour() {
        // 2024-01-01 01:30:00 UTC → 2024-01-01 01:00:00 UTC
        let ts = 1_704_070_800_000i64; // 2024-01-01 01:00:00
        let ts_mid = ts + 30 * 60_000; // +30min
        assert_eq!(time_bucket(3_600_000, ts_mid), ts);
    }

    #[test]
    fn time_bucket_5min() {
        let base = 1_704_067_200_000i64; // 2024-01-01 00:00:00
        assert_eq!(time_bucket(300_000, base + 120_000), base); // +2min → floor to 0:00
        assert_eq!(time_bucket(300_000, base + 360_000), base + 300_000); // +6min → floor to 0:05
    }

    #[test]
    fn time_bucket_day() {
        let day_ms = 86_400_000i64;
        let ts = day_ms * 5 + 43_200_000; // day 5 at noon
        assert_eq!(time_bucket(day_ms, ts), day_ms * 5);
    }

    #[test]
    fn parse_interval() {
        assert_eq!(parse_interval_ms("5m").unwrap(), 300_000);
        assert_eq!(parse_interval_ms("1h").unwrap(), 3_600_000);
        assert_eq!(parse_interval_ms("1d").unwrap(), 86_400_000);
        assert_eq!(parse_interval_ms("2w").unwrap(), 2 * 604_800_000);
        assert!(parse_interval_ms("0m").is_err());
        assert!(parse_interval_ms("").is_err());
    }

    #[test]
    fn generate_buckets_basic() {
        let buckets = generate_buckets(0, 300_000, 60_000);
        assert_eq!(buckets, vec![0, 60_000, 120_000, 180_000, 240_000, 300_000]);
    }

    #[test]
    fn fill_prev() {
        let mut data = vec![
            (0, Some(1.0)),
            (1, None),
            (2, None),
            (3, Some(4.0)),
            (4, None),
        ];
        apply_fill(&mut data, &FillStrategy::Prev);
        assert_eq!(data[1].1, Some(1.0));
        assert_eq!(data[2].1, Some(1.0));
        assert_eq!(data[4].1, Some(4.0));
    }

    #[test]
    fn fill_constant() {
        let mut data = vec![(0, Some(1.0)), (1, None), (2, Some(3.0))];
        apply_fill(&mut data, &FillStrategy::Constant(0.0));
        assert_eq!(data[1].1, Some(0.0));
    }

    #[test]
    fn fill_linear() {
        let mut data = vec![(0, Some(10.0)), (1, None), (2, None), (3, Some(40.0))];
        apply_fill(&mut data, &FillStrategy::Linear);
        // Linear interpolation: 10 → 40 over 4 steps.
        assert!(data[1].1.is_some());
        assert!(data[2].1.is_some());
        // Values should be between 10 and 40.
        let v1 = data[1].1.unwrap();
        let v2 = data[2].1.unwrap();
        assert!(v1 > 10.0 && v1 < 40.0);
        assert!(v2 > v1 && v2 < 40.0);
    }

    #[test]
    fn latest_by_key_basic() {
        let data = vec![
            (100, "host-a", 1.0),
            (200, "host-b", 2.0),
            (300, "host-a", 3.0),
            (150, "host-b", 2.5),
        ];
        let latest = latest_by_key(&data);
        assert_eq!(latest.len(), 2);
        // host-a latest at ts=300, host-b latest at ts=200.
        let a = latest.iter().find(|(_, k, _)| *k == "host-a").unwrap();
        assert_eq!(a.0, 300);
        assert!((a.2 - 3.0f64).abs() < f64::EPSILON);
        let b = latest.iter().find(|(_, k, _)| *k == "host-b").unwrap();
        assert_eq!(b.0, 200);
    }
}
