//! Vectorized scan pipeline for common timeseries queries.
//!
//! Purpose-built fast path for:
//! ```sql
//! SELECT time_bucket('1h', ts), avg(cpu)
//! FROM metrics
//! WHERE ts BETWEEN t1 AND t2
//! GROUP BY 1
//! ```
//!
//! Pipeline:
//! 1. Sparse index → identify relevant blocks
//! 2. Decode only needed columns (projection pushdown)
//! 3. SIMD filter (timestamp range + predicate)
//! 4. SIMD aggregate (sum/min/max/count in one pass)
//!
//! Bypasses DataFusion for the bread-and-butter timeseries query pattern.
//! DataFusion remains the general path for joins, subqueries, etc.

use std::path::Path;

use super::columnar_agg::{AggResult, aggregate_by_time_bucket, aggregate_f64_filtered};
use super::columnar_memtable::ColumnType;
use super::columnar_segment::ColumnarSegmentReader;
use super::sparse_index::BlockPredicate;

/// A scan request for the vectorized pipeline.
#[derive(Debug, Clone)]
pub struct ScanRequest {
    /// Time range filter (inclusive).
    pub start_ms: i64,
    pub end_ms: i64,
    /// Column to aggregate (e.g., "cpu", "value").
    pub value_column: String,
    /// Time bucket interval (0 = no bucketing, return raw aggregate).
    pub bucket_interval_ms: i64,
    /// Optional predicates for block-level pushdown.
    pub predicates: Vec<BlockPredicate>,
    /// Maximum rows to return (0 = unlimited).
    pub limit: usize,
}

/// Result of a vectorized scan.
#[derive(Debug)]
pub enum ScanResult {
    /// Aggregated by time bucket.
    Bucketed(Vec<(i64, AggResult)>),
    /// Single aggregate over the entire range.
    Aggregate(AggResult),
    /// Raw (timestamp, value) pairs.
    Raw(Vec<(i64, f64)>),
}

/// Execute a vectorized scan over a single partition directory.
///
/// 1. Load sparse index (if available) for block-level skip
/// 2. Apply time range + predicate filters at block level
/// 3. Read only the timestamp and value columns
/// 4. Apply row-level time range filter
/// 5. Aggregate or return raw data
pub fn scan_partition(
    partition_dir: &Path,
    request: &ScanRequest,
) -> Result<ScanResult, ScanError> {
    // Step 1: Load sparse index for block-level skip.
    let sparse_idx = ColumnarSegmentReader::read_sparse_index(partition_dir)
        .map_err(|e| ScanError::Io(format!("sparse index: {e}")))?;

    // Step 2: Determine which rows to read based on sparse index.
    let row_ranges = if let Some(ref idx) = sparse_idx {
        let matching_blocks =
            idx.filter_blocks(request.start_ms, request.end_ms, &request.predicates);

        if matching_blocks.is_empty() {
            return Ok(if request.bucket_interval_ms > 0 {
                ScanResult::Bucketed(Vec::new())
            } else {
                ScanResult::Aggregate(AggResult::default())
            });
        }

        // Collect row ranges from matching blocks.
        let ranges: Vec<(usize, usize)> = matching_blocks
            .iter()
            .map(|&bi| idx.block_row_range(bi))
            .collect();
        Some(ranges)
    } else {
        None // No sparse index → scan all rows.
    };

    // Step 3: Read timestamp column.
    let ts_data =
        ColumnarSegmentReader::read_column(partition_dir, "timestamp", ColumnType::Timestamp)
            .map_err(|e| ScanError::Io(format!("read timestamp: {e}")))?;
    let timestamps = ts_data.as_timestamps();

    // Step 4: Read value column.
    let val_data = ColumnarSegmentReader::read_column(
        partition_dir,
        &request.value_column,
        ColumnType::Float64,
    )
    .map_err(|e| ScanError::Io(format!("read {}: {e}", request.value_column)))?;
    let values = val_data.as_f64();

    // Step 5: Build selection vector from time range filter.
    let indices: Vec<u32> = if let Some(ref ranges) = row_ranges {
        // Only check rows in matching blocks.
        let mut idx_vec = Vec::new();
        for &(start, end) in ranges {
            for (row, &ts) in timestamps[start..end.min(timestamps.len())]
                .iter()
                .enumerate()
            {
                if ts >= request.start_ms && ts <= request.end_ms {
                    idx_vec.push((start + row) as u32);
                }
            }
        }
        idx_vec
    } else {
        // No sparse index — linear scan.
        super::columnar_agg::timestamp_range_filter(timestamps, request.start_ms, request.end_ms)
    };

    if indices.is_empty() {
        return Ok(if request.bucket_interval_ms > 0 {
            ScanResult::Bucketed(Vec::new())
        } else {
            ScanResult::Aggregate(AggResult::default())
        });
    }

    // Step 6: Aggregate or return raw.
    if request.bucket_interval_ms > 0 {
        // Time-bucket aggregation.
        let filtered_ts: Vec<i64> = indices.iter().map(|&i| timestamps[i as usize]).collect();
        let filtered_vals: Vec<f64> = indices.iter().map(|&i| values[i as usize]).collect();
        let buckets =
            aggregate_by_time_bucket(&filtered_ts, &filtered_vals, request.bucket_interval_ms);
        Ok(ScanResult::Bucketed(buckets))
    } else if request.limit > 0 {
        // Raw scan with limit.
        let raw: Vec<(i64, f64)> = indices
            .iter()
            .take(request.limit)
            .map(|&i| (timestamps[i as usize], values[i as usize]))
            .collect();
        Ok(ScanResult::Raw(raw))
    } else {
        // Full aggregate over filtered rows.
        let agg = aggregate_f64_filtered(values, &indices);
        Ok(ScanResult::Aggregate(agg))
    }
}

/// Scan multiple partition directories and merge results.
pub fn scan_partitions(
    partition_dirs: &[&Path],
    request: &ScanRequest,
) -> Result<ScanResult, ScanError> {
    if partition_dirs.is_empty() {
        return Ok(ScanResult::Aggregate(AggResult::default()));
    }

    if partition_dirs.len() == 1 {
        return scan_partition(partition_dirs[0], request);
    }

    // Scan each partition and merge.
    match request.bucket_interval_ms {
        0 if request.limit > 0 => {
            // Raw scan: collect from all partitions, sort by timestamp, apply limit.
            let mut all_raw = Vec::new();
            for &dir in partition_dirs {
                if let Ok(ScanResult::Raw(rows)) = scan_partition(dir, request) {
                    all_raw.extend(rows);
                }
            }
            all_raw.sort_by_key(|&(ts, _)| ts);
            all_raw.truncate(request.limit);
            Ok(ScanResult::Raw(all_raw))
        }
        0 => {
            // Merge aggregates across partitions.
            let mut merged = AggResult {
                count: 0,
                sum: 0.0,
                min: f64::INFINITY,
                max: f64::NEG_INFINITY,
                first: f64::NAN,
                last: f64::NAN,
            };
            let mut first_seen = false;

            for &dir in partition_dirs {
                if let Ok(ScanResult::Aggregate(agg)) = scan_partition(dir, request) {
                    if agg.count == 0 {
                        continue;
                    }
                    merged.count += agg.count;
                    merged.sum += agg.sum;
                    if agg.min < merged.min {
                        merged.min = agg.min;
                    }
                    if agg.max > merged.max {
                        merged.max = agg.max;
                    }
                    // Track first/last by partition order (already time-sorted).
                    if !first_seen {
                        merged.first = agg.first;
                        first_seen = true;
                    }
                    merged.last = agg.last;
                }
            }
            Ok(ScanResult::Aggregate(merged))
        }
        _ => {
            // Bucketed: merge bucket maps across partitions.
            let mut bucket_map: std::collections::BTreeMap<i64, AggResult> =
                std::collections::BTreeMap::new();

            for &dir in partition_dirs {
                if let Ok(ScanResult::Bucketed(buckets)) = scan_partition(dir, request) {
                    for (bucket_ts, agg) in buckets {
                        let entry = bucket_map.entry(bucket_ts).or_insert(AggResult {
                            count: 0,
                            sum: 0.0,
                            min: f64::INFINITY,
                            max: f64::NEG_INFINITY,
                            first: f64::NAN,
                            last: f64::NAN,
                        });
                        entry.count += agg.count;
                        entry.sum += agg.sum;
                        if agg.min < entry.min {
                            entry.min = agg.min;
                        }
                        if agg.max > entry.max {
                            entry.max = agg.max;
                        }
                        if entry.first.is_nan() {
                            entry.first = agg.first;
                        }
                        entry.last = agg.last;
                    }
                }
            }

            Ok(ScanResult::Bucketed(bucket_map.into_iter().collect()))
        }
    }
}

/// Error type for vectorized scan operations.
#[derive(thiserror::Error, Debug)]
pub enum ScanError {
    #[error("scan error: {0}")]
    Io(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::timeseries::columnar_memtable::{ColumnarMemtable, ColumnarMemtableConfig};
    use crate::engine::timeseries::columnar_segment::ColumnarSegmentWriter;
    use nodedb_types::timeseries::MetricSample;
    use tempfile::TempDir;

    fn test_config() -> ColumnarMemtableConfig {
        ColumnarMemtableConfig {
            max_memory_bytes: 10 * 1024 * 1024,
            hard_memory_limit: 20 * 1024 * 1024,
            max_tag_cardinality: 1000,
        }
    }

    fn create_test_partition(
        dir: &Path,
        name: &str,
        count: usize,
        start_ts: i64,
        interval_ms: i64,
    ) {
        let mut mt = ColumnarMemtable::new_metric(test_config());
        for i in 0..count {
            mt.ingest_metric(
                1,
                MetricSample {
                    timestamp_ms: start_ts + i as i64 * interval_ms,
                    value: (i % 100) as f64,
                },
            );
        }
        let drain = mt.drain();
        let writer = ColumnarSegmentWriter::new(dir);
        writer.write_partition(name, &drain, 0, 0).unwrap();
    }

    #[test]
    fn scan_single_partition_aggregate() {
        let tmp = TempDir::new().unwrap();
        create_test_partition(tmp.path(), "p1", 1000, 1_700_000_000_000, 1000);

        let request = ScanRequest {
            start_ms: 1_700_000_000_000,
            end_ms: 1_700_000_999_000,
            value_column: "value".into(),
            bucket_interval_ms: 0,
            predicates: vec![],
            limit: 0,
        };

        let result = scan_partition(&tmp.path().join("p1"), &request).unwrap();
        match result {
            ScanResult::Aggregate(agg) => {
                assert_eq!(agg.count, 1000);
                assert!(agg.min >= 0.0);
                assert!(agg.max <= 99.0);
            }
            _ => panic!("expected Aggregate"),
        }
    }

    #[test]
    fn scan_with_time_bucket() {
        let tmp = TempDir::new().unwrap();
        create_test_partition(tmp.path(), "p1", 3600, 1_700_000_000_000, 1000);

        let request = ScanRequest {
            start_ms: 1_700_000_000_000,
            end_ms: 1_700_003_599_000,
            value_column: "value".into(),
            bucket_interval_ms: 60_000, // 1-minute buckets
            predicates: vec![],
            limit: 0,
        };

        let result = scan_partition(&tmp.path().join("p1"), &request).unwrap();
        match result {
            ScanResult::Bucketed(buckets) => {
                // 3600 samples at 1s intervals → ~60 one-minute buckets.
                // Edge buckets may have slightly more/fewer samples.
                assert!(
                    (59..=61).contains(&buckets.len()),
                    "expected ~60 buckets, got {}",
                    buckets.len()
                );
                let total_count: u64 = buckets.iter().map(|(_, agg)| agg.count).sum();
                assert_eq!(total_count, 3600);
            }
            _ => panic!("expected Bucketed"),
        }
    }

    #[test]
    fn scan_with_time_range_filter() {
        let tmp = TempDir::new().unwrap();
        create_test_partition(tmp.path(), "p1", 10_000, 1_700_000_000_000, 1000);

        // Query middle 5000 rows: timestamps 2000*1000 through 6999*1000.
        // At 1000ms intervals, 5000 timestamps fall in this range.
        let start = 1_700_000_000_000 + 2000 * 1000;
        let end = 1_700_000_000_000 + 6999 * 1000;
        let request = ScanRequest {
            start_ms: start,
            end_ms: end,
            value_column: "value".into(),
            bucket_interval_ms: 0,
            predicates: vec![],
            limit: 0,
        };

        let result = scan_partition(&tmp.path().join("p1"), &request).unwrap();
        match result {
            ScanResult::Aggregate(agg) => {
                assert_eq!(agg.count, 5000);
            }
            _ => panic!("expected Aggregate"),
        }
    }

    #[test]
    fn scan_raw_with_limit() {
        let tmp = TempDir::new().unwrap();
        create_test_partition(tmp.path(), "p1", 1000, 1_700_000_000_000, 1000);

        let request = ScanRequest {
            start_ms: 1_700_000_000_000,
            end_ms: 1_700_000_999_000,
            value_column: "value".into(),
            bucket_interval_ms: 0,
            predicates: vec![],
            limit: 10,
        };

        let result = scan_partition(&tmp.path().join("p1"), &request).unwrap();
        match result {
            ScanResult::Raw(rows) => {
                assert_eq!(rows.len(), 10);
                // Should be sorted by timestamp.
                for i in 1..rows.len() {
                    assert!(rows[i].0 >= rows[i - 1].0);
                }
            }
            _ => panic!("expected Raw"),
        }
    }

    #[test]
    fn scan_empty_range() {
        let tmp = TempDir::new().unwrap();
        create_test_partition(tmp.path(), "p1", 100, 1_700_000_000_000, 1000);

        let request = ScanRequest {
            start_ms: 9_000_000_000_000, // way in the future
            end_ms: 9_000_001_000_000,
            value_column: "value".into(),
            bucket_interval_ms: 0,
            predicates: vec![],
            limit: 0,
        };

        let result = scan_partition(&tmp.path().join("p1"), &request).unwrap();
        match result {
            ScanResult::Aggregate(agg) => assert_eq!(agg.count, 0),
            _ => panic!("expected empty Aggregate"),
        }
    }

    #[test]
    fn scan_multiple_partitions() {
        let tmp = TempDir::new().unwrap();
        create_test_partition(tmp.path(), "p1", 1000, 1_700_000_000_000, 1000);
        create_test_partition(tmp.path(), "p2", 1000, 1_700_001_000_000, 1000);

        let dirs = [tmp.path().join("p1"), tmp.path().join("p2")];
        let dir_refs: Vec<&Path> = dirs.iter().map(|d| d.as_path()).collect();

        let request = ScanRequest {
            start_ms: 1_700_000_000_000,
            end_ms: 1_700_001_999_000,
            value_column: "value".into(),
            bucket_interval_ms: 0,
            predicates: vec![],
            limit: 0,
        };

        let result = scan_partitions(&dir_refs, &request).unwrap();
        match result {
            ScanResult::Aggregate(agg) => {
                assert_eq!(agg.count, 2000);
            }
            _ => panic!("expected Aggregate"),
        }
    }
}
