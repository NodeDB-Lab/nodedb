//! Parallel partition scan for multi-partition timeseries queries.
//!
//! On Origin: naturally parallel via per-core partitions (TPC model).
//! On Lite-C native: uses thread pool (std::thread) for parallel I/O.
//! On WASM: sequential fallback (single-threaded).
//!
//! This module provides a parallel scan abstraction that works across
//! all targets without requiring rayon (to keep the dependency tree small
//! and WASM-compatible).

use std::path::{Path, PathBuf};

use super::vectorized_scan::{ScanError, ScanRequest, ScanResult};

/// Scan multiple partitions in parallel (on native) or sequentially (on WASM).
///
/// Returns merged results. Automatically chooses parallelism based on
/// partition count and available cores.
pub fn parallel_scan(
    partition_dirs: &[PathBuf],
    request: &ScanRequest,
) -> Result<ScanResult, ScanError> {
    if partition_dirs.is_empty() {
        return Ok(ScanResult::Aggregate(
            super::columnar_agg::AggResult::default(),
        ));
    }

    if partition_dirs.len() == 1 {
        return super::vectorized_scan::scan_partition(&partition_dirs[0], request);
    }

    // On native with multiple partitions: use thread::scope for parallel I/O.
    #[cfg(not(target_arch = "wasm32"))]
    {
        parallel_scan_native(partition_dirs, request)
    }

    // On WASM: sequential scan (single-threaded).
    #[cfg(target_arch = "wasm32")]
    {
        let dir_refs: Vec<&Path> = partition_dirs.iter().map(|d| d.as_path()).collect();
        super::vectorized_scan::scan_partitions(&dir_refs, request)
    }
}

/// Native parallel scan using std::thread::scope (no rayon dependency).
#[cfg(not(target_arch = "wasm32"))]
fn parallel_scan_native(
    partition_dirs: &[PathBuf],
    request: &ScanRequest,
) -> Result<ScanResult, ScanError> {
    let available_threads = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);

    // Don't spawn more threads than partitions or available cores.
    let thread_count = available_threads.min(partition_dirs.len()).min(8);

    if thread_count <= 1 {
        let dir_refs: Vec<&Path> = partition_dirs.iter().map(|d| d.as_path()).collect();
        return super::vectorized_scan::scan_partitions(&dir_refs, request);
    }

    // Partition work across threads.
    let chunk_size = partition_dirs.len().div_ceil(thread_count);
    let chunks: Vec<&[PathBuf]> = partition_dirs.chunks(chunk_size).collect();

    let results: Vec<Result<ScanResult, ScanError>> = std::thread::scope(|s| {
        let handles: Vec<_> = chunks
            .iter()
            .map(|chunk| {
                let req = request.clone();
                s.spawn(move || {
                    let dir_refs: Vec<&Path> = chunk.iter().map(|d| d.as_path()).collect();
                    super::vectorized_scan::scan_partitions(&dir_refs, &req)
                })
            })
            .collect();

        handles
            .into_iter()
            .map(|h| {
                h.join()
                    .unwrap_or(Err(ScanError::Io("thread panicked".into())))
            })
            .collect()
    });

    // Merge all thread results.
    merge_scan_results(results, request)
}

/// Merge multiple ScanResults into one.
fn merge_scan_results(
    results: Vec<Result<ScanResult, ScanError>>,
    request: &ScanRequest,
) -> Result<ScanResult, ScanError> {
    match request.bucket_interval_ms {
        0 if request.limit > 0 => {
            let mut all_raw = Vec::new();
            for r in results {
                if let Ok(ScanResult::Raw(rows)) = r {
                    all_raw.extend(rows);
                }
            }
            all_raw.sort_by_key(|&(ts, _)| ts);
            all_raw.truncate(request.limit);
            Ok(ScanResult::Raw(all_raw))
        }
        0 => {
            let mut merged = super::columnar_agg::AggResult {
                count: 0,
                sum: 0.0,
                min: f64::INFINITY,
                max: f64::NEG_INFINITY,
                first: f64::NAN,
                last: f64::NAN,
            };
            for r in results {
                if let Ok(ScanResult::Aggregate(agg)) = r {
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
                    if merged.first.is_nan() {
                        merged.first = agg.first;
                    }
                    merged.last = agg.last;
                }
            }
            Ok(ScanResult::Aggregate(merged))
        }
        _ => {
            let mut bucket_map: std::collections::BTreeMap<i64, super::columnar_agg::AggResult> =
                std::collections::BTreeMap::new();

            for r in results {
                if let Ok(ScanResult::Bucketed(buckets)) = r {
                    for (bucket_ts, agg) in buckets {
                        let entry =
                            bucket_map
                                .entry(bucket_ts)
                                .or_insert(super::columnar_agg::AggResult {
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

    fn create_partition(dir: &Path, name: &str, count: usize, start_ts: i64) {
        let mut mt = ColumnarMemtable::new_metric(test_config());
        for i in 0..count {
            mt.ingest_metric(
                1,
                MetricSample {
                    timestamp_ms: start_ts + i as i64 * 1000,
                    value: (i % 100) as f64,
                },
            );
        }
        let drain = mt.drain();
        ColumnarSegmentWriter::new(dir)
            .write_partition(name, &drain, 0, 0)
            .unwrap();
    }

    #[test]
    fn parallel_scan_single() {
        let tmp = TempDir::new().unwrap();
        create_partition(tmp.path(), "p1", 1000, 1_700_000_000_000);

        let dirs = vec![tmp.path().join("p1")];
        let request = ScanRequest {
            start_ms: 1_700_000_000_000,
            end_ms: 1_700_000_999_000,
            value_column: "value".into(),
            bucket_interval_ms: 0,
            predicates: vec![],
            limit: 0,
        };

        let result = parallel_scan(&dirs, &request).unwrap();
        match result {
            ScanResult::Aggregate(agg) => assert_eq!(agg.count, 1000),
            _ => panic!("expected Aggregate"),
        }
    }

    #[test]
    fn parallel_scan_multiple() {
        let tmp = TempDir::new().unwrap();
        create_partition(tmp.path(), "p1", 500, 1_700_000_000_000);
        create_partition(tmp.path(), "p2", 500, 1_700_000_500_000);
        create_partition(tmp.path(), "p3", 500, 1_700_001_000_000);

        let dirs = vec![
            tmp.path().join("p1"),
            tmp.path().join("p2"),
            tmp.path().join("p3"),
        ];
        let request = ScanRequest {
            start_ms: 1_700_000_000_000,
            end_ms: 1_700_001_500_000,
            value_column: "value".into(),
            bucket_interval_ms: 0,
            predicates: vec![],
            limit: 0,
        };

        let result = parallel_scan(&dirs, &request).unwrap();
        match result {
            ScanResult::Aggregate(agg) => assert_eq!(agg.count, 1500),
            _ => panic!("expected Aggregate"),
        }
    }
}
