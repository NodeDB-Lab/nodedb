// SPDX-License-Identifier: BUSL-1.1

//! Disk-partition scanning for raw mode.
//!
//! Every partition reads on the core thread. A core owns one shard and one
//! jemalloc arena, and stalls its whole shard while it waits. So it spawns no
//! scan thread and never joins one.
//!
//! A task runs to completion on its core, so a long scan holds the core's
//! other work back. Deadline safe points bound that hold: one before each
//! partition, and one per row stride. An expired scan answers
//! `DeadlineExceeded` and carries no rows.

use nodedb_crs::CrsContext;
use nodedb_wal::crypto::WalEncryptionKey;

use crate::bridge::scan_filter::ScanFilter;
use crate::data::executor::deadline::DeadlineCheck;
use crate::data::executor::handlers::timeseries::partition_read::{
    PartitionIo, PartitionQuarantine, TsPartitionColumns, account_partition_read,
    partition_corrupt, read_ts_partition_on_core,
};
use crate::engine::timeseries::columnar_agg::timestamp_range_filter;
use crate::engine::timeseries::columnar_memtable::{ColumnData, ColumnType};
use crate::types::RequestId;

use super::row_emit::{emit_partition_row, extract_timestamp, row_admitted};

/// The read path the corruption report names.
const SITE: &str = "timeseries_raw_scan";

/// The read-only inputs every partition scan shares.
#[derive(Clone, Copy)]
pub(super) struct PartitionScan<'a> {
    /// Inclusive `(min_ms, max_ms)` bounds on the time key.
    pub time_range: (i64, i64),
    /// Row ceiling.
    pub limit: usize,
    /// WHERE predicates; applied only when `has_filters` is set.
    pub filter_predicates: &'a [ScanFilter],
    pub has_filters: bool,
    /// The caller's read policy: every row is checked against it before it
    /// counts toward `limit`.
    pub rls_predicates: &'a [ScanFilter],
    /// The core's timeseries segment key.
    pub kek: Option<&'a WalEncryptionKey>,
    /// Where a corrupt partition is charged. `None` when no registry is wired.
    pub quarantine: Option<&'a PartitionQuarantine>,
    /// The core's batched reader. `None` reads each file on its own.
    pub io: Option<PartitionIo<'a>>,
    /// The deadline of the request the scan answers. `None` runs to its end.
    pub deadline: Option<ScanDeadline<'a>>,
}

/// The deadline a scan checks, and the request its error names.
#[derive(Clone, Copy)]
pub(super) struct ScanDeadline<'a> {
    pub check: &'a DeadlineCheck,
    pub request_id: RequestId,
}

impl ScanDeadline<'_> {
    /// The error of an expired scan.
    pub(super) fn exceeded(&self) -> crate::Error {
        crate::Error::DeadlineExceeded {
            request_id: self.request_id,
        }
    }
}

/// Scan disk partitions on this core, in the time order the registry lists
/// them, up to `limit` rows. Rows of several partitions come back sorted by
/// timestamp: late rows make partitions overlap.
///
/// `Err` when a partition is corrupt, a stored time cell cannot be read as
/// its column's instant, a predicate expression divides by zero, or the
/// request's deadline passes.
pub(super) fn scan_partitions(
    partition_dirs: &[std::path::PathBuf],
    scan: &PartitionScan<'_>,
    crs: &CrsContext<'_>,
) -> crate::Result<Vec<rmpv::Value>> {
    let limit = scan.limit;
    let mut results = Vec::new();
    for dir in partition_dirs {
        if results.len() >= limit {
            break;
        }
        // Safe point: a partition boundary, before its files are read.
        if let Some(deadline) = scan.deadline
            && deadline.check.expired_now()
        {
            return Err(deadline.exceeded());
        }
        let remaining = PartitionScan {
            limit: limit - results.len(),
            ..*scan
        };
        let rows = account_partition_read(
            scan.quarantine,
            dir,
            scan_one_partition(dir, &remaining, crs),
        )?;
        results.extend(rows);
    }
    if partition_dirs.len() > 1 {
        results.sort_by_key(extract_timestamp);
    }
    Ok(results)
}

/// Scan a single disk partition, returning rmpv rows.
///
/// The WHERE predicates run on the typed columns when the evaluator can
/// lower them and per row otherwise. The read policy runs per row after
/// them, before the row counts toward `limit`. `Err` when a stored time
/// cell cannot be read as its column's instant or a predicate expression
/// divides by zero.
fn scan_one_partition(
    part_dir: &std::path::Path,
    scan: &PartitionScan<'_>,
    crs: &CrsContext<'_>,
) -> crate::Result<Vec<rmpv::Value>> {
    let PartitionScan {
        time_range,
        limit,
        filter_predicates,
        has_filters,
        rls_predicates,
        kek,
        quarantine: _,
        io,
        deadline,
    } = *scan;
    // A partition that does not read refuses the scan. Skipping it would
    // answer without the partition's rows.
    let TsPartitionColumns {
        schema,
        columns: col_data,
        sym_dicts,
    } = read_ts_partition_on_core(part_dir, SITE, kek, io)?;
    let all_col_names: Vec<String> = schema.columns.iter().map(|(n, _)| n.clone()).collect();

    let Some(ColumnData::Timestamp(timestamps)) =
        col_data.get(schema.timestamp_idx).and_then(|d| d.as_ref())
    else {
        return Err(partition_corrupt(
            part_dir,
            "column",
            SITE,
            format!(
                "time column index {} holds no time cells",
                schema.timestamp_idx
            ),
        ));
    };
    let indices = timestamp_range_filter(timestamps, time_range.0, time_range.1);

    let schema_vec: Vec<(String, ColumnType)> = schema.columns.clone();
    let part_src = crate::data::executor::handlers::columnar_filter::PartitionColumns {
        schema: &schema_vec,
        columns: &col_data,
        sym_dicts: &sym_dicts,
    };

    // `row_filters` carries the WHERE predicates only when the typed-column
    // evaluator could not lower them, so they still apply per row instead
    // of being dropped.
    let row_count = timestamps.len();
    let (filtered_indices, row_filters): (Vec<u32>, &[ScanFilter]) = if has_filters {
        if let Some(bitmask) =
            crate::data::executor::handlers::columnar_filter::eval_filters_bitmask(
                &part_src,
                filter_predicates,
                row_count,
            )
        {
            (nodedb_query::simd_filter::bitmask_to_indices(&bitmask), &[])
        } else {
            match crate::data::executor::handlers::columnar_filter::eval_filters_sparse(
                &part_src,
                filter_predicates,
                &indices,
            ) {
                Some(mask) => (
                    crate::data::executor::handlers::columnar_filter::apply_mask(&indices, &mask),
                    &[],
                ),
                None => (indices, filter_predicates),
            }
        }
    } else {
        (indices, &[])
    };

    let mut rows = Vec::with_capacity(filtered_indices.len().min(limit));
    for &idx in &filtered_indices {
        if rows.len() >= limit {
            break;
        }
        // Safe point: a row boundary. The check reads the clock once per stride.
        if let Some(deadline) = deadline
            && deadline.check.expired()
        {
            return Err(deadline.exceeded());
        }
        let row = emit_partition_row(part_dir, &schema_vec, &col_data, &sym_dicts, idx as usize)?;
        if !row_admitted(&row, row_filters, rls_predicates, crs)? {
            continue;
        }
        rows.push(row);
    }

    // Release page cache for this partition.
    crate::data::io::fadvise::release_partition_columns(part_dir, &all_col_names);

    Ok(rows)
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use super::*;
    use crate::engine::timeseries::columnar_segment::test_partition::write_metric_partition as write_partition;

    fn scan_of(limit: usize) -> PartitionScan<'static> {
        PartitionScan {
            time_range: (0, i64::MAX),
            limit,
            filter_predicates: &[],
            has_filters: false,
            rls_predicates: &[],
            kek: None,
            quarantine: None,
            io: None,
            deadline: None,
        }
    }

    /// Every `.rs` file below `dir`.
    fn rust_files(dir: &Path, out: &mut Vec<PathBuf>) {
        for entry in std::fs::read_dir(dir).expect("read dir").flatten() {
            let path = entry.path();
            if path.is_dir() {
                rust_files(&path, out);
            } else if path.extension().is_some_and(|ext| ext == "rs") {
                out.push(path);
            }
        }
    }

    /// A scoped thread joins before it returns, which stalls the core's whole
    /// shard, and its spawn panics if the OS refuses. So a timeseries scan
    /// path on a core must not use one. The check reads the sources.
    #[test]
    fn no_timeseries_scan_path_uses_scoped_threads() {
        let needle = concat!("thread", "::", "scope");
        let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let mut files = Vec::new();
        rust_files(&src.join("data/executor/handlers/timeseries"), &mut files);
        rust_files(&src.join("engine/timeseries"), &mut files);
        assert!(!files.is_empty());

        let offenders: Vec<&PathBuf> = files
            .iter()
            .filter(|path| {
                std::fs::read_to_string(path)
                    .expect("read source")
                    .contains(needle)
            })
            .collect();

        assert!(offenders.is_empty(), "{offenders:?}");
    }

    /// An expired request stops at the first partition boundary, before any
    /// file reads. The answer is an error and carries no rows.
    #[test]
    fn an_expired_deadline_stops_the_scan_at_a_partition_boundary() {
        let dir = tempfile::tempdir().expect("tempdir");
        let dirs = vec![dir.path().join("ts-never-read")];
        let mut task = crate::data::executor::core_loop::tests::make_default_task();
        task.request.deadline = std::time::Instant::now() - std::time::Duration::from_millis(1);
        let check = DeadlineCheck::for_task(&task);
        let scan = PartitionScan {
            deadline: Some(ScanDeadline {
                check: &check,
                request_id: task.request_id(),
            }),
            ..scan_of(10)
        };

        let result = scan_partitions(&dirs, &scan, &CrsContext::builtin_only());

        assert!(matches!(result, Err(crate::Error::DeadlineExceeded { .. })));
    }

    /// A live deadline leaves the scan alone, through the core's batched reader.
    #[test]
    fn a_live_deadline_scans_through_the_batched_reader() {
        use crate::data::io::IoMetrics;
        use crate::data::io::uring_reader::UringReader;

        let dir = tempfile::tempdir().expect("tempdir");
        let dirs = vec![
            write_partition(dir.path(), "ts-a", &[100]),
            write_partition(dir.path(), "ts-b", &[200]),
        ];
        let task = crate::data::executor::core_loop::tests::make_default_task();
        let check = DeadlineCheck::for_task(&task);
        let reader = std::cell::RefCell::new(UringReader::new());
        let metrics = IoMetrics::default();
        let scan = PartitionScan {
            deadline: Some(ScanDeadline {
                check: &check,
                request_id: task.request_id(),
            }),
            io: Some(PartitionIo {
                reader: &reader,
                priority: crate::bridge::envelope::Priority::Normal,
                metrics: &metrics,
            }),
            ..scan_of(10)
        };

        let rows = scan_partitions(&dirs, &scan, &CrsContext::builtin_only()).expect("scan");

        assert_eq!(rows.len(), 2);
    }

    /// Partitions that overlap in time merge in timestamp order, the row limit
    /// holds across them, and later partitions stay unread once it is met.
    #[test]
    fn partitions_merge_in_time_order_on_the_calling_thread() {
        let dir = tempfile::tempdir().expect("tempdir");
        let dirs = vec![
            write_partition(dir.path(), "ts-a", &[100, 300]),
            write_partition(dir.path(), "ts-b", &[200, 400]),
        ];
        let crs = CrsContext::builtin_only();

        let rows = scan_partitions(&dirs, &scan_of(10), &crs).expect("scan");
        let times: Vec<i64> = rows.iter().map(extract_timestamp).collect();
        assert_eq!(times, vec![100, 200, 300, 400]);

        let unread = dir.path().join("ts-missing");
        let limited = vec![dirs[0].clone(), unread];
        let rows = scan_partitions(&limited, &scan_of(2), &crs).expect("the limit ends the scan");
        assert_eq!(rows.len(), 2);
    }

    /// A corrupt partition refuses the whole scan with a typed error.
    #[test]
    fn a_corrupt_partition_refuses_the_scan() {
        let dir = tempfile::tempdir().expect("tempdir");
        let good = write_partition(dir.path(), "ts-a", &[100]);
        let full = write_partition(dir.path(), "ts-b", &[200, 201, 202]);
        let short = write_partition(dir.path(), "ts-c", &[300]);
        std::fs::copy(short.join("value.col"), full.join("value.col")).expect("swap column");
        let crs = CrsContext::builtin_only();

        let result = scan_partitions(&[good, full], &scan_of(10), &crs);

        assert!(matches!(result, Err(crate::Error::SegmentCorrupted { .. })));
    }
}
