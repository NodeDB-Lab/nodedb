// SPDX-License-Identifier: BUSL-1.1

//! Capture sites for columnar segment reads.

use faultbox::{Capture, EventKind, error_chain_of};

use super::shared::error_class;
use crate::diag::context;

/// Report a flushed columnar segment that does not open or decode.
///
/// Called only from the shared flushed-segment readers in
/// `columnar_read/flushed_segment.rs`, which every segment read goes through.
/// The caller returns the error alongside this report.
pub fn columnar_segment_corrupt(
    err: &crate::Error,
    collection: &str,
    segment_id: u64,
    stage: &'static str,
    site: &'static str,
) {
    let class = error_class(err);
    let ctx = context::ColumnarSegmentCorrupt {
        collection,
        segment_id,
        stage,
        site,
        error_class: &class,
    };
    let _ = Capture::new(
        EventKind::Corruption,
        "flushed columnar segment does not decode, so the read is refused",
    )
    .error_chain(error_chain_of(err))
    .domain(&ctx)
    .with_backtrace()
    .emit();
}

/// Report an on-disk timeseries partition whose schema, column, or symbol
/// dictionary does not read.
///
/// Called only from `read_ts_partition`. The caller returns the error
/// alongside this report.
pub fn timeseries_partition_unreadable(
    err: &crate::Error,
    partition: &str,
    stage: &'static str,
    site: &'static str,
) {
    let class = error_class(err);
    let ctx = context::TimeseriesPartitionUnreadable {
        partition,
        stage,
        site,
        error_class: &class,
    };
    let _ = Capture::new(
        EventKind::Corruption,
        "timeseries partition does not read, so the scan is refused",
    )
    .error_chain(error_chain_of(err))
    .domain(&ctx)
    .with_backtrace()
    .emit();
}

/// Report a record the quarantine registry dropped at its bound. Called only
/// from the registry's eviction. An evicted quarantine reads again and
/// quarantines again after two more failures.
pub fn quarantine_record_evicted(segment: &str, engine: &'static str, was_quarantined: bool) {
    let ctx = context::QuarantineRecordEvicted {
        segment,
        engine,
        was_quarantined,
    };
    let _ = Capture::new(
        EventKind::InvariantViolation,
        "quarantine registry is full, so it forgot one segment's record",
    )
    .domain(&ctx)
    .emit();
}

/// Report a committed multi-partition timeseries flush whose install did not
/// finish. Called only from the install step of `flush_ts_collection` and of
/// its roll-forward, which keep the flush pending and retry it.
pub fn timeseries_flush_not_installed(
    err: &crate::Error,
    database_id: u64,
    tenant_id: u64,
    collection: &str,
) {
    let class = error_class(err);
    let ctx = context::TimeseriesFlushNotInstalled {
        database_id,
        tenant_id,
        collection,
        error_class: &class,
    };
    let _ = Capture::new(
        EventKind::Error,
        "committed timeseries flush did not install, so the collection waits for its roll-forward",
    )
    .error_chain(error_chain_of(err))
    .domain(&ctx)
    .with_backtrace()
    .emit();
}

/// Report a timeseries memtable the idle maintenance flush could not write.
/// Called only from the idle flush, which keeps the rows and retries later.
pub fn timeseries_idle_flush_failed(
    err: &crate::Error,
    database_id: u64,
    tenant_id: u64,
    collection: &str,
) {
    let class = error_class(err);
    let ctx = context::TimeseriesIdleFlushFailed {
        database_id,
        tenant_id,
        collection,
        error_class: &class,
    };
    let _ = Capture::new(
        EventKind::Error,
        "idle timeseries flush failed, so the memtable keeps its rows",
    )
    .error_chain(error_chain_of(err))
    .domain(&ctx)
    .with_backtrace()
    .emit();
}
