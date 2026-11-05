// SPDX-License-Identifier: BUSL-1.1

//! Capture sites for audit entries that did not reach the system catalog.

use faultbox::{Capture, EventKind, error_chain_of};

use super::shared::error_class;
use crate::diag::context;

/// Report an audit entry whose WAL append failed, so a crash before its flush loses it.
/// Called from each best-effort record arm that logs the error and continues.
pub fn audit_wal_append_failed(err: &crate::Error, seq: u64) {
    let class = error_class(err);
    let ctx = context::AuditWalAppendFailed {
        seq,
        error_class: &class,
    };
    let _ = Capture::new(
        EventKind::Error,
        "audit record: the WAL append failed, so the entry is not crash-safe until it flushes",
    )
    .error_chain(error_chain_of(err))
    .domain(&ctx)
    .with_backtrace()
    .emit();
}

/// Report a security bus lag, so the audit rows of `dropped` events are lost.
/// Called from the bus consumer, the one reader of these buses.
pub fn security_bus_lagged(bus: &str, dropped: u64) {
    let ctx = context::SecurityBusLagged { bus, dropped };
    let _ = Capture::new(
        EventKind::InvariantViolation,
        "security bus: the consumer lagged, so the bus lost events before their audit rows",
    )
    .domain(&ctx)
    .emit();
}

/// Report a batch of security bus audit rows whose blocking task did not finish.
/// Called from the bus consumer that awaits that task.
pub fn security_bus_rows_not_recorded(rows: usize) {
    let ctx = context::SecurityBusRowsNotRecorded { rows };
    let _ = Capture::new(
        EventKind::InvariantViolation,
        "security bus: an audit batch task did not finish, so its rows are not recorded",
    )
    .domain(&ctx)
    .emit();
}

/// Report an audit flush whose catalog commit failed, so its entries stay buffered.
/// Called from the flush arm that keeps the entries and returns.
pub fn audit_flush_not_persisted(err: &crate::Error, entry_count: usize, through_seq: u64) {
    let class = error_class(err);
    let ctx = context::AuditFlushNotPersisted {
        entry_count,
        through_seq,
        error_class: &class,
    };
    let _ = Capture::new(
        EventKind::Error,
        "audit flush: the catalog commit failed, so the entries stay only in memory",
    )
    .error_chain(error_chain_of(err))
    .domain(&ctx)
    .with_backtrace()
    .emit();
}
