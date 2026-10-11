// SPDX-License-Identifier: BUSL-1.1

//! Capture sites on the data-group apply path.

use std::sync::atomic::{AtomicU64, Ordering};

use faultbox::{Capture, EventKind, error_chain_of};

use super::shared::error_class;

use crate::diag::context;

/// Count of committed Raft entries the apply loop applied a second time.
static RAFT_ENTRIES_REAPPLIED: AtomicU64 = AtomicU64::new(0);

/// Read the count of committed Raft entries applied twice. Exposed for the
/// metrics exporter and tests.
pub fn raft_entries_reapplied() -> u64 {
    RAFT_ENTRIES_REAPPLIED.load(Ordering::Relaxed)
}

/// Report an apply of a Raft log index the apply loop already applied.
/// Called from the apply loop, the one site that sees every applied index.
pub fn raft_entry_reapplied(group_id: u64, log_index: u64, highest_applied: u64) {
    RAFT_ENTRIES_REAPPLIED.fetch_add(1, Ordering::Relaxed);
    let ctx = context::RaftEntryReapplied {
        group_id,
        log_index,
        highest_applied,
    };
    let _ = Capture::new(
        EventKind::InvariantViolation,
        "committed Raft entry applied a second time",
    )
    .domain(&ctx)
    .with_backtrace()
    .emit();
}

/// Report a catalog error on a vShard's stored dependent-read barrier log.
/// Called from the sites that read and write the rows. `op` names the operation: `save`, `load` or `remove`.
pub fn calvin_barrier_log_store_failed(
    vshard_id: u32,
    txn: Option<(u64, u32)>,
    op: &'static str,
    err: &crate::Error,
) {
    let class = error_class(err);
    let ctx = context::CalvinBarrierLogStoreFailed {
        vshard_id,
        txn,
        op,
        error_class: &class,
    };
    let _ = Capture::new(
        EventKind::Error,
        "calvin dependent-read barrier log store failed",
    )
    .error_chain(error_chain_of(err))
    .domain(&ctx)
    .emit();
}
