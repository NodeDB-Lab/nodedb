// SPDX-License-Identifier: BUSL-1.1

//! Capture sites on the data-group apply path.

use std::sync::atomic::{AtomicU64, Ordering};

use faultbox::{Capture, EventKind};

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
