// SPDX-License-Identifier: BUSL-1.1

//! Capture sites on the data-group apply path.

use std::sync::atomic::{AtomicU64, Ordering};

use faultbox::{Capture, EventKind};

use crate::diag::context;

/// Count of committed Raft entries the apply loop applied a second time.
static RAFT_ENTRIES_REAPPLIED: AtomicU64 = AtomicU64::new(0);

/// Count of replicated writes parked behind a staged Calvin transaction.
static REPLICATED_WRITES_PARKED: AtomicU64 = AtomicU64::new(0);

/// Read the count of committed Raft entries applied twice. Exposed for the
/// metrics exporter and tests.
pub fn raft_entries_reapplied() -> u64 {
    RAFT_ENTRIES_REAPPLIED.load(Ordering::Relaxed)
}

/// Read the count of replicated writes parked behind a staged Calvin
/// transaction. Exposed for the metrics exporter and tests.
pub fn replicated_writes_parked() -> u64 {
    REPLICATED_WRITES_PARKED.load(Ordering::Relaxed)
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

/// Report a replicated write a core parked behind a staged Calvin
/// transaction. Called from the core's Calvin fence when it parks a write
/// whose order Raft already fixed.
pub fn replicated_write_parked(core_id: usize, collection: &str, owner: (u64, u32, u32)) {
    REPLICATED_WRITES_PARKED.fetch_add(1, Ordering::Relaxed);
    let ctx = context::ReplicatedWriteParked {
        core_id,
        collection: collection.to_owned(),
        owner_epoch: owner.0,
        owner_position: owner.1,
        owner_vshard: owner.2,
    };
    let _ = Capture::new(
        EventKind::Error,
        "replicated write parked behind a staged Calvin transaction",
    )
    .domain(&ctx)
    .emit();
}
