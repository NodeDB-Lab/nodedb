// SPDX-License-Identifier: BUSL-1.1

//! Capture sites for the boot judge of committed records with no outcome.

use faultbox::{Capture, EventKind};

use crate::diag::context;

/// Report a committed record with no outcome and no complete commit
/// boundary. Called from boot settle, the one site that finds it.
pub fn redo_boundary_missing(lsn: u64, vshard_id: u32) {
    let ctx = context::RedoBoundaryMissing { lsn, vshard_id };
    let _ = Capture::new(
        EventKind::Corruption,
        "committed record with no outcome has no commit boundary",
    )
    .domain(&ctx)
    .with_backtrace()
    .emit();
}

/// Report a committed record whose header names an unknown event source.
/// Called from boot settle, the one site that reads it.
pub fn redo_event_source_unknown(lsn: u64, code: u8) {
    let ctx = context::RedoEventSourceUnknown { lsn, code };
    let _ = Capture::new(
        EventKind::Corruption,
        "committed record names an unknown event source",
    )
    .domain(&ctx)
    .with_backtrace()
    .emit();
}
