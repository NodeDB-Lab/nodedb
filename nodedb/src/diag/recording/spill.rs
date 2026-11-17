// SPDX-License-Identifier: BUSL-1.1

//! Capture sites for spill files that cleanup could not remove.

use faultbox::{Capture, EventKind, error_chain_of};

use crate::diag::context;

/// Report a spill directory or file that cleanup could not remove. Called
/// only from the spill cleanup helpers, which also return the error.
pub fn spill_cleanup_failed(
    err: &std::io::Error,
    kind: &'static str,
    target: &'static str,
    path: &std::path::Path,
) {
    let path = path.display().to_string();
    let ctx = context::SpillCleanupFailed {
        kind,
        target,
        path: &path,
    };
    let _ = Capture::new(
        EventKind::Error,
        "spill cleanup: a spill directory or file was not removed",
    )
    .error_chain(error_chain_of(err))
    .domain(&ctx)
    .with_backtrace()
    .emit();
}
