// SPDX-License-Identifier: BUSL-1.1

//! Capture site for a version cleanup pass that did not finish.

use faultbox::{Capture, EventKind, error_chain_of};

use super::shared::error_class;
use crate::diag::context;

/// Report a cleanup pass step that failed. Called from the cleanup task,
/// which retries at the next pass.
pub fn version_cleanup_failed(err: &crate::Error, step: &str, horizon: i64) {
    let class = error_class(err);
    let ctx = context::VersionCleanupFailed {
        step,
        horizon,
        error_class: &class,
    };
    let _ = Capture::new(
        EventKind::Error,
        "version cleanup: a pass did not finish, so superseded versions stay below the horizon",
    )
    .error_chain(error_chain_of(err))
    .domain(&ctx)
    .with_backtrace()
    .emit();
}

/// Report a document history recharge that failed at `site`. The caller
/// keeps the prior charge and continues: the write or purge already landed.
pub fn history_charge_stale(err: &crate::Error, site: &str) {
    let class = error_class(err);
    let ctx = context::HistoryChargeStale {
        site,
        error_class: &class,
    };
    let _ = Capture::new(
        EventKind::Error,
        "document history charge: a recharge failed, so the governor holds a stale charge",
    )
    .error_chain(error_chain_of(err))
    .domain(&ctx)
    .with_backtrace()
    .emit();
}
