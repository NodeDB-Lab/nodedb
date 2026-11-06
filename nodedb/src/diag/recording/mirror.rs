// SPDX-License-Identifier: BUSL-1.1

//! Capture site for a mirror's in-memory state.

use faultbox::{Capture, EventKind, error_chain_of};

use super::shared::error_class;
use crate::diag::context;

/// Report a status step whose row did not read back in the post-apply.
pub fn mirror_status_unread(err: &crate::Error, database_id: u64) {
    let class = error_class(err);
    let ctx = context::MirrorStatusUnread {
        database_id,
        error_class: &class,
    };
    let _ = Capture::new(
        EventKind::Error,
        "mirror status: a step applied and its row did not read back",
    )
    .error_chain(error_chain_of(err))
    .domain(&ctx)
    .with_backtrace()
    .emit();
}
