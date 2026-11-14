// SPDX-License-Identifier: BUSL-1.1

//! Capture sites for the Control-Plane change feed's journal.

use faultbox::{Capture, EventKind, error_chain_of};

use crate::diag::context;

/// Report that the change journal did not do `operation` for `group_id`'s
/// feed. Called from the one site that runs each operation: the stage of a
/// page, the settle of a spilled entry, the floor save, and the publish of
/// a forwarded run.
pub fn change_journal_failed(group_id: u64, operation: &'static str, error: &crate::Error) {
    let ctx = context::ChangeJournalFailed {
        group_id,
        operation,
        error: error.to_string(),
    };
    let _ = Capture::new(
        EventKind::Error,
        "the change journal did not take or return a group's change events",
    )
    .domain(&ctx)
    .error_chain(error_chain_of(error))
    .emit();
}

/// Report that follower `peer` rejected `group_id`'s change feed, so this
/// node stops feeding it the group. Called from the one site that reads the
/// rejection, the leader's feed of the follower.
pub fn change_run_rejected(peer: u64, group_id: u64, detail: &str) {
    let ctx = context::ChangeRunRejected {
        peer,
        group_id,
        detail: detail.to_owned(),
    };
    let _ = Capture::new(
        EventKind::InvariantViolation,
        "a follower rejected a group's change feed: the feed of the group stops",
    )
    .domain(&ctx)
    .emit();
}

/// Report that the change journal kept refusing `group_id`'s events, so the
/// group halts and the node reports not ready. Called from the one site
/// that decides the halt, once per halt.
pub fn change_journal_halted(group_id: u64, detail: &str) {
    let ctx = context::ChangeJournalHalted {
        group_id,
        detail: detail.to_owned(),
    };
    let _ = Capture::new(
        EventKind::InvariantViolation,
        "the change journal keeps refusing a group's events: the group halts",
    )
    .domain(&ctx)
    .emit();
}
