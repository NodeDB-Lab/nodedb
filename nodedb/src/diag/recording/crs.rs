// SPDX-License-Identifier: BUSL-1.1

//! Capture site for the user-defined CRS set install.

use faultbox::{Capture, EventKind, error_chain_of};

use super::shared::error_class;
use crate::diag::context;

/// Report a committed user-defined CRS set that did not reach every core of
/// this node. Called from the CRS post-apply and image reconcile, which
/// cannot propagate: `cause` names the change that was sent.
pub fn custom_crs_set_not_applied(err: &crate::Error, cause: &'static str, entries: usize) {
    let class = error_class(err);
    let ctx = context::CustomCrsSetNotApplied {
        cause,
        entries,
        error_class: &class,
    };
    let _ = Capture::new(
        EventKind::InvariantViolation,
        "user-defined CRS post-apply: the committed CRS set never reached every core of this node",
    )
    .error_chain(error_chain_of(err))
    .domain(&ctx)
    .with_backtrace()
    .emit();
}
