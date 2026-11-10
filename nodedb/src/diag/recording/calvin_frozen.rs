// SPDX-License-Identifier: BUSL-1.1

//! Capture site of the stored frozen Calvin `RETURNING` replies.

use faultbox::{Capture, EventKind, error_chain_of};

use super::shared::error_class;

use crate::diag::context;

/// Report a catalog error on the stored frozen replies. Called from the
/// sites that read and write the rows. `op` names the operation: `store`,
/// `load`, `remove` or `forget`.
pub fn calvin_frozen_reply_store_failed(
    vshard_id: u32,
    txn: Option<(u64, u32)>,
    op: &'static str,
    err: &crate::Error,
) {
    let class = error_class(err);
    let ctx = context::CalvinFrozenReplyStoreFailed {
        vshard_id,
        txn,
        op,
        error_class: &class,
    };
    let _ = Capture::new(
        EventKind::Error,
        "calvin frozen RETURNING reply store failed",
    )
    .error_chain(error_chain_of(err))
    .domain(&ctx)
    .emit();
}
