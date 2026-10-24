// SPDX-License-Identifier: BUSL-1.1

//! Capture site for the departure purge of vShards this node left.

use faultbox::{Capture, EventKind, error_chain_of};

use crate::diag::context;

/// Report that `step` of the departure purge failed for `vshards`. The
/// vShards stay pending and the next pass retries them. Called from the
/// departure pass, the one site that runs each step.
pub fn vshard_departure_purge_failed(step: &'static str, vshards: &[u32], error: &crate::Error) {
    let ctx = context::VShardDeparturePurgeFailed {
        step,
        vshards: vshards.to_vec(),
        error: error.to_string(),
    };
    let _ = Capture::new(
        EventKind::Error,
        "departure purge step failed: the stale copy of vShards this node left stays until a \
         retry purges it",
    )
    .domain(&ctx)
    .error_chain(error_chain_of(error))
    .emit();
}
