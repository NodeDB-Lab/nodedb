// SPDX-License-Identifier: BUSL-1.1

//! Capture sites of a backup cut's data-group barriers.

use faultbox::{Capture, EventKind};

use crate::diag::context;

/// Report that a cut's barrier driver stopped before `group_id` applied the
/// cut's barrier on this node. Called only from the driver, the one site
/// that sees the cut's ordering window close.
pub fn cut_barrier_not_placed(
    group_id: u64,
    hlc: u64,
    restore_point: u64,
    led_here: bool,
    lagging: Vec<u32>,
) {
    let ctx = context::CutBarrierNotPlaced {
        group_id,
        hlc,
        restore_point,
        led_here,
        lagging,
    };
    let _ = Capture::new(
        EventKind::Error,
        "backup cut barrier not placed in a data group",
    )
    .domain(&ctx)
    .emit();
}
