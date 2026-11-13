// SPDX-License-Identifier: BUSL-1.1

//! Shared test fixture for this module's write and load paths.
//!
//! A real [`CoreLoop`] rooted at a temp dir, so a checkpoint can be published
//! through the production writer and read back through the production loader —
//! the only way a resurrection or a path divergence between the two shows up.

use std::sync::Arc;

use nodedb_bridge::buffer::RingBuffer;
use nodedb_types::OrdinalClock;

use crate::bridge::dispatch::{BridgeRequest, BridgeResponse};
use crate::data::executor::core_loop::CoreLoop;

/// Open a core rooted at `dir`. Two cores in one test share a data dir the way
/// a restart does: the second reads exactly what the first wrote, so the first
/// must be dropped before the second opens (a core owns its redb exclusively).
pub(super) fn open_core_at(dir: &std::path::Path) -> CoreLoop {
    let hlc = Arc::new(OrdinalClock::new());
    let (req_tx, req_rx) = RingBuffer::channel::<BridgeRequest>(64);
    let (resp_tx, _resp_rx) = RingBuffer::channel::<BridgeResponse>(64);
    drop(req_tx); // no requests are dispatched in these tests
    CoreLoop::open(
        crate::data::executor::core_loop::CoreOpenParams::unencrypted(
            0,
            req_rx,
            resp_tx,
            dir,
            hlc,
            crate::data::executor::core_loop::test_governor(),
        ),
    )
    .expect("CoreLoop::open")
}
