// SPDX-License-Identifier: BUSL-1.1

//! The dispatcher's hold on the outcome floor for every accepted request that
//! carries a WAL LSN.
//!
//! A request holds its window from the moment the dispatcher accepts it until
//! the core's final response arrives. A core that died, or a drain that gave
//! up on a core, also settles the window: that core never publishes a
//! watermark again.

use std::collections::HashMap;
use std::sync::Arc;

use crate::types::Lsn;

use super::outcome_floor::{OutcomeFloor, WriteWindow};

/// Open windows of dispatched requests, by request id.
#[derive(Debug, Default)]
pub(super) struct DispatchedLsns {
    windows: HashMap<u64, WriteWindow>,
}

impl DispatchedLsns {
    /// Hold the floor below `lsn` until request `request_id` is answered.
    pub(super) fn track(&mut self, floor: &Arc<OutcomeFloor>, request_id: u64, lsn: Lsn) {
        self.windows.insert(request_id, floor.open_dispatched(lsn));
    }

    /// Release the hold of request `request_id`, if it has one.
    pub(super) fn settle(&mut self, request_id: u64) {
        if let Some(window) = self.windows.remove(&request_id) {
            window.settle();
        }
    }

    /// Number of requests holding the floor.
    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.windows.len()
    }
}
