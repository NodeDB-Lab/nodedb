// SPDX-License-Identifier: BUSL-1.1

//! The Calvin transaction state a core holds.

use std::collections::HashMap;

use super::commit_pending::PendingCommit;

/// Staged Calvin transactions this core holds.
pub(in crate::data::executor) struct CalvinCoreState {
    /// Staged Calvin transactions awaiting the global verdict, keyed by
    /// `(epoch, position, vshard)`. `CalvinExecuteStatic` validates a
    /// transaction, stages its plans into the synthetic overlay, and inserts
    /// its entry here without mutating base. The install of the slice's
    /// stamped redo entry consumes the entry, and `CalvinDrop` discards it.
    /// Nothing staged here is observable in the base engines until the
    /// install.
    ///
    /// The vShard is part of the key because vShards round-robin onto cores.
    /// Several vShard slices of one multi-participant transaction share the
    /// same `(epoch, position)` and can land on the same core. Each slice
    /// stages and installs on its own.
    pub(in crate::data::executor) commit_pending: HashMap<(u64, u32, u32), PendingCommit>,
}

impl CalvinCoreState {
    pub(in crate::data::executor) fn new() -> Self {
        Self {
            commit_pending: HashMap::new(),
        }
    }
}
