// SPDX-License-Identifier: BUSL-1.1

//! The Calvin transaction state a core holds.

use std::collections::HashMap;

use super::calvin_fence::CalvinFence;
use super::commit_pending::PendingCommit;

/// Staged Calvin transactions, the writes that wait on them, and the leader
/// flag of the transaction executing now.
pub(in crate::data::executor) struct CalvinCoreState {
    /// Staged Calvin transactions awaiting the global verdict, keyed by
    /// `(epoch, position, vshard)`. `CalvinExecuteStatic` validates a
    /// transaction, stages its plans into the synthetic overlay, and inserts
    /// its entry here without mutating base. The verdict-driven `CalvinFlush`
    /// installs the transaction's redo record, and `CalvinDrop` discards the
    /// staged state. Nothing staged here is observable in the base engines
    /// until a flush.
    ///
    /// The vShard is part of the key because vShards round-robin onto cores.
    /// Several vShard slices of one multi-participant transaction share the
    /// same `(epoch, position)` and can land on the same core. Each slice
    /// stages and flushes on its own.
    pub(in crate::data::executor) commit_pending: HashMap<(u64, u32, u32), PendingCommit>,

    /// Whether this node leads the data group of the Calvin transaction
    /// executing now.
    ///
    /// `execute_calvin_execute_active` sets it from the scheduler-stamped,
    /// per-node `is_group_leader` around its OLLP verification, then restores
    /// the resting value `true`. A shard that runs a bulk DML directly has no
    /// replication followers, so it must run OLLP drift verification.
    ///
    /// OLLP determinism: the verification emits `OllpRetryRequired` only when
    /// this is `true`. Every replica stages the carried
    /// `ollp_predicted_surrogates` set verbatim, so all replicas write the same
    /// surrogate set whatever their local scans read.
    pub(in crate::data::executor) ollp_is_group_leader: bool,

    /// Writes that wait for the staged transaction owning their rows.
    pub(in crate::data::executor) fence: CalvinFence,
}

impl CalvinCoreState {
    pub(in crate::data::executor) fn new() -> Self {
        Self {
            commit_pending: HashMap::new(),
            ollp_is_group_leader: true,
            fence: CalvinFence::default(),
        }
    }
}
