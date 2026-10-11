// SPDX-License-Identifier: BUSL-1.1

//! The scheduler's view of its vShard's installed base.
//!
//! A data-group snapshot install replaces the vShard's base and its applied
//! ledger. A scheduler started under an older base proposes nothing more:
//! the next scheduler reconcile starts it again from the installed state.
//!
//! The data group's apply gate orders every install against snapshot
//! captures and installs: a committed slice installs from the log, in the
//! apply loop, under that gate.

use super::scheduler::Scheduler;
use crate::control::security::auth_fence::cluster::group_of_vshard;
use crate::control::state::SharedState;

/// The base generation a scheduler started under.
pub(in crate::control::cluster::calvin::scheduler::driver::core) struct InstallGate {
    generation: u64,
}

impl InstallGate {
    /// The gate of a scheduler starting for `vshard_id` now.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn new(
        shared: &SharedState,
        vshard_id: u32,
    ) -> Self {
        Self {
            generation: shared.calvin.bases.generation(vshard_id),
        }
    }

    /// Whether a snapshot install replaced `vshard_id`'s base since this
    /// scheduler started.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn is_retired(
        &self,
        shared: &SharedState,
        vshard_id: u32,
    ) -> bool {
        shared.calvin.bases.generation(vshard_id) != self.generation
    }
}

impl Scheduler {
    /// Record that this replica's Calvin state of the vShard has a hole, and
    /// make its data-group replica refuse log entries until a snapshot
    /// brings the state back.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn lose_calvin_base(&self) {
        if let Err(error) = self
            .shared
            .calvin
            .bases
            .record_lost(self.shared.credentials.catalog(), self.vshard_id)
        {
            tracing::error!(
                vshard_id = self.vshard_id,
                %error,
                "calvin: the vShard's lost base did not persist"
            );
        }
        match group_of_vshard(&self.shared, self.vshard_id) {
            Ok(group_id) => {
                self.multi_raft
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .set_snapshot_required(group_id, true);
            }
            Err(error) => tracing::error!(
                vshard_id = self.vshard_id,
                %error,
                "calvin: no data group for the vShard; its replica cannot ask for a snapshot"
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::bridge::dispatch::Dispatcher;
    use crate::wal::WalManager;

    /// A snapshot install that replaced the vShard's base retires the
    /// scheduler started before it.
    #[test]
    fn an_installed_base_retires_the_older_scheduler() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal =
            Arc::new(WalManager::open_for_testing(&dir.path().join("gate.wal")).expect("wal"));
        let (dispatcher, _sides) = Dispatcher::new(1, 64);
        let state = SharedState::new(dispatcher, wal).expect("shared state");
        let vshard_id = 0;
        let gate = InstallGate::new(&state, vshard_id);
        assert!(!gate.is_retired(&state, vshard_id));

        state
            .calvin
            .bases
            .record_snapshot(state.credentials.catalog(), &[vshard_id], 9)
            .expect("record the installed base");
        assert!(gate.is_retired(&state, vshard_id));
    }
}
