// SPDX-License-Identifier: BUSL-1.1

//! The per-vShard fence between a departure purge and a data-group install.
//!
//! A purge removes this node's data of every vShard outside the residency it
//! read: rows homed on a departed vShard, and edges with no endpoint home in
//! that residency. An install writes the data of its group's vShards. On a
//! vShard both touch, the install lands after the purge:
//!
//! - A purge starts only while no install of a vShard outside its residency
//!   runs. It holds the fence until every core answered.
//! - An install of a vShard outside the running purge's residency waits for
//!   the purge to end. An install of a resident vShard never waits: the
//!   purge keeps every record a resident vShard homes.
//!
//! The purge takes the fence under the `MultiRaft` lock, at the residency
//! read. A group installs here only once it mounts, so a group that mounts
//! after that read installs after the purge.
//!
//! The fence lives in memory only. A crash ends the purge and the install
//! together. Boot recovery completes an interrupted install before the first
//! departure pass runs, and that pass hosts the vShards of every mounted
//! group again, so it purges none of them.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::{Mutex, MutexGuard};

use tokio::sync::Notify;

/// The fence one node's purges and installs share.
#[derive(Debug, Default)]
pub struct DepartureFence {
    state: Mutex<FenceState>,
    /// Woken when a purge ends.
    purge_ended: Notify,
}

#[derive(Debug, Default)]
struct FenceState {
    /// The residency the running purge read. `None` while no purge runs.
    purge: Option<BTreeSet<u32>>,
    /// The installs running for each vShard.
    installing: HashMap<u32, usize>,
}

impl FenceState {
    /// Whether an install of `vshards` conflicts with the running purge.
    fn purge_blocks(&self, vshards: &HashSet<u32>) -> bool {
        self.purge
            .as_ref()
            .is_some_and(|resident| vshards.iter().any(|v| !resident.contains(v)))
    }
}

/// The running purge's hold on the fence. Dropping it ends the purge.
#[derive(Debug)]
#[must_use = "the purge holds the fence only while its guard lives"]
pub struct PurgeGuard<'a> {
    fence: &'a DepartureFence,
}

/// A running install's hold on the fence. Dropping it ends the install.
#[derive(Debug)]
#[must_use = "the install holds the fence only while its guard lives"]
pub struct InstallGuard<'a> {
    fence: &'a DepartureFence,
    vshards: Vec<u32>,
}

impl DepartureFence {
    fn state(&self) -> MutexGuard<'_, FenceState> {
        self.state.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Start a purge of every vShard outside `resident`. `None` while an
    /// install of such a vShard runs, or while another purge runs.
    pub fn try_purge(&self, resident: &BTreeSet<u32>) -> Option<PurgeGuard<'_>> {
        let mut state = self.state();
        let installing_outside = state
            .installing
            .keys()
            .any(|vshard_id| !resident.contains(vshard_id));
        if state.purge.is_some() || installing_outside {
            return None;
        }
        state.purge = Some(resident.clone());
        Some(PurgeGuard { fence: self })
    }

    /// Start an install of `vshards`, once no running purge removes data of
    /// any of them.
    pub async fn begin_install(&self, vshards: &HashSet<u32>) -> InstallGuard<'_> {
        loop {
            let ended = self.purge_ended.notified();
            tokio::pin!(ended);
            // Registered before the check, so a purge that ends after it
            // wakes this wait.
            ended.as_mut().enable();
            {
                let mut state = self.state();
                if !state.purge_blocks(vshards) {
                    for &vshard_id in vshards {
                        *state.installing.entry(vshard_id).or_default() += 1;
                    }
                    return InstallGuard {
                        fence: self,
                        vshards: vshards.iter().copied().collect(),
                    };
                }
            }
            ended.await;
        }
    }
}

impl Drop for PurgeGuard<'_> {
    fn drop(&mut self) {
        self.fence.state().purge = None;
        self.fence.purge_ended.notify_waiters();
    }
}

impl Drop for InstallGuard<'_> {
    fn drop(&mut self) {
        let mut state = self.fence.state();
        for vshard_id in &self.vshards {
            if let Some(count) = state.installing.get_mut(vshard_id) {
                *count -= 1;
                if *count == 0 {
                    state.installing.remove(vshard_id);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, RwLock};
    use std::time::Duration;

    use nodedb_cluster::RoutingTable;

    use super::*;
    use crate::bridge::dispatch::Dispatcher;
    use crate::control::cluster::snapshot_applier::DataPlaneSnapshotApplier;
    use crate::control::state::SharedState;
    use crate::types::TenantDataSnapshot;
    use crate::wal::WalManager;

    #[test]
    fn a_purge_and_an_install_of_a_vshard_outside_its_residency_exclude_each_other() {
        let fence = DepartureFence::default();
        let resident = BTreeSet::from([1, 2]);
        let install = futures::executor::block_on(fence.begin_install(&HashSet::from([3])));
        assert!(
            fence.try_purge(&resident).is_none(),
            "no purge starts while an install of a vShard it purges runs"
        );
        drop(install);
        let purge = fence.try_purge(&resident).expect("the install ended");
        assert!(fence.try_purge(&resident).is_none(), "one purge at a time");
        let resident_install =
            futures::executor::block_on(fence.begin_install(&HashSet::from([1, 2])));
        drop(resident_install);
        drop(purge);
        assert!(fence.state().installing.is_empty());
        assert!(fence.state().purge.is_none());
    }

    /// Wait up to five seconds for core 0 to receive a request.
    async fn core_receives(sides: &mut [crate::bridge::dispatch::CoreChannelDataSide]) -> bool {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            if sides[0].request_rx.try_pop().is_ok() {
                return true;
            }
            if std::time::Instant::now() > deadline {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// A group mounts here again while a purge of its vShards runs. Its
    /// snapshot install reaches no core until the purge ends, and a purge
    /// that finds the install running does not start.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_rehost_install_waits_for_an_in_flight_purge() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = Arc::new(
            WalManager::open_for_testing(&dir.path().join("fence.wal")).expect("test WAL"),
        );
        let (dispatcher, mut sides) = Dispatcher::new(1, 64);
        let mut shared = SharedState::new(dispatcher, wal).expect("shared state");
        let routing = RoutingTable::uniform(2, &[1], 1);
        // `group_ids` lists metadata group 0 too, which homes no vShard.
        let groups: Vec<u64> = routing
            .group_ids()
            .into_iter()
            .filter(|&group_id| group_id != nodedb_cluster::METADATA_GROUP_ID)
            .collect();
        let (rehosted, kept) = (groups[0], groups[1]);
        let resident: BTreeSet<u32> = routing.vshards_for_group(kept).into_iter().collect();
        assert!(
            !routing.vshards_for_group(rehosted).is_empty(),
            "the re-hosted group homes vShards the purge covers"
        );
        Arc::get_mut(&mut shared)
            .expect("unshared state")
            .cluster_routing = Some(Arc::new(RwLock::new(routing)));

        let snapshot = TenantDataSnapshot {
            group_calvin: Some(crate::types::GroupCalvinCut::default()),
            ..Default::default()
        };
        let bytes = zerompk::to_msgpack_vec(&snapshot).expect("encode snapshot");
        let applier = Arc::new(DataPlaneSnapshotApplier::new(Arc::clone(&shared)));

        // The departure pass read a residency without the group, and purges.
        let purge = shared
            .departure_fence
            .try_purge(&resident)
            .expect("no install runs");
        let installing = {
            let applier = Arc::clone(&applier);
            let bytes = bytes.clone();
            tokio::spawn(async move { applier.install(rehosted, &bytes).await })
        };
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(
            sides[0].request_rx.try_pop().is_err(),
            "no core receives the install while the purge runs"
        );

        drop(purge);
        assert!(
            core_receives(&mut sides).await,
            "the install reaches the core once the purge ended"
        );
        assert!(
            shared.departure_fence.try_purge(&resident).is_none(),
            "no purge starts while the install of a vShard it purges runs"
        );

        installing.abort();
        let _ = installing.await;
        assert!(
            shared.departure_fence.try_purge(&resident).is_some(),
            "a purge starts once the install ended"
        );
    }
}
