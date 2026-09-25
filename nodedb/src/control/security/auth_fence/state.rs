// SPDX-License-Identifier: BUSL-1.1

//! Shared state of the authorization fence.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use tokio::sync::Notify;

use crate::control::cluster::calvin::scheduler::AppliedMirrors;
use crate::control::security::auth_lease::{
    CalvinAckCoverage, LeaderLeaseService, LeaseHolder, LeaseTiming,
};
use crate::control::security::permission_tree::SourceIndex;
use crate::event::progress::CoreEmitProgress;

use super::read_index::ReadIndexCoalescer;
use super::tree_defs::PendingTreeDefs;

/// State the authorization fence and the authorization lease share.
#[derive(Debug)]
pub struct AuthorizationFence {
    /// One counter per Data Plane core, installed when the Event Plane starts.
    emit_progress: OnceLock<Vec<Arc<CoreEmitProgress>>>,
    /// Woken after the permission step or a reload advances the cache.
    permission_applied: Notify,
    /// Read-index coalescers, by Raft group.
    read_index: Mutex<HashMap<u64, Arc<ReadIndexCoalescer>>>,
    /// Tree-definition changes the metadata applier committed.
    tree_defs: PendingTreeDefs,
    /// The permission cache's source collections, readable without its lock.
    sources: Arc<SourceIndex>,
    /// Which Calvin positions this node's schedulers applied.
    calvin_mirrors: AppliedMirrors,
    /// Sequencer completion acks not yet settled against local schedulers.
    calvin_acks: CalvinAckCoverage,
    /// This node's authorization lease.
    holder: LeaseHolder,
    /// Lease timing, installed when the node joins a cluster. Absent on a
    /// single node, which plans without a lease.
    timing: OnceLock<LeaseTiming>,
    /// The leader-side lease service, installed with the Raft loop.
    leader: OnceLock<Arc<LeaderLeaseService>>,
    /// A Raft snapshot was installed since the permission cache last
    /// reloaded for one.
    snapshot_installed: AtomicBool,
}

impl AuthorizationFence {
    /// State sharing `sources` with the permission cache.
    pub fn new(sources: Arc<SourceIndex>) -> Self {
        Self {
            emit_progress: OnceLock::new(),
            permission_applied: Notify::new(),
            read_index: Mutex::new(HashMap::new()),
            tree_defs: PendingTreeDefs::default(),
            sources,
            calvin_mirrors: AppliedMirrors::default(),
            calvin_acks: CalvinAckCoverage::default(),
            holder: LeaseHolder::default(),
            timing: OnceLock::new(),
            leader: OnceLock::new(),
            snapshot_installed: AtomicBool::new(false),
        }
    }

    /// Record that a Raft snapshot replaced data-group state. The rows it
    /// brought emitted no events.
    pub fn note_snapshot_installed(&self) {
        self.snapshot_installed.store(true, Ordering::Release);
    }

    /// Whether a snapshot was installed since the last call.
    pub fn take_snapshot_installed(&self) -> bool {
        self.snapshot_installed.swap(false, Ordering::AcqRel)
    }

    /// Install the emitted-event counters, one per core in core order.
    /// Returns `false` when counters were already installed.
    pub fn install_emit_progress(&self, progress: Vec<Arc<CoreEmitProgress>>) -> bool {
        self.emit_progress.set(progress).is_ok()
    }

    /// The emitted-event counter of every core, read now. `None` before the
    /// Event Plane starts: no permission step runs, so the cache cannot track
    /// writes and a coverage wait reloads it.
    pub fn emitted_snapshot(&self) -> Option<Vec<u64>> {
        self.emit_progress
            .get()
            .map(|cores| cores.iter().map(|core| core.emitted()).collect())
    }

    /// The wake-up the permission step and a reload fire.
    pub fn permission_applied(&self) -> &Notify {
        &self.permission_applied
    }

    /// The tree-definition changes waiting for the cache.
    pub fn tree_defs(&self) -> &PendingTreeDefs {
        &self.tree_defs
    }

    /// The permission cache's source collections.
    pub fn sources(&self) -> &SourceIndex {
        &self.sources
    }

    /// Which Calvin positions this node's schedulers applied.
    pub fn calvin_mirrors(&self) -> &AppliedMirrors {
        &self.calvin_mirrors
    }

    /// Sequencer completion acks not yet settled against local schedulers.
    pub fn calvin_acks(&self) -> &CalvinAckCoverage {
        &self.calvin_acks
    }

    /// This node's authorization lease.
    pub fn holder(&self) -> &LeaseHolder {
        &self.holder
    }

    /// Install the lease timing. Returns `false` when already installed.
    pub fn install_timing(&self, timing: LeaseTiming) -> bool {
        self.timing.set(timing).is_ok()
    }

    /// The lease timing, when this node runs in a cluster.
    pub fn timing(&self) -> Option<LeaseTiming> {
        self.timing.get().copied()
    }

    /// Install the leader-side lease service. Returns `false` when already
    /// installed.
    pub fn install_leader(&self, service: Arc<LeaderLeaseService>) -> bool {
        self.leader.set(service).is_ok()
    }

    /// The leader-side lease service, once installed.
    pub fn leader(&self) -> Option<&Arc<LeaderLeaseService>> {
        self.leader.get()
    }

    /// The read-index coalescer of `group_id`.
    pub fn read_index_coalescer(&self, group_id: u64) -> Arc<ReadIndexCoalescer> {
        let mut coalescers = self.read_index.lock().unwrap_or_else(|p| p.into_inner());
        Arc::clone(
            coalescers
                .entry(group_id)
                .or_insert_with(|| Arc::new(ReadIndexCoalescer::new())),
        )
    }
}
