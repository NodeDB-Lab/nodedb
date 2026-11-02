// SPDX-License-Identifier: BUSL-1.1

//! The per-node state that closes and holds read cuts.

use std::sync::Arc;

use nodedb_cluster::SnapshotBoard;

use super::reservations::OrdinalReservations;
use super::snapshots::ActiveSnapshots;

/// The ordinals this node's writes reserved and have not applied, and the
/// transaction snapshots open on this node.
pub struct ReadCutState {
    pub reservations: Arc<OrdinalReservations>,
    pub snapshots: Arc<ActiveSnapshots>,
}

impl ReadCutState {
    pub fn new() -> Self {
        Self {
            reservations: OrdinalReservations::new(),
            snapshots: ActiveSnapshots::new(Arc::new(SnapshotBoard::new())),
        }
    }

    /// The board the open snapshots publish to. The health monitor and the
    /// Raft loop exchange its reports with every peer.
    pub fn board(&self) -> &Arc<SnapshotBoard> {
        self.snapshots.board()
    }
}

impl Default for ReadCutState {
    fn default() -> Self {
        Self::new()
    }
}
