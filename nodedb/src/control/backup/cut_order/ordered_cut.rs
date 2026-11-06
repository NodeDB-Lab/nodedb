// SPDX-License-Identifier: BUSL-1.1

//! One cut whose data-group barriers are ordered with its Calvin marker.
//!
//! The cut's marker carries the barrier. In every data group, the leader
//! proposes the barrier once every scheduler of the group's vShards on that
//! node passed the marker, and holds the redo of every later transaction
//! until it applied the barrier. A transaction sequenced before the marker
//! installed before its scheduler passed it, so its redo sits before the
//! barrier. A later one proposes its redo only after the barrier.
//!
//! The ordering has a window, measured from the cut's watermark on the HLC
//! wall clock. A leader proposes the barrier only inside
//! [`CutWindow::propose_until`]. A scheduler holds later redo only until
//! [`CutWindow::hold_until`], a clock-skew margin later. A leader that
//! released its holds at the end of the window therefore never sees a new
//! leader place the barrier after the released redo, while clock skew stays
//! below the margin. A cut whose window closed with no barrier in a group
//! failed: its coordinator's deadline ends inside the window.

use std::time::Duration;

use nodedb_cluster::calvin::{CutBarrierWire, CutCaptureWire};
use nodedb_physical::physical_plan::CutCaptureRequest;
use nodedb_types::MirrorCutMark;

use crate::control::state::SharedState;
use crate::control::wal_replication::ReplicatedWrite;
use crate::control::wal_replication::types::{MirrorBarrier, MirrorCalvinFloor};
use crate::types::CoveredCutBarrier;

/// The clock skew between two nodes a cut's window tolerates.
const CLOCK_SKEW_MARGIN: Duration = Duration::from_secs(30);

/// What names one cut on every node: its watermark, its restore point, its
/// capture request, `0` for none, and its mirror cut number, `0` for none.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CutKey {
    pub hlc: u64,
    pub restore_point: u64,
    pub capture: u64,
    pub mirror: u64,
}

/// A cut whose barriers the data-group leaders order with its marker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderedCut {
    /// The cut's watermark: HLC wall time in nanoseconds.
    pub hlc: u64,
    /// The cluster restore point the cut takes, `0` for a backup's cut.
    pub restore_point: u64,
    /// A database backup's capture request.
    pub capture: Option<CutCaptureRequest>,
    /// What the cut tells a source's mirrors. Such a cut records no restore
    /// floor: it runs far too often to fsync one, and no restore names it.
    pub mirror: Option<MirrorCutMark>,
}

impl OrderedCut {
    pub fn key(&self) -> CutKey {
        CutKey {
            hlc: self.hlc,
            restore_point: self.restore_point,
            capture: self
                .capture
                .as_ref()
                .map_or(0, |request| request.request_id),
            mirror: self.mirror.map_or(0, |mark| mark.seq),
        }
    }

    /// The cut a marker carrying `hlc`, `restore_point` and `barrier` names.
    pub fn from_marker(hlc: u64, restore_point: u64, barrier: &CutBarrierWire) -> Self {
        Self {
            hlc,
            restore_point,
            capture: barrier.capture.as_ref().map(|wire| CutCaptureRequest {
                request_id: wire.request_id,
                database_id: wire.database_id,
                tenants: wire.tenants.clone(),
            }),
            mirror: barrier.mirror,
        }
    }

    /// The barrier this cut's marker carries.
    pub fn to_wire(&self) -> CutBarrierWire {
        CutBarrierWire {
            capture: self.capture.as_ref().map(|request| CutCaptureWire {
                request_id: request.request_id,
                database_id: request.database_id,
                tenants: request.tenants.clone(),
            }),
            mirror: self.mirror,
        }
    }

    /// The write of this cut's barrier entry. `calvin_floors` are the fully
    /// applied Calvin epochs of the group's vShards, read before the propose.
    /// A cut of no mirror ignores them.
    pub fn barrier_write(&self, calvin_floors: Vec<MirrorCalvinFloor>) -> ReplicatedWrite {
        ReplicatedWrite::CutBarrier {
            hlc: self.hlc,
            restore_point: self.restore_point,
            capture: self.capture.clone(),
            mirror: self.mirror.map(|mark| MirrorBarrier {
                mark,
                calvin_floors,
            }),
        }
    }

    /// This cut's barrier at `log_index`, as a data-group snapshot carries it.
    pub fn covered_at(&self, log_index: u64) -> CoveredCutBarrier {
        CoveredCutBarrier {
            hlc: self.hlc,
            restore_point: self.restore_point,
            capture: self.capture.clone(),
            mirror: self.mirror,
            log_index,
        }
    }

    /// The cut a data-group snapshot's `barrier` names.
    pub fn from_covered(barrier: &CoveredCutBarrier) -> Self {
        Self {
            hlc: barrier.hlc,
            restore_point: barrier.restore_point,
            capture: barrier.capture.clone(),
            mirror: barrier.mirror,
        }
    }
}

/// The ordering window of one cut on this node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CutWindow {
    /// The last HLC wall time a leader proposes the cut's barrier at.
    pub propose_until: u64,
    /// The last HLC wall time a scheduler holds later redo at.
    pub hold_until: u64,
}

impl CutWindow {
    /// The window of the cut at `hlc`, given the statement deadline
    /// `deadline`. A coordinator waits one deadline from its watermark, and
    /// a remote source node one more from the request it receives.
    pub fn of(hlc: u64, deadline: Duration) -> Self {
        let propose = deadline.saturating_mul(2);
        let propose_until = hlc.saturating_add(nanos(propose));
        Self {
            propose_until,
            hold_until: propose_until.saturating_add(nanos(CLOCK_SKEW_MARGIN)),
        }
    }

    /// The window of the cut at `hlc` under `state`'s statement deadline.
    pub fn on(state: &SharedState, hlc: u64) -> Self {
        Self::of(
            hlc,
            Duration::from_secs(state.tuning.network.default_deadline_secs),
        )
    }
}

/// `duration` in nanoseconds, saturating.
fn nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_cut_round_trips_through_its_marker() {
        let cut = OrderedCut {
            hlc: 7,
            restore_point: 3,
            capture: Some(CutCaptureRequest {
                request_id: 11,
                database_id: 2,
                tenants: vec![1, 4],
            }),
            mirror: None,
        };
        let back = OrderedCut::from_marker(7, 3, &cut.to_wire());
        assert_eq!(back, cut);
        assert_eq!(
            back.key(),
            CutKey {
                hlc: 7,
                restore_point: 3,
                capture: 11,
                mirror: 0,
            }
        );
        let plain = OrderedCut {
            hlc: 7,
            restore_point: 0,
            capture: None,
            mirror: None,
        };
        assert_eq!(plain.key().capture, 0);
        assert_ne!(plain.key(), cut.key(), "the capture keeps two cuts apart");
    }

    /// A mirror cut carries its mark through the marker and the snapshot, and
    /// never shares a key with a backup's cut at the same watermark.
    #[test]
    fn a_mirror_cut_keeps_its_mark_and_its_own_key() {
        let mark = MirrorCutMark {
            seq: 9,
            cap_ordinal: 5,
            metadata_index: 4,
        };
        let cut = OrderedCut {
            hlc: 7,
            restore_point: 0,
            capture: None,
            mirror: Some(mark),
        };
        assert_eq!(OrderedCut::from_marker(7, 0, &cut.to_wire()), cut);
        assert_eq!(OrderedCut::from_covered(&cut.covered_at(40)), cut);
        let backup = OrderedCut {
            mirror: None,
            ..cut.clone()
        };
        assert_ne!(cut.key(), backup.key());
        let floors = vec![MirrorCalvinFloor {
            vshard: 2,
            epoch: 3,
        }];
        let ReplicatedWrite::CutBarrier { mirror, .. } = cut.barrier_write(floors.clone()) else {
            panic!("a cut's write is a barrier");
        };
        assert_eq!(
            mirror,
            Some(MirrorBarrier {
                mark,
                calvin_floors: floors,
            })
        );
    }

    #[test]
    fn a_cut_round_trips_through_a_snapshot() {
        let cut = OrderedCut {
            hlc: 7,
            restore_point: 3,
            capture: Some(CutCaptureRequest {
                request_id: 11,
                database_id: 2,
                tenants: vec![1],
            }),
            mirror: None,
        };
        let covered = cut.covered_at(40);
        assert_eq!(covered.log_index, 40);
        assert_eq!(OrderedCut::from_covered(&covered), cut);
    }

    #[test]
    fn the_hold_outlasts_the_proposal_window_by_the_skew_margin() {
        let window = CutWindow::of(1_000, Duration::from_secs(30));
        assert_eq!(window.propose_until, 1_000 + 60_000_000_000);
        assert_eq!(
            window.hold_until - window.propose_until,
            nanos(CLOCK_SKEW_MARGIN)
        );
    }
}
