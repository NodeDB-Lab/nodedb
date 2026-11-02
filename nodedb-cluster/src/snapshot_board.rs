// SPDX-License-Identifier: BUSL-1.1

//! The oldest transaction snapshot each node holds, exchanged on the health
//! ping.
//!
//! A node reports `min(oldest snapshot it holds, its wall clock now)`. A
//! snapshot a node opens after its report reads at or above its wall clock
//! at the open, so it is never below that report. Every health ping carries
//! the sender's report, and every pong carries the responder's report.
//!
//! The cleanup horizon is the minimum over the live members
//! ([`SnapshotBoard::horizon`]). A member counts with its last report,
//! raised to `now - max_hold`. A member this node never heard from counts
//! at `now - max_hold`. A snapshot older than `max_hold` serves no statement
//! (the host refuses a statement whose snapshot is too old), so a down or
//! partitioned member holds the horizon back at most `max_hold`, and never
//! lets it pass a snapshot its sessions still read at.

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

/// A report value that carries no snapshot information: a ping that is a
/// bare probe sends it, and a receiver ignores it.
pub const NO_SNAPSHOT_REPORT: i64 = 0;

/// The local value no open snapshot holds.
const NO_OPEN_SNAPSHOT: i64 = i64::MAX;

/// This node's oldest snapshot and the last report of each peer.
#[derive(Debug)]
pub struct SnapshotBoard {
    /// The oldest snapshot this node holds, [`NO_OPEN_SNAPSHOT`] when none.
    local_oldest: AtomicI64,
    /// The last report each peer sent, by node id.
    peers: Mutex<HashMap<u64, i64>>,
}

impl Default for SnapshotBoard {
    fn default() -> Self {
        Self::new()
    }
}

impl SnapshotBoard {
    pub fn new() -> Self {
        Self {
            local_oldest: AtomicI64::new(NO_OPEN_SNAPSHOT),
            peers: Mutex::new(HashMap::new()),
        }
    }

    /// Set the oldest snapshot this node holds. `None` when it holds none.
    pub fn set_local_oldest(&self, oldest: Option<i64>) {
        self.local_oldest
            .store(oldest.unwrap_or(NO_OPEN_SNAPSHOT), Ordering::Release);
    }

    /// The oldest snapshot this node holds, `None` when it holds none.
    pub fn local_oldest(&self) -> Option<i64> {
        let oldest = self.local_oldest.load(Ordering::Acquire);
        (oldest != NO_OPEN_SNAPSHOT).then_some(oldest)
    }

    /// The report this node sends now: its oldest snapshot, capped at its
    /// wall clock.
    pub fn local_report(&self) -> i64 {
        let now = nodedb_types::wall_now_ns_i64().max(1);
        self.local_oldest.load(Ordering::Acquire).min(now)
    }

    /// Record the report `node_id` sent. [`NO_SNAPSHOT_REPORT`] records
    /// nothing.
    pub fn record_peer(&self, node_id: u64, report: i64) {
        if report == NO_SNAPSHOT_REPORT {
            return;
        }
        self.peers
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(node_id, report);
    }

    /// Forget `node_id`: it left the cluster.
    pub fn forget_peer(&self, node_id: u64) {
        self.peers
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(&node_id);
    }

    /// The cleanup horizon: no snapshot any of `members` holds is below it.
    ///
    /// `members` names every live node of the cluster. This node counts with
    /// its own report. A member counts with its last report raised to
    /// `now_ns - max_hold`, and a member with no report counts at
    /// `now_ns - max_hold`. `max_hold` is the longest any statement reads at
    /// one snapshot: the snapshot age limit, plus the longest statement
    /// deadline, plus the clock skew bound.
    pub fn horizon(
        &self,
        self_node_id: u64,
        members: impl IntoIterator<Item = u64>,
        now_ns: i64,
        max_hold: Duration,
    ) -> i64 {
        let hold_ns = i64::try_from(max_hold.as_nanos()).unwrap_or(i64::MAX);
        let floor = now_ns.saturating_sub(hold_ns);
        let peers = self.peers.lock().unwrap_or_else(|p| p.into_inner());
        let mut horizon = self.local_report();
        for member in members {
            if member == self_node_id {
                continue;
            }
            let held = peers
                .get(&member)
                .map_or(floor, |report| (*report).max(floor));
            horizon = horizon.min(held);
        }
        horizon
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const HOUR: Duration = Duration::from_secs(3_600);

    #[test]
    fn the_local_report_is_the_oldest_snapshot_capped_at_now() {
        let board = SnapshotBoard::new();
        let before = nodedb_types::wall_now_ns_i64();
        assert!(board.local_report() >= before, "no snapshot reports now");
        board.set_local_oldest(Some(42));
        assert_eq!(board.local_report(), 42);
        assert_eq!(board.local_oldest(), Some(42));
        board.set_local_oldest(None);
        assert_eq!(board.local_oldest(), None);
    }

    #[test]
    fn the_horizon_is_the_lowest_member_report() {
        let board = SnapshotBoard::new();
        let now = nodedb_types::wall_now_ns_i64();
        board.set_local_oldest(Some(now - 50));
        board.record_peer(2, now - 70);
        board.record_peer(3, now - 10);
        assert_eq!(board.horizon(1, [1, 2, 3], now, HOUR), now - 70);
        assert_eq!(board.horizon(1, [1, 3], now, HOUR), now - 50);
    }

    /// A member that never reported, or whose snapshot is older than the
    /// hold bound, holds the horizon only to `now - max_hold`.
    #[test]
    fn a_silent_or_expired_member_holds_the_horizon_only_to_the_hold_bound() {
        let board = SnapshotBoard::new();
        let now = nodedb_types::wall_now_ns_i64();
        let hold = Duration::from_secs(10);
        let floor = now - 10_000_000_000;
        assert_eq!(board.horizon(1, [1, 2], now, hold), floor);
        board.record_peer(2, floor - 5);
        assert_eq!(board.horizon(1, [1, 2], now, hold), floor);
    }

    #[test]
    fn a_bare_probe_records_nothing_and_a_left_member_is_forgotten() {
        let board = SnapshotBoard::new();
        let now = nodedb_types::wall_now_ns_i64();
        board.record_peer(2, NO_SNAPSHOT_REPORT);
        assert_eq!(
            board.horizon(1, [1, 2], now, HOUR),
            now - HOUR.as_nanos() as i64
        );
        board.record_peer(2, now - 5);
        board.forget_peer(2);
        assert_eq!(
            board.horizon(1, [1, 2], now, HOUR),
            now - HOUR.as_nanos() as i64
        );
    }
}
