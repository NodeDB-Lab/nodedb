//! Per-group closed-timestamp tracker with HLC skew bounding.
//!
//! Every time a Raft group applies a committed entry, the applier
//! records the wall-clock instant as that group's "closed timestamp".
//! A follower whose closed timestamp for a group is within the
//! caller's staleness bound can serve reads locally — no gateway hop
//! to the leader.
//!
//! ## HLC integration
//!
//! The tracker also owns the node-wide [`HlcClock`]. When an apply
//! path knows the leader-stamped `Hlc` for the entry it is applying,
//! it should call [`ClosedTimestampTracker::fold_remote_hlc`] instead
//! of [`ClosedTimestampTracker::mark_applied`]. Folding the remote
//! HLC into the local clock bounds cross-node `_ts_system` skew at
//! this node: any subsequent local stamp is strictly greater than
//! every observed remote HLC, so versions written here can never
//! collide with — or appear earlier than — versions a leader has
//! already replicated.
//!
//! Apply-side wiring is intentionally optional. Code paths that don't
//! yet thread the leader's HLC keep using `mark_applied` and only
//! lose the cross-node skew bound; correctness of the local
//! `_ts_system` stamp is unaffected because [`HlcClock::now`] already
//! advances past the local wall clock.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use nodedb_types::{Hlc, HlcClock};

/// Tracks the most recent apply instant per Raft group plus the
/// shared node-wide HLC.
pub struct ClosedTimestampTracker {
    groups: RwLock<HashMap<u64, Instant>>,
    hlc: Arc<HlcClock>,
}

impl ClosedTimestampTracker {
    /// Construct a tracker with a fresh, node-private HLC. Tests and
    /// stand-alone follower-read setups use this; production paths
    /// should call [`Self::with_hlc`] to share the node-wide clock.
    pub fn new() -> Self {
        Self {
            groups: RwLock::new(HashMap::new()),
            hlc: Arc::new(HlcClock::new()),
        }
    }

    /// Construct a tracker wired to a caller-supplied HLC. Use this
    /// in production so the tracker's `fold_remote_hlc` advances the
    /// same clock that other subsystems read via `now()`.
    pub fn with_hlc(hlc: Arc<HlcClock>) -> Self {
        Self {
            groups: RwLock::new(HashMap::new()),
            hlc,
        }
    }

    /// Read access to the shared HLC. Other apply-side subsystems
    /// (descriptor leases, metadata cache) advance and read it
    /// through this handle.
    pub fn hlc(&self) -> &Arc<HlcClock> {
        &self.hlc
    }

    /// Record that `group_id` just applied one or more entries.
    /// Called by the raft-loop applier after each apply batch.
    pub fn mark_applied(&self, group_id: u64) {
        let mut g = self.groups.write().unwrap_or_else(|p| p.into_inner());
        g.insert(group_id, Instant::now());
    }

    /// Record that `group_id` just applied, using a caller-supplied
    /// instant. Exposed for deterministic testing with paused time.
    pub fn mark_applied_at(&self, group_id: u64, at: Instant) {
        let mut g = self.groups.write().unwrap_or_else(|p| p.into_inner());
        g.insert(group_id, at);
    }

    /// Mark a group applied AND fold the leader-stamped `remote` HLC
    /// into the local clock. Returns the merged HLC that any local
    /// stamp emitted after this call is guaranteed to be strictly
    /// greater than.
    ///
    /// This is the production apply-path entry point: every committed
    /// entry that carries a leader HLC (descriptor leases, catalog
    /// DDL, drain events) should route through here so cross-node
    /// `_ts_system` skew is bounded at this node.
    pub fn fold_remote_hlc(&self, group_id: u64, remote: Hlc) -> Hlc {
        self.mark_applied(group_id);
        self.hlc.update(remote)
    }

    /// Check whether this node's replica of `group_id` has applied
    /// recently enough that a read with `max_staleness` can be
    /// served locally.
    ///
    /// Returns `false` if the group has never applied on this node
    /// (no closed timestamp recorded).
    pub fn is_fresh_enough(&self, group_id: u64, max_staleness: Duration) -> bool {
        let g = self.groups.read().unwrap_or_else(|p| p.into_inner());
        match g.get(&group_id) {
            Some(last) => last.elapsed() <= max_staleness,
            None => false,
        }
    }

    /// Return the age of the closed timestamp for a group, or `None`
    /// if the group has never applied on this node. Useful for
    /// observability (metrics, SHOW commands).
    pub fn staleness(&self, group_id: u64) -> Option<Duration> {
        let g = self.groups.read().unwrap_or_else(|p| p.into_inner());
        g.get(&group_id).map(|last| last.elapsed())
    }
}

impl Default for ClosedTimestampTracker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_group_is_not_fresh() {
        let tracker = ClosedTimestampTracker::new();
        assert!(!tracker.is_fresh_enough(99, Duration::from_secs(10)));
    }

    #[test]
    fn recently_applied_is_fresh() {
        let tracker = ClosedTimestampTracker::new();
        tracker.mark_applied(1);
        assert!(tracker.is_fresh_enough(1, Duration::from_secs(5)));
    }

    #[test]
    fn stale_group_is_not_fresh() {
        let tracker = ClosedTimestampTracker::new();
        let old = Instant::now() - Duration::from_secs(30);
        tracker.mark_applied_at(1, old);
        assert!(!tracker.is_fresh_enough(1, Duration::from_secs(5)));
    }

    #[test]
    fn staleness_returns_none_for_unknown() {
        let tracker = ClosedTimestampTracker::new();
        assert!(tracker.staleness(42).is_none());
    }

    #[test]
    fn staleness_returns_age_for_known() {
        let tracker = ClosedTimestampTracker::new();
        tracker.mark_applied(1);
        let s = tracker.staleness(1).unwrap();
        assert!(s < Duration::from_millis(100));
    }

    #[test]
    fn mark_applied_updates_monotonically() {
        let tracker = ClosedTimestampTracker::new();
        let old = Instant::now() - Duration::from_secs(10);
        tracker.mark_applied_at(1, old);
        assert!(!tracker.is_fresh_enough(1, Duration::from_secs(5)));
        tracker.mark_applied(1);
        assert!(tracker.is_fresh_enough(1, Duration::from_secs(5)));
    }

    #[test]
    fn fold_remote_hlc_bounds_cross_node_skew() {
        // Local clock is fresh — its first `now()` will sit near
        // current wall-clock. A leader far in the future stamps an
        // entry; folding it MUST advance the local clock past it so
        // any subsequent local stamp can never collide with or
        // precede the leader's observation.
        let tracker = ClosedTimestampTracker::new();
        let local_before = tracker.hlc().now();
        let remote = Hlc::new(local_before.wall_ns + 60_000_000_000, 7); // +60s
        let merged = tracker.fold_remote_hlc(1, remote);

        assert!(merged > remote, "merged HLC strictly greater than remote");
        assert!(
            merged > local_before,
            "merged HLC strictly greater than prior local"
        );
        assert!(tracker.is_fresh_enough(1, Duration::from_secs(5)));

        // Subsequent local `now()` is strictly greater than the merged
        // observation — the skew bound holds for every following stamp.
        let after = tracker.hlc().now();
        assert!(
            after > merged,
            "subsequent local stamp dominates folded remote"
        );
    }

    #[test]
    fn fold_remote_hlc_idempotent_under_replay() {
        // Replaying the same remote HLC must not regress the clock.
        let tracker = ClosedTimestampTracker::new();
        let remote = Hlc::new(1_000_000_000_000, 0);
        let first = tracker.fold_remote_hlc(1, remote);
        let second = tracker.fold_remote_hlc(1, remote);
        assert!(
            second > first,
            "replay still advances logical counter, never regresses"
        );
    }

    #[test]
    fn with_hlc_shares_clock_across_subsystems() {
        // Two trackers sharing one HlcClock observe each other's
        // remote folds. This is the production wiring shape:
        // ClosedTimestampTracker + MetadataCache + descriptor lease
        // applier all hold the same Arc<HlcClock>.
        let hlc = Arc::new(HlcClock::new());
        let t1 = ClosedTimestampTracker::with_hlc(Arc::clone(&hlc));
        let t2 = ClosedTimestampTracker::with_hlc(Arc::clone(&hlc));

        let remote = Hlc::new(2_000_000_000_000, 5);
        let merged = t1.fold_remote_hlc(1, remote);
        // t2's clock has already advanced past `remote` because the
        // Arc is shared.
        let observed = t2.hlc().now();
        assert!(observed > merged);
    }
}
