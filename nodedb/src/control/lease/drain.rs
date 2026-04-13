//! Descriptor lease drain state.
//!
//! While a descriptor is being drained, any new lease acquire at
//! `version <= up_to_version` must be rejected cluster-wide so the
//! in-flight DDL that bumps the version can make progress.
//!
//! **State ownership**: the canonical drain state is replicated
//! through the metadata raft group via
//! `MetadataEntry::DescriptorDrainStart` / `DescriptorDrainEnd`
//! entries. Every node's `MetadataCommitApplier` decodes those
//! entries and calls `install_start` / `install_end` on a local
//! `DescriptorDrainTracker` mounted on `SharedState.lease_drain`.
//! Reads of the tracker happen on every lease acquire (the
//! `is_draining` check in `force_refresh_lease`) and during the
//! proposer's drain wait loop. This file owns the in-memory
//! state only; the propose-side orchestration (including the
//! rolling-upgrade gate and the wait-for-leases-to-release loop)
//! lives in `drain_propose.rs`.
//!
//! **TTL semantics**: every drain entry carries an `expires_at`
//! HLC. A crashed proposer would otherwise leave an orphaned
//! drain entry that blocks the cluster forever. `is_draining`
//! filters expired entries at read time; we do NOT run a
//! periodic GC task (same lazy-cleanup approach the lease store
//! uses). If nothing ever re-writes the key, the expired entry
//! sits in the map until the next `install_end` on the same id
//! or until process restart (drain state is not persisted to
//! redb — it's raft-log-derived and rebuilds on replay).

use std::collections::HashMap;
use std::sync::RwLock;

use nodedb_cluster::DescriptorId;
use nodedb_types::Hlc;

/// One drain entry: "this descriptor is draining leases at
/// versions <= `up_to_version` until `expires_at`".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DrainEntry {
    pub up_to_version: u64,
    pub expires_at: Hlc,
}

/// In-memory drain state for descriptors being altered.
///
/// All public mutations (`install_start`, `install_end`) are
/// called by the metadata applier's decode path. All public
/// reads (`is_draining`, `snapshot`, `count`) are called by the
/// lease acquire path and the drain wait loop.
#[derive(Debug, Default)]
pub struct DescriptorDrainTracker {
    active: RwLock<HashMap<DescriptorId, DrainEntry>>,
}

impl DescriptorDrainTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record the start of a drain for `id` at `up_to_version`
    /// with a TTL of `expires_at`. Overwrites any prior entry
    /// for the same key — a subsequent start with a higher
    /// `up_to_version` extends the drain rather than creating a
    /// conflicting record.
    ///
    /// Called by the metadata applier on every node when a
    /// `DescriptorDrainStart` raft entry commits.
    pub fn install_start(&self, id: DescriptorId, up_to_version: u64, expires_at: Hlc) {
        let mut map = self.active.write().unwrap_or_else(|p| p.into_inner());
        map.insert(
            id,
            DrainEntry {
                up_to_version,
                expires_at,
            },
        );
    }

    /// Remove the drain entry for `id`, if any. Called by the
    /// metadata applier both on explicit `DescriptorDrainEnd`
    /// raft entries AND on the implicit clear path that runs
    /// after a successful `Put*` apply.
    pub fn install_end(&self, id: &DescriptorId) {
        let mut map = self.active.write().unwrap_or_else(|p| p.into_inner());
        map.remove(id);
    }

    /// Whether an acquire on `(id, requested_version)` must be
    /// rejected because a drain is active that covers this
    /// version.
    ///
    /// Returns `true` iff an entry exists for `id` with
    /// `expires_at > now_wall_ns` (i.e. not stale) AND
    /// `requested_version <= entry.up_to_version` (i.e. the
    /// requested version is inside the drain range).
    ///
    /// `now_wall_ns` is a real wall-clock timestamp — pass the
    /// same nanosecond count the lease module uses for expiry
    /// checks so both comparisons land in the same reference
    /// frame. See the wall-clock rationale in
    /// `lease::renewal::tick`.
    pub fn is_draining(&self, id: &DescriptorId, requested_version: u64, now_wall_ns: u64) -> bool {
        let map = self.active.read().unwrap_or_else(|p| p.into_inner());
        match map.get(id) {
            Some(entry) => {
                entry.expires_at.wall_ns > now_wall_ns && requested_version <= entry.up_to_version
            }
            None => false,
        }
    }

    /// Snapshot the full (id, entry) set for diagnostics and
    /// tests. Returns all entries including expired ones — the
    /// caller is responsible for filtering by `now_wall_ns` if
    /// it wants the live set.
    pub fn snapshot(&self) -> Vec<(DescriptorId, DrainEntry)> {
        let map = self.active.read().unwrap_or_else(|p| p.into_inner());
        map.iter().map(|(id, e)| (id.clone(), *e)).collect()
    }

    /// Count of active (non-expired) drain entries at the given
    /// wall-clock time. Used by the cluster harness test helpers.
    pub fn count_active(&self, now_wall_ns: u64) -> usize {
        let map = self.active.read().unwrap_or_else(|p| p.into_inner());
        map.values()
            .filter(|e| e.expires_at.wall_ns > now_wall_ns)
            .count()
    }

    /// Total count including expired entries. Mainly for
    /// debugging.
    pub fn total_count(&self) -> usize {
        let map = self.active.read().unwrap_or_else(|p| p.into_inner());
        map.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_cluster::DescriptorKind;

    fn id(name: &str) -> DescriptorId {
        DescriptorId::new(1, DescriptorKind::Collection, name.to_string())
    }

    fn hlc(wall_ns: u64) -> Hlc {
        Hlc::new(wall_ns, 0)
    }

    #[test]
    fn install_then_is_draining_true_for_versions_in_range() {
        let tracker = DescriptorDrainTracker::new();
        let d = id("orders");
        tracker.install_start(d.clone(), 5, hlc(1_000_000));
        // Now wall is before expiry. Versions 1..=5 are inside
        // the drain range; version 6 is outside.
        assert!(tracker.is_draining(&d, 1, 500_000));
        assert!(tracker.is_draining(&d, 3, 500_000));
        assert!(tracker.is_draining(&d, 5, 500_000));
        assert!(!tracker.is_draining(&d, 6, 500_000));
        assert!(!tracker.is_draining(&d, 100, 500_000));
    }

    #[test]
    fn install_end_clears_entry() {
        let tracker = DescriptorDrainTracker::new();
        let d = id("orders");
        tracker.install_start(d.clone(), 5, hlc(1_000_000));
        assert!(tracker.is_draining(&d, 5, 500_000));

        tracker.install_end(&d);
        assert!(!tracker.is_draining(&d, 5, 500_000));
        assert_eq!(tracker.total_count(), 0);
    }

    #[test]
    fn is_draining_filters_expired_entries() {
        let tracker = DescriptorDrainTracker::new();
        let d = id("stale");
        // Entry expired at wall_ns = 1000.
        tracker.install_start(d.clone(), 5, hlc(1_000));
        // Wall now is 2000 — past expiry. Must be treated as
        // not draining even though the entry is still in the map.
        assert!(!tracker.is_draining(&d, 1, 2_000));
        assert!(!tracker.is_draining(&d, 5, 2_000));
        // But if wall now is before expiry it IS draining.
        assert!(tracker.is_draining(&d, 5, 500));
    }

    #[test]
    fn multiple_descriptors_are_independent() {
        let tracker = DescriptorDrainTracker::new();
        let a = id("a");
        let b = id("b");
        tracker.install_start(a.clone(), 1, hlc(1_000_000));
        tracker.install_start(b.clone(), 10, hlc(1_000_000));

        assert!(tracker.is_draining(&a, 1, 500_000));
        assert!(!tracker.is_draining(&a, 2, 500_000));
        assert!(tracker.is_draining(&b, 5, 500_000));
        assert!(tracker.is_draining(&b, 10, 500_000));
        assert!(!tracker.is_draining(&b, 11, 500_000));
    }

    #[test]
    fn install_start_overwrites_prior_entry() {
        let tracker = DescriptorDrainTracker::new();
        let d = id("orders");
        tracker.install_start(d.clone(), 5, hlc(1_000_000));
        // Start again with a higher up_to_version — the new
        // entry extends the drain range.
        tracker.install_start(d.clone(), 10, hlc(2_000_000));

        assert!(tracker.is_draining(&d, 10, 500_000));
        assert_eq!(tracker.total_count(), 1);
        let snap = tracker.snapshot();
        assert_eq!(snap[0].1.up_to_version, 10);
        assert_eq!(snap[0].1.expires_at.wall_ns, 2_000_000);
    }

    #[test]
    fn count_active_filters_expired() {
        let tracker = DescriptorDrainTracker::new();
        let a = id("live");
        let b = id("dead");
        tracker.install_start(a, 1, hlc(10_000_000));
        tracker.install_start(b, 1, hlc(100));

        assert_eq!(tracker.total_count(), 2);
        assert_eq!(tracker.count_active(1_000), 1);
        assert_eq!(tracker.count_active(20_000_000), 0);
    }
}
