//! Applied-index gate.
//!
//! Ensures the metadata raft group has finished replaying its
//! committed log before the node advances past
//! `CatalogSanityCheck`. A gap here means the applier fell
//! behind between `raft_ready_rx` firing (which only waits for
//! the first entry) and the recovery check running. Serving
//! client traffic against that state is a correctness bug —
//! the next DDL would race an unapplied prior entry.
//!
//! Implementation note: `MetadataCache.applied_index` is the
//! local applier's watermark. The "expected committed index"
//! is read from the `AppliedIndexWatcher::current()` accessor,
//! which is advanced by the same applier. In practice a gap
//! can only occur if the applier crashed mid-batch or the
//! `current()` source diverges from the cache — both are
//! programming bugs the sanity check exists to surface.

use crate::control::state::SharedState;

/// Outcome of the applied-index gate.
#[derive(Debug, Clone, Copy)]
pub struct AppliedIndexGate {
    /// `MetadataCache.applied_index` observed at check time.
    pub cache_applied: u64,
    /// Watermark observed from `AppliedIndexWatcher::current`.
    pub watcher_current: u64,
    /// `watcher_current - cache_applied`. Zero means no gap.
    pub gap: u64,
}

impl AppliedIndexGate {
    pub fn is_ok(&self) -> bool {
        self.gap == 0
    }
}

/// Read both the `MetadataCache.applied_index` and the
/// `AppliedIndexWatcher::current` and report any gap.
///
/// Single-node mode (no cluster handle) returns a gate with
/// zero gap and zero indexes — there is nothing to replay.
pub fn check_applied_index(shared: &SharedState) -> AppliedIndexGate {
    // If we're in single-node mode, neither source exists in a
    // meaningful sense. Return a trivially-ok gate.
    if shared.cluster_topology.is_none() {
        return AppliedIndexGate {
            cache_applied: 0,
            watcher_current: 0,
            gap: 0,
        };
    }

    let cache_applied = {
        let cache = match shared.metadata_cache.read() {
            Ok(c) => c,
            Err(p) => {
                tracing::error!(
                    "metadata_cache RwLock poisoned during applied-index gate — \
                     recovering guard"
                );
                p.into_inner()
            }
        };
        cache.applied_index
    };

    let watcher_current = shared.metadata_applied_index_watcher.current();

    let gap = watcher_current.saturating_sub(cache_applied);
    AppliedIndexGate {
        cache_applied,
        watcher_current,
        gap,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gate_ok_when_indexes_match() {
        let g = AppliedIndexGate {
            cache_applied: 42,
            watcher_current: 42,
            gap: 0,
        };
        assert!(g.is_ok());
    }

    #[test]
    fn gate_fails_on_gap() {
        let g = AppliedIndexGate {
            cache_applied: 10,
            watcher_current: 42,
            gap: 32,
        };
        assert!(!g.is_ok());
    }
}
