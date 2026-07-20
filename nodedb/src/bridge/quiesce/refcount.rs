// SPDX-License-Identifier: BUSL-1.1

//! Scan refcount + `ScanGuard` RAII wrapper.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::sync::Notify;

/// Per-collection quiesce state.
#[derive(Debug, Default)]
pub(super) struct CollectionState {
    /// Number of scans currently open against this collection.
    pub(super) open_scans: usize,
    /// `true` once `begin_drain` has been called; new scans are rejected
    /// and the collection is waiting for `open_scans` to reach 0.
    pub(super) draining: bool,
}

/// Why `try_start_scan` refused a new scan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanStartError {
    /// The collection is being hard-deleted. The caller should surface
    /// `NodeDbError::collection_draining` to the client.
    Draining,
}

impl std::fmt::Display for ScanStartError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Draining => f.write_str("collection is draining"),
        }
    }
}

impl std::error::Error for ScanStartError {}

/// Shared, mutex-guarded registry of scan counts and drain state.
///
/// Wrapped in [`Arc`] by callers (both scan handlers and the purge
/// handler hold a clone). Calls happen at scan open/close and at purge
/// time — not per-row — so mutex contention is negligible.
#[derive(Debug, Default)]
pub struct CollectionQuiesce {
    pub(super) inner: Mutex<Inner>,
    /// Woken whenever a scan count decrements. `drain::wait_until_drained`
    /// awaits this and re-checks until count is 0.
    pub(super) notify: Notify,
}

#[derive(Debug, Default)]
pub(super) struct Inner {
    pub(super) states: HashMap<(u64, u64, String), CollectionState>,
}

impl CollectionQuiesce {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Acquire a scan permit for `(tenant_id, collection)`. Returns
    /// `Err(ScanStartError::Draining)` if a drain is in progress;
    /// otherwise returns a [`ScanGuard`] that decrements on drop.
    pub fn try_start_scan(
        self: &Arc<Self>,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
    ) -> Result<ScanGuard, ScanStartError> {
        let mut inner = self.inner.lock().expect("CollectionQuiesce mutex poisoned");
        let entry = inner
            .states
            .entry((database_id, tenant_id, collection.to_string()))
            .or_default();
        if entry.draining {
            return Err(ScanStartError::Draining);
        }
        entry.open_scans += 1;
        Ok(ScanGuard {
            registry: Arc::clone(self),
            database_id,
            tenant_id,
            collection: collection.to_string(),
            released: false,
        })
    }

    /// Current open-scan count. For tests / metrics.
    pub fn open_scans(&self, database_id: u64, tenant_id: u64, collection: &str) -> usize {
        let inner = self.inner.lock().expect("CollectionQuiesce mutex poisoned");
        inner
            .states
            .get(&(database_id, tenant_id, collection.to_string()))
            .map_or(0, |s| s.open_scans)
    }

    /// Whether a drain is currently in progress for this collection.
    pub fn is_draining(&self, database_id: u64, tenant_id: u64, collection: &str) -> bool {
        let inner = self.inner.lock().expect("CollectionQuiesce mutex poisoned");
        inner
            .states
            .get(&(database_id, tenant_id, collection.to_string()))
            .is_some_and(|s| s.draining)
    }

    pub(super) fn release_scan(&self, database_id: u64, tenant_id: u64, collection: &str) {
        let mut inner = self.inner.lock().expect("CollectionQuiesce mutex poisoned");
        if let Some(state) = inner
            .states
            .get_mut(&(database_id, tenant_id, collection.to_string()))
        {
            debug_assert!(state.open_scans > 0, "release without matching acquire");
            state.open_scans = state.open_scans.saturating_sub(1);
        }
        drop(inner);
        self.notify.notify_waiters();
    }
}

/// RAII guard representing one open scan. Drop releases the refcount
/// and notifies any in-progress drain.
#[must_use = "ScanGuard must be held for the lifetime of the scan"]
pub struct ScanGuard {
    registry: Arc<CollectionQuiesce>,
    database_id: u64,
    tenant_id: u64,
    collection: String,
    released: bool,
}

impl ScanGuard {
    /// Explicit release. Equivalent to `drop(guard)`; useful when the
    /// guard has been moved into a struct whose Drop order is uncertain.
    pub fn release(mut self) {
        self.released = true;
        self.registry
            .release_scan(self.database_id, self.tenant_id, &self.collection);
    }
}

impl Drop for ScanGuard {
    fn drop(&mut self) {
        if !self.released {
            self.registry
                .release_scan(self.database_id, self.tenant_id, &self.collection);
        }
    }
}

impl std::fmt::Debug for ScanGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScanGuard")
            .field("database_id", &self.database_id)
            .field("tenant_id", &self.tenant_id)
            .field("collection", &self.collection)
            .field("released", &self.released)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DB: u64 = 0;

    #[test]
    fn guard_increments_and_decrements() {
        let q = CollectionQuiesce::new();
        assert_eq!(q.open_scans(DB, 1, "c"), 0);
        {
            let _g = q.try_start_scan(DB, 1, "c").unwrap();
            assert_eq!(q.open_scans(DB, 1, "c"), 1);
        }
        assert_eq!(q.open_scans(DB, 1, "c"), 0);
    }

    #[test]
    fn multiple_concurrent_guards() {
        let q = CollectionQuiesce::new();
        let g1 = q.try_start_scan(DB, 1, "c").unwrap();
        let g2 = q.try_start_scan(DB, 1, "c").unwrap();
        let g3 = q.try_start_scan(DB, 1, "c").unwrap();
        assert_eq!(q.open_scans(DB, 1, "c"), 3);
        drop(g2);
        assert_eq!(q.open_scans(DB, 1, "c"), 2);
        drop(g1);
        drop(g3);
        assert_eq!(q.open_scans(DB, 1, "c"), 0);
    }

    #[test]
    fn drain_rejects_new_scans() {
        let q = CollectionQuiesce::new();
        q.begin_drain(DB, 1, "c");
        let err = q.try_start_scan(DB, 1, "c").unwrap_err();
        assert_eq!(err, ScanStartError::Draining);
        assert!(q.is_draining(DB, 1, "c"));
    }

    #[test]
    fn drain_does_not_affect_other_scopes() {
        let q = CollectionQuiesce::new();
        q.begin_drain(DB, 1, "c");
        assert!(q.try_start_scan(DB, 1, "other").is_ok());
        assert!(q.try_start_scan(DB, 2, "c").is_ok());
        assert!(q.try_start_scan(1, 1, "c").is_ok());
    }

    #[test]
    fn explicit_release_matches_drop() {
        let q = CollectionQuiesce::new();
        let g = q.try_start_scan(DB, 1, "c").unwrap();
        assert_eq!(q.open_scans(DB, 1, "c"), 1);
        g.release();
        assert_eq!(q.open_scans(DB, 1, "c"), 0);
    }
}
